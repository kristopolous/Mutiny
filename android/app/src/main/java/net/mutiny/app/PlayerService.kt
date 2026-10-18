package net.mutiny.app

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.ComponentName
import android.content.Context
import android.content.Intent
import android.content.pm.ServiceInfo
import android.os.Binder
import android.os.Build
import android.os.Handler
import android.os.IBinder
import android.os.Looper
import android.net.Uri
import java.io.File
import android.util.Log
import androidx.core.app.NotificationCompat
import androidx.core.content.ContextCompat
import android.support.v4.media.MediaMetadataCompat
import android.support.v4.media.session.MediaSessionCompat
import android.support.v4.media.session.PlaybackStateCompat
import androidx.media.session.MediaButtonReceiver
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.json.JSONObject

/**
 * Foreground media service.
 *
 * Owns the [NativePlayer] (ExoPlayer + the web's d() state machine) and a
 * [MediaSessionCompat] that hardware media keys (headsets, BT, the lock
 * screen and the notification) drive. It runs as a foreground service of
 * type mediaPlayback, so music keeps playing with the screen off / the app
 * in the background / the WebView frozen — the exact case where the plain
 * web page fails.
 */
class PlayerService : Service() {

    companion object {
        private const val TAG = "PlayerService"
        const val ACTION_STOP = "net.mutiny.app.ACTION_STOP"
        const val NOTIF_CHANNEL = "mutiny_music"
        const val NOTIF_ID = 42

        @Volatile var instance: PlayerService? = null

        /** Start (or restart) the service. */
        fun start(ctx: Context) {
            ContextCompat.startForegroundService(ctx, Intent(ctx, PlayerService::class.java))
        }

        /** Toggle play/pause, starting the service when it is not running. */
        fun toggle(ctx: Context) {
            val s = instance
            if (s != null) {
                if (s.isPlaying()) s.pause() else s.play()
            } else {
                start(ctx)
            }
        }

        /** Stop playback and the service. */
        fun stop(ctx: Context) {
            ctx.startService(Intent(ctx, PlayerService::class.java).setAction(ACTION_STOP))
        }
    }

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Main.immediate)
    private lateinit var api: MutinyApi
    private lateinit var player: NativePlayer
    private lateinit var session: MediaSessionCompat
    private var sessionActivity: PendingIntent? = null
    private var artworkJob: Job? = null
    private var lastPersist = 0L
    private var lastNav = 0L

    /** The activity registers this to keep the WebView UI in sync. */
    @Volatile var trackListener: ((Track, ReleaseState) -> Unit)? = null

    private val ticker: Handler = Handler(Looper.getMainLooper()) {
        tick()
        ticker.sendEmptyMessageDelayed(0, 1000)
    }

    private val binder = object : Binder() {
        val service: PlayerService get() = this@PlayerService
    }

    // ------------------------------------------------------------------
    // service lifecycle
    // ------------------------------------------------------------------

    override fun onCreate() {
        super.onCreate()
        instance = this
        api = MutinyApi(Prefs.base(this))
        player = NativePlayer(this, api, playerListener)
        createSession()
        ensureChannel()
        ticker.sendEmptyMessageDelayed(0, 1000)
        // resume after a process death (a mediaPlayback FGS is restartable)
        val json = Prefs.resume(this)
        if (json.isNotEmpty()) {
            try {
                player.restore(JSONObject(json))
                Log.d(TAG, "restored playback from snapshot")
            } catch (e: Exception) {
                Log.w(TAG, "restore failed: $e")
            }
        }
    }

    override fun onBind(intent: Intent): IBinder? = binder

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        if (intent?.action == ACTION_STOP) {
            stopPlayback()
            return START_NOT_STICKY
        }
        // always a foreground service while we're alive
        val notif = notification()
        if (Build.VERSION.SDK_INT >= 29) {
            startForeground(NOTIF_ID, notif, ServiceInfo.FOREGROUND_SERVICE_TYPE_MEDIA_PLAYBACK)
        } else {
            startForeground(NOTIF_ID, notif)
        }
        return START_STICKY
    }

    override fun onDestroy() {
        Log.d(TAG, "onDestroy")
        ticker.removeCallbacksAndMessages(null)
        player.release()
        session.release()
        scope.cancel()
        if (instance === this) instance = null
        super.onDestroy()
    }

    /** The user closed the task; keep playing. */
    override fun onTaskRemoved(rootIntent: Intent?) = Unit

    // ------------------------------------------------------------------
    // media session
    // ------------------------------------------------------------------

    private fun createSession() {
        val cb = object : MediaSessionCompat.Callback() {
            override fun onPlay() = play()
            override fun onPause() = pause()
            override fun onSkipToNext() = next()
            override fun onSkipToPrevious() = prev()
            override fun onStop() = stopPlayback()
            override fun onSeekTo(pos: Long) = player.seekTo(pos)
            override fun onFastForward() = player.seekTo(player.positionMs() + 10_000)
            override fun onRewind() = player.seekTo(player.positionMs() - 10_000)
        }
        sessionActivity = PendingIntent.getActivity(
            this, 0,
            Intent(this, MainActivity::class.java).addFlags(Intent.FLAG_ACTIVITY_SINGLE_TOP),
            PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT,
        )
        // pass null for the 4th arg: the library then derives the media-button
        // broadcast intent from the receiver component above. Passing an
        // activity PendingIntent there makes the platform reject the session.
        session = MediaSessionCompat(
            this,
            "mutiny",
            ComponentName(this, MediaButtonReceiver::class.java),
            null,
        )
        session.setSessionActivity(sessionActivity!!)
        session.setCallback(cb)
        session.setActive(true)
        session.setPlaybackState(basePlaybackState(PlaybackStateCompat.STATE_STOPPED, 0L))
    }

    private fun basePlaybackState(state: Int, pos: Long): PlaybackStateCompat =
        PlaybackStateCompat.Builder()
            .setState(state, pos, 0f)
            .setActions(
                PlaybackStateCompat.ACTION_PLAY or
                    PlaybackStateCompat.ACTION_PAUSE or
                    PlaybackStateCompat.ACTION_PLAY_PAUSE or
                    PlaybackStateCompat.ACTION_SKIP_TO_NEXT or
                    PlaybackStateCompat.ACTION_SKIP_TO_PREVIOUS or
                    PlaybackStateCompat.ACTION_STOP or
                    PlaybackStateCompat.ACTION_SEEK_TO or
                    PlaybackStateCompat.ACTION_FAST_FORWARD or
                    PlaybackStateCompat.ACTION_REWIND
            )
            .build()

    private fun notification(): Notification {
        val builder = NotificationCompat.Builder(this, NOTIF_CHANNEL)
            .setSmallIcon(R.drawable.ic_music_note)
            .setContentIntent(sessionActivity ?: PendingIntent.getActivity(
                this, 0,
                Intent(this, MainActivity::class.java),
                PendingIntent.FLAG_IMMUTABLE,
            ))
            .setCategory(NotificationCompat.CATEGORY_TRANSPORT)
            .setOnlyAlertOnce(true)
            .setOngoing(false)
        return builder
            .setStyle(
                androidx.media.app.NotificationCompat.MediaStyle(builder)
                    .setMediaSession(session.sessionToken)
                    .setShowCancelButton(false)
            )
            .build()
    }

    // ------------------------------------------------------------------
    // native player callbacks
    // ------------------------------------------------------------------

    private val playerListener = object : NativePlayer.Listener {
        override fun onTrackChanged(track: Track, state: ReleaseState) {
            curTrack = track
            curState = state
            // a new release clears stale art until its own art loads
            val release = track.release.ifBlank { state.release }
            if (artForRelease != release) {
                artForRelease = release
                artBitmap = null
            }
            session.setMetadata(buildMeta(track, state, release))
            session.setPlaybackState(
                basePlaybackState(
                    if (player.isPlaying) PlaybackStateCompat.STATE_PLAYING
                    else PlaybackStateCompat.STATE_PAUSED,
                    player.positionMs(),
                )
            )
            persist()
            loadArtwork(track, release)
            trackListener?.invoke(track, state)
        }

        override fun onPlayStateChanged(playing: Boolean) {
            session.setPlaybackState(
                basePlaybackState(
                    if (playing) PlaybackStateCompat.STATE_PLAYING
                    else PlaybackStateCompat.STATE_PAUSED,
                    player.positionMs(),
                )
            )
            persist()
        }

        override fun onTrackEnded() = Unit
    }

    // metadata is rebuilt from scratch (never mutated) so old artwork from a
    // previous release can never leak into the next one
    private var curTrack: Track? = null
    private var curState: ReleaseState? = null
    private var artForRelease: String? = null
    private var artBitmap: android.graphics.Bitmap? = null

    private fun buildMeta(
        track: Track,
        state: ReleaseState,
        release: String,
    ): MediaMetadataCompat {
        val (artist, title) = splitTrack(track.track)
        val album = release.replace(Regex("-"), " ")
        val b = MediaMetadataCompat.Builder()
            .putString(MediaMetadataCompat.METADATA_KEY_TITLE, title)
            .putString(MediaMetadataCompat.METADATA_KEY_ARTIST, artist)
            .putString(MediaMetadataCompat.METADATA_KEY_ALBUM, album)
            .putLong(MediaMetadataCompat.METADATA_KEY_TRACK_NUMBER, (track.id + 1).toLong())
            .putLong(MediaMetadataCompat.METADATA_KEY_NUM_TRACKS, state.tracks.size.toLong())
        val bmp = if (artForRelease == release) artBitmap else null
        if (bmp != null) b.putBitmap(MediaMetadataCompat.METADATA_KEY_ALBUM_ART, bmp)
        return b.build()
    }

    /** Fetch the release's album-art.jpg (optional) for the notification. */
    private fun loadArtwork(track: Track, release: String) {
        val base = Prefs.base(this)
        val dir = track.path.substringBeforeLast('/')
        if (dir.isBlank() || release.isBlank()) return
        val artUrl = "$base$dir/album-art.jpg"
        artworkJob?.cancel()
        artworkJob = scope.launch(Dispatchers.IO) {
            val bmp = try {
                Artwork.load(this@PlayerService, artUrl)
            } catch (e: Exception) {
                Log.w(TAG, "artwork failed: $e")
                null
            }
            if (bmp != null && artForRelease == release) {
                artBitmap = bmp
                withContext(Dispatchers.Main) {
                    val t = curTrack
                    val s = curState
                    if (t != null && s != null) {
                        session.setMetadata(buildMeta(t, s, release))
                    }
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // ticker / persistence
    // ------------------------------------------------------------------

    private fun tick() {
        if (!player.started) return
        val state = if (player.isPlaying) PlaybackStateCompat.STATE_PLAYING
        else PlaybackStateCompat.STATE_PAUSED
        session.setPlaybackState(basePlaybackState(state, player.positionMs()))
        val now = System.currentTimeMillis()
        if (now - lastPersist > 10_000) persist()
    }

    private fun persist() {
        if (!player.started) return
        Prefs.setResume(this, player.snapshot().toString())
        lastPersist = System.currentTimeMillis()
    }

    // ------------------------------------------------------------------
    // controls
    // ------------------------------------------------------------------

    fun play() = player.play()

    fun pause() = player.pause()

    /** Media keys can arrive both as a broadcast (MediaButtonReceiver) and a
     *  key event; coalesce rapid duplicates. */
    fun next() {
        val n = System.currentTimeMillis()
        if (n - lastNav < 400) return
        lastNav = n
        player.next()
    }

    fun prev() {
        val n = System.currentTimeMillis()
        if (n - lastNav < 400) return
        lastNav = n
        player.prev()
    }

    fun isPlaying(): Boolean = player.isPlaying

    fun currentHash(): String = player.hash()

    /** The web page picked a track: adopt it and play the real stream. */
    fun onWebTrack(url: String, snap: JSONObject) {
        player.onWebTrack(url, snap)
    }

    /** Switch the server (menu > server): re-point the api at the new base. */
    fun reconfigure(base: String) {
        player.stop()
        player.api = MutinyApi(base)
        Prefs.setResume(this, "")
        Prefs.setHash(this, "")
    }

    /** Debug: play a bundled local tone (no network) to test the player pipeline. */
    fun playDebugTone() {
        try {
            val dest = File(filesDir, "test_tone.wav")
            if (!dest.exists() || dest.length() == 0L) {
                assets.open("test_tone.wav").use { input ->
                    dest.outputStream().use { output -> input.copyTo(output) }
                }
            }
            val uri = Uri.fromFile(dest).toString()
            Log.e(TAG, "playDebugTone: $uri (${dest.length()} bytes)")
            player.playLocal(uri)
        } catch (e: Exception) {
            Log.e(TAG, "playDebugTone failed: $e")
        }
    }

    /** Debug: dump the live ExoPlayer state to logcat. */
    fun dumpDebugState() {
        player.dumpState()
    }

    /** Stop playback, clear the resume snapshot, and stop the service. */
    fun stopPlayback() {
        Log.d(TAG, "stopPlayback")
        player.stop()
        Prefs.setResume(this, "")
        session.setPlaybackState(basePlaybackState(PlaybackStateCompat.STATE_STOPPED, 0L))
        stopForeground(STOP_FOREGROUND_REMOVE)
        stopSelf()
    }

    private fun ensureChannel() {
        if (Build.VERSION.SDK_INT >= 26) {
            val nm = getSystemService(NotificationManager::class.java)
            if (nm.getNotificationChannel(NOTIF_CHANNEL) == null) {
                nm.createNotificationChannel(
                    NotificationChannel(
                        NOTIF_CHANNEL,
                        getString(R.string.notif_channel),
                        NotificationManager.IMPORTANCE_LOW,
                    )
                )
            }
        }
    }

    /** Debug: play the tone through a brand-new standalone ExoPlayer. */
    fun rawTone() {
        try {
            val dest = File(filesDir, "test_tone.wav")
            if (!dest.exists() || dest.length() == 0L) {
                assets.open("test_tone.wav").use { input ->
                    dest.outputStream().use { output -> input.copyTo(output) }
                }
            }
            val uri = Uri.fromFile(dest).toString()
            Log.e(TAG, "rawTone: $uri")
            player.rawTest(uri)
        } catch (e: Exception) {
            Log.e(TAG, "rawTone failed: $e")
        }
    }

    /** web track names are "artist - title-<bandcamp id>"; drop the id for display. */
    private fun splitTrack(s: String): Pair<String, String> {
        val i = s.indexOf(" - ")
        val (a, t) = if (i > 0) s.substring(0, i) to s.substring(i + 3) else s to s
        return a to t.replace(Regex("-(\\d+)$"), "")
    }
}