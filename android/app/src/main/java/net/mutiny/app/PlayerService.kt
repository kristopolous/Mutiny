package net.mutiny.app

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Context
import android.content.Intent
import android.content.pm.ServiceInfo
import android.graphics.Bitmap
import android.net.wifi.WifiManager
import android.os.Binder
import android.os.Build
import android.os.IBinder
import android.os.PowerManager
import android.support.v4.media.MediaMetadataCompat
import android.support.v4.media.session.MediaSessionCompat
import android.support.v4.media.session.PlaybackStateCompat
import androidx.core.app.NotificationCompat
import androidx.core.content.ContextCompat
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import org.json.JSONObject

/**
 * Foreground media service that keeps the process (and so the WebView that
 * plays the music) alive in the background.
 *
 * It plays nothing itself. The page's <audio> element does; this service
 * holds the wake/wifi locks while it plays, mirrors its state into a
 * [MediaSessionCompat] (notification, lock screen, headset/BT keys) and
 * forwards those controls back into the page through [controller].
 */
class PlayerService : Service() {

    /** Implemented by the activity: runs the action inside the page. */
    interface Controller {
        fun action(name: String)
        fun seek(ms: Long)
    }

    companion object {
        const val ACTION_STOP = "net.mutiny.app.ACTION_STOP"
        const val NOTIF_CHANNEL = "mutiny_music"
        const val NOTIF_ID = 42

        @Volatile var instance: PlayerService? = null

        fun start(ctx: Context) {
            ContextCompat.startForegroundService(ctx, Intent(ctx, PlayerService::class.java))
        }

        fun stop(ctx: Context) {
            ctx.startService(Intent(ctx, PlayerService::class.java).setAction(ACTION_STOP))
        }
    }

    var controller: Controller? = null

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Main.immediate)
    private lateinit var session: MediaSessionCompat
    private lateinit var wakeLock: PowerManager.WakeLock
    private lateinit var wifiLock: WifiManager.WifiLock
    private var sessionActivity: PendingIntent? = null

    private var playing = false
    private var title = ""
    private var artist = ""
    private var album = ""
    private var duration = 0L
    private var artUrl = ""
    private var art: Bitmap? = null
    private var artJob: Job? = null

    override fun onCreate() {
        super.onCreate()
        instance = this
        val pm = getSystemService(PowerManager::class.java)
        wakeLock = pm.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "mutiny:play").apply {
            setReferenceCounted(false)
        }
        @Suppress("DEPRECATION")
        wifiLock = applicationContext.getSystemService(WifiManager::class.java)
            .createWifiLock(WifiManager.WIFI_MODE_FULL_HIGH_PERF, "mutiny:play").apply {
                setReferenceCounted(false)
            }
        sessionActivity = PendingIntent.getActivity(
            this, 0,
            Intent(this, MainActivity::class.java).addFlags(Intent.FLAG_ACTIVITY_SINGLE_TOP),
            PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT,
        )
        session = MediaSessionCompat(this, "mutiny").apply {
            setSessionActivity(sessionActivity!!)
            setCallback(object : MediaSessionCompat.Callback() {
                override fun onPlay() = act("play")
                override fun onPause() = act("pause")
                override fun onSkipToNext() = act("nexttrack")
                override fun onSkipToPrevious() = act("previoustrack")
                override fun onStop() = act("pause")
                override fun onSeekTo(pos: Long) {
                    this@PlayerService.controller?.seek(pos)
                }
            })
            isActive = true
        }
        ensureChannel()
    }

    override fun onBind(intent: Intent): IBinder = Binder()

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        if (intent?.action == ACTION_STOP) {
            act("pause")
            releaseLocks()
            stopForeground(STOP_FOREGROUND_REMOVE)
            stopSelf()
            return START_NOT_STICKY
        }
        if (Build.VERSION.SDK_INT >= 29) {
            startForeground(NOTIF_ID, notification(), ServiceInfo.FOREGROUND_SERVICE_TYPE_MEDIA_PLAYBACK)
        } else {
            startForeground(NOTIF_ID, notification())
        }
        // without the page there is nothing to play; don't come back alone
        return START_NOT_STICKY
    }

    override fun onDestroy() {
        releaseLocks()
        session.release()
        scope.cancel()
        if (instance === this) instance = null
        super.onDestroy()
    }

    fun act(name: String) {
        controller?.action(name)
    }

    fun isPlaying(): Boolean = playing

    /** The page's <audio> changed (see MainActivity.HOOK). */
    fun onWebState(s: JSONObject) {
        playing = s.optBoolean("playing")
        val pos = s.optLong("pos")
        val t = s.optString("title")
        val a = s.optString("artist")
        val al = s.optString("album")
        val dur = s.optLong("dur")
        val newArt = s.optString("art")
        val metaChanged = t != title || a != artist || al != album || dur != duration
        title = t
        artist = a
        album = al
        duration = dur
        if (newArt != artUrl) {
            artUrl = newArt
            art = null
            loadArt(newArt)
        }
        if (metaChanged) session.setMetadata(meta())
        session.setPlaybackState(
            PlaybackStateCompat.Builder()
                .setState(
                    if (playing) PlaybackStateCompat.STATE_PLAYING else PlaybackStateCompat.STATE_PAUSED,
                    pos,
                    if (playing) 1f else 0f,
                )
                .setActions(
                    PlaybackStateCompat.ACTION_PLAY or
                        PlaybackStateCompat.ACTION_PAUSE or
                        PlaybackStateCompat.ACTION_PLAY_PAUSE or
                        PlaybackStateCompat.ACTION_SKIP_TO_NEXT or
                        PlaybackStateCompat.ACTION_SKIP_TO_PREVIOUS or
                        PlaybackStateCompat.ACTION_SEEK_TO
                )
                .build()
        )
        if (playing) {
            if (!wakeLock.isHeld) wakeLock.acquire()
            if (!wifiLock.isHeld) wifiLock.acquire()
        } else {
            // a paused page keeps its locks briefly: the page pauses between
            // tracks while it fetches the next one
            scope.launch {
                kotlinx.coroutines.delay(30_000)
                if (!playing) releaseLocks()
            }
        }
        getSystemService(NotificationManager::class.java).notify(NOTIF_ID, notification())
    }

    private fun releaseLocks() {
        if (wakeLock.isHeld) wakeLock.release()
        if (wifiLock.isHeld) wifiLock.release()
    }

    private fun meta(): MediaMetadataCompat {
        val b = MediaMetadataCompat.Builder()
            .putString(MediaMetadataCompat.METADATA_KEY_TITLE, title)
            .putString(MediaMetadataCompat.METADATA_KEY_ARTIST, artist)
            .putString(MediaMetadataCompat.METADATA_KEY_ALBUM, album)
        if (duration > 0) b.putLong(MediaMetadataCompat.METADATA_KEY_DURATION, duration)
        art?.let { b.putBitmap(MediaMetadataCompat.METADATA_KEY_ALBUM_ART, it) }
        return b.build()
    }

    private fun loadArt(url: String) {
        artJob?.cancel()
        if (url.isBlank()) return
        artJob = scope.launch {
            val bmp = try {
                Artwork.load(this@PlayerService, url)
            } catch (e: Exception) {
                null
            }
            if (bmp != null && url == artUrl) {
                art = bmp
                session.setMetadata(meta())
                getSystemService(NotificationManager::class.java).notify(NOTIF_ID, notification())
            }
        }
    }

    private fun notification(): Notification {
        val builder = NotificationCompat.Builder(this, NOTIF_CHANNEL)
            .setSmallIcon(R.drawable.ic_music_note)
            .setContentTitle(title.ifBlank { getString(R.string.app_name) })
            .setContentText(artist)
            .setLargeIcon(art)
            .setContentIntent(sessionActivity)
            .setCategory(NotificationCompat.CATEGORY_TRANSPORT)
            .setVisibility(NotificationCompat.VISIBILITY_PUBLIC)
            .setOnlyAlertOnce(true)
            .addAction(R.drawable.ic_skip_previous, getString(R.string.menu_prev), button(PlaybackStateCompat.ACTION_SKIP_TO_PREVIOUS))
            .addAction(
                if (playing) R.drawable.ic_pause else R.drawable.ic_play,
                getString(if (playing) R.string.menu_pause else R.string.menu_play),
                button(PlaybackStateCompat.ACTION_PLAY_PAUSE),
            )
            .addAction(R.drawable.ic_skip_next, getString(R.string.menu_next), button(PlaybackStateCompat.ACTION_SKIP_TO_NEXT))
        return builder
            .setStyle(
                androidx.media.app.NotificationCompat.MediaStyle()
                    .setMediaSession(session.sessionToken)
                    .setShowActionsInCompactView(0, 1, 2)
            )
            .build()
    }

    /** notification buttons go straight to the session callback */
    private fun button(action: Long): PendingIntent {
        val key = when (action) {
            PlaybackStateCompat.ACTION_SKIP_TO_NEXT -> android.view.KeyEvent.KEYCODE_MEDIA_NEXT
            PlaybackStateCompat.ACTION_SKIP_TO_PREVIOUS -> android.view.KeyEvent.KEYCODE_MEDIA_PREVIOUS
            else -> android.view.KeyEvent.KEYCODE_MEDIA_PLAY_PAUSE
        }
        val i = Intent(this, MediaKeyReceiver::class.java)
            .putExtra(Intent.EXTRA_KEY_EVENT, android.view.KeyEvent(android.view.KeyEvent.ACTION_DOWN, key))
        return PendingIntent.getBroadcast(this, key, i, PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT)
    }

    /** a key from the notification buttons */
    fun onKey(code: Int) {
        when (code) {
            android.view.KeyEvent.KEYCODE_MEDIA_NEXT -> act("nexttrack")
            android.view.KeyEvent.KEYCODE_MEDIA_PREVIOUS -> act("previoustrack")
            android.view.KeyEvent.KEYCODE_MEDIA_PLAY -> act("play")
            android.view.KeyEvent.KEYCODE_MEDIA_PAUSE -> act("pause")
            else -> act(if (playing) "pause" else "play")
        }
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
}
