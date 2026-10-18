package net.mutiny.app

import android.content.Context
import android.net.Uri
import android.util.Log
import androidx.media3.common.AudioAttributes
import androidx.media3.common.C
import androidx.media3.common.MediaItem
import androidx.media3.exoplayer.ExoPlayer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.json.JSONObject

/**
 * The native playback engine.
 *
 * It mirrors the state machine of the web app (web/scripts.js):
 *
 *   lookup()  -> api.trackUrl()     (url2mp3.php)
 *   remote()  -> fetchPlaylist()    (get_playlist.php)
 *   d(skip)   -> d(skip, orig)      (the media-key / track-end state machine)
 *
 * The WebView's <audio> element is replaced by a silent stub (see
 * [MainActivity]), so this engine is the *only* thing that produces sound.
 * It keeps running while the WebView is stopped, frozen or destroyed, which
 * is the whole point of the app: background playback that the browser would
 * stop.
 *
 * The web page remains the UI and (while it is alive) the driver: whenever it
 * picks a track it reports it through the JS bridge ([Listener.onWebTrack]),
 * and this engine adopts it. When the web page is frozen (background) this
 * engine drives itself: hardware media keys and track-end call [d], exactly
 * like the web's own media-key handlers do.
 */
class NativePlayer(
    private val ctx: Context,
    var api: MutinyApi,
    private val listener: Listener,
) {
    interface Listener {
        /** A track is now current (metadata changed). */
        fun onTrackChanged(track: Track, state: ReleaseState)
        /** Playback started / stopped. */
        fun onPlayStateChanged(playing: Boolean)
        /** The current track ended (used by the service for logging etc). */
        fun onTrackEnded()
    }

    // ---- state mirroring the web globals --------------------------------
    @Volatile var format: Int = 2        // web's _format: 0 opus / 1 heaac / 2 mp3
    @Volatile var filter: Int = 1        // web's _filter: 0 recent / 1 all
    @Volatile var q: String = ""         // web's _qstr (already url-encoded)
    @Volatile var loop: Boolean = false  // web's _lock.loop

    @Volatile var state: ReleaseState? = null          // web's _my
    @Volatile var boundaries: Map<String, ReleaseState> = emptyMap() // web's _next (+label/-release/+release)
    @Volatile var track: Track? = null                 // web's _track
    @Volatile var currentUrl: String? = null
    @Volatile var started: Boolean = false             // a track has been requested
    @Volatile var busy: Boolean = false                // mid-navigation (web: #controls.disabled)
    @Volatile var isPlaying: Boolean = false
    private var lastEmittedPath: String? = null        // dedup for track-changed emits

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Main.immediate)
    private var navJob: Job? = null
    private var exo: ExoPlayer? = null
    private val urlCache = HashMap<String, String>()   // web's _db: path -> playable url

    // =====================================================================
    // web-driven (bridge)
    // =====================================================================

    /**
     * The web page picked a track. [url] is the absolute url its <audio>
     * element would play (the stubbed one). Adopt the web's state machine
     * and play the real thing.
     */
    fun onWebTrack(url: String, snap: JSONObject) {
        try {
            // mirror the web globals from the snapshot
            q = snap.optString("q", q)
            format = snap.optInt("f", format)
            filter = snap.optInt("fl", filter)
            loop = snap.optInt("loop", if (loop) 1 else 0) == 1
            val my = snap.optJSONObject("my")
            if (my != null) {
                state = parseRelease(my)
                track = state?.track ?: track
            }
            val next = snap.optJSONObject("next")
            if (next != null) boundaries = parseBoundaries(next)
        } catch (e: Exception) {
            Log.w(TAG, "onWebTrack: bad snapshot: $e")
        }
        // make sure the notification reflects the web's current track
        emitTrackChanged()
        // unwrap the broken /pl proxy url (bandcamp fallback tracks)
        val real = api.unwrapWebUrl(url)
        // re-selecting the same url (play_url re-fires) must not restart playback
        if (real == currentUrl) {
            val p = exo
            if (p == null || p.mediaItemCount == 0) {
                // player has no item (edge: service recreated) -> full play
                playUrl(real, track, 0)
            } else if (!isPlaying) {
                play()
            }
            return
        }
        playUrl(real, track, 0)
    }

    // =====================================================================
    // transport
    // =====================================================================

    fun play() {
        val p = ensureExo()
        if (p.mediaItemCount == 0) {
            Log.e(TAG, "play() with no media item — currentUrl=$currentUrl")
            return
        }
        p.play()
    }

    /** Debug: play a local file (no network) to isolate player vs stream issues. */
    fun playLocal(fileUri: String) {
        playUrl(fileUri, null, 0)
    }

    /** Debug: dump the live ExoPlayer state to logcat. */
    fun dumpState() {
        val p = exo
        if (p == null) {
            Log.e(TAG, "STATE: exo=null started=$started")
            return
        }
        Log.e(TAG, "STATE: playbackState=${stateName(p.playbackState)}(raw=${p.playbackState}) isPlaying=${p.isPlaying} playWhenReady=${p.playWhenReady} position=${p.currentPosition} duration=${p.duration} items=${p.mediaItemCount} error=${p.playerError} consts[IDLE=${androidx.media3.common.Player.STATE_IDLE} BUF=${androidx.media3.common.Player.STATE_BUFFERING} RDY=${androidx.media3.common.Player.STATE_READY} END=${androidx.media3.common.Player.STATE_ENDED}]")
    }

    /**
     * Debug: build a brand-new, standalone ExoPlayer (independent of [exo]) and
     * play [fileUri]. Aggressively logs every state change. If this plays but
     * [exo] does not, the problem is in the playUrl flow; if it also fails,
     * the problem is the player/environment.
     */
    fun rawTest(fileUri: String) {
        Log.e(TAG, "RAW: building standalone player for $fileUri")
        val r = ExoPlayer.Builder(ctx)
            .setAudioAttributes(
                AudioAttributes.Builder()
                    .setUsage(C.USAGE_MEDIA)
                    .setContentType(C.AUDIO_CONTENT_TYPE_MUSIC)
                    .build(),
                false,
            )
            .build()
        rawPlayer = r
        r.addListener(object : androidx.media3.common.Player.Listener {
            override fun onPlaybackStateChanged(playbackState: Int) {
                Log.e(TAG, "RAW: state -> ${stateName(playbackState)} (raw=$playbackState)")
            }
            override fun onIsPlayingChanged(playing: Boolean) {
                Log.e(TAG, "RAW: isPlaying=$playing pos=${r.currentPosition}")
            }
            override fun onPlayerError(error: androidx.media3.common.PlaybackException) {
                Log.e(TAG, "RAW: error=${error.errorCodeName} ${error.message}")
            }
        })
        r.addMediaItem(androidx.media3.common.MediaItem.fromUri(Uri.parse(fileUri)))
        Log.e(TAG, "RAW: preparing + playing")
        r.prepare()
        r.play()
        // sample state for the next ~8s
        val handler = android.os.Handler(android.os.Looper.getMainLooper())
        for (i in 1..8) {
            handler.postDelayed({
                Log.e(TAG, "RAW: t=${i}s state=${stateName(r.playbackState)} isPlaying=${r.isPlaying} pos=${r.currentPosition} dur=${r.duration}")
            }, i * 1000L)
        }
    }

    /** Standalone test player (debug). */
    private var rawPlayer: androidx.media3.exoplayer.ExoPlayer? = null

    fun pause() {
        exo?.pause()
    }

    /** web: the 'stop' media action -> playbackState none. */
    fun stop() {
        Log.e(TAG, ">>> stop() called\n" + Throwable().stackTraceToString())
        navJob?.cancel()
        navJob = null
        exo?.let {
            it.stop()
        }
        currentUrl = null
        started = false
    }

    fun positionMs(): Long = exo?.currentPosition?.toLong() ?: 0L

    fun durationMs(): Long {
        val d = exo?.duration ?: return 0L
        return if (d >= 0L) d else 0L
    }

    fun seekTo(ms: Long) {
        exo?.seekTo(ms.coerceAtLeast(0))
    }

    // =====================================================================
    // the web's d(skip, orig) state machine
    // =====================================================================

    fun next() = d("+track", null)

    fun prev() = d("-track", null)

    /**
     * Mirrors web/scripts.js d(skip, orig):
     *
     *   - within a release:  step the trackList (no network, like the web)
     *   - at a release end:  escalate to +release / -release
     *   - at a label end:    escalate to +label / -label
     *   - release/label:     play the cached boundary track, then remote()
     *   - with loop on:      wrap inside the release instead of leaving it
     */
    fun d(skip: String, orig: String?) {
        val st = state
        if (st == null || st.tracks.isEmpty()) {
            // no state yet (cold start / after bail) -> ask the backend
            fetchPlaylist(skip, orig ?: skip, onDone = { if (!it) bail() })
            return
        }
        if (busy) return

        val ttl = st.tracks.size
        val next: Any? = when (skip) {
            "+track" -> st.tracks[(st.trackIx + 1) % ttl]
            "-track" -> st.tracks[(ttl + st.trackIx - 1) % ttl]
            else -> boundaries[skip]
        }

        if (next != null) {
            // the web's escalation conditions
            val escalate = !loop && when {
                next is Track && skip == "+track" -> next.id == 0
                next is Track && skip == "-track" -> next.id >= (track?.id ?: -1)
                next is ReleaseState && skip == "+release" -> next.number == 0
                next is ReleaseState && skip == "-release" -> next.number >= st.number
                else -> false
            }
            if (escalate) {
                val word = skip[0].toString() + if (skip[1] == 't') "release" else "label"
                d(word, orig ?: skip)
                return
            }

            when (val n = next) {
                is Track -> {
                    // "+track"/"-track": local, no network (web: return play_url(next))
                    playTrack(n, st)
                    return
                }
                is ReleaseState -> {
                    if (orig == null || skip == orig) {
                        // snappy: play the cached boundary track right away ...
                        val t = n.track ?: n.tracks.firstOrNull()
                        if (t != null) playTrack(t, n)
                    }
                    // ... then remote() to refresh state + boundaries (web falls
                    // through to remote for release/label)
                    fetchPlaylist(skip, orig ?: skip)
                    return
                }
            }
        }

        // no cached next -> ask the backend (web: return remote(...))
        fetchPlaylist(skip, orig ?: skip, onDone = { if (!it) bail() })
    }

    /** ExoPlayer track ended. web: _DOM.player.onended -> d("+track"). */
    fun onEnded() {
        listener.onTrackEnded()
        d("+track", null)
    }

    /** The web's location.hash for the current state: #label/release/track-ix/q/f/fl */
    fun hash(): String {
        val st = state
        val t = track
        if (st == null || t == null) return ""
        return "#${st.label}/${st.release}/${t.id}/${q}/${format}/${filter}"
    }

    // =====================================================================
    // internals
    // =====================================================================

    /** web: play_url(play) — sets the current track, hash, and plays the url. */
    private fun playTrack(t: Track, st: ReleaseState) {
        state = st.copy(trackIx = st.tracks.indexOf(t).coerceAtLeast(0))
        track = t
        emitTrackChanged()
        // resolve the playable url exactly like the web's lookup()
        val fmt = format
        navJob?.cancel()
        navJob = scope.launch {
            busy = true
            var url = urlCache[t.path]
            if (url == null) {
                url = withContext(Dispatchers.IO) { api.trackUrl(fmt, t.path) }
                if (url != null) urlCache[t.path] = url
            }
            busy = false
            if (url == null) {
                Log.e(TAG, "playTrack: url2mp3 failed for ${t.path}; retrying in 2s")
                scope.launch {
                    delay(2000)
                    val retry = withContext(Dispatchers.IO) { api.trackUrl(fmt, t.path) }
                    if (retry != null) {
                        urlCache[t.path] = retry
                        playUrl(retry, t, 0)
                    }
                }
                return@launch
            }
            playUrl(url, t, 0)
        }
    }

    /** Start (or restart) playing [url] for [t] (nullable when adopting the web's src). */
    private fun playUrl(url: String, t: Track? = null, posMs: Long = 0) {
        Log.e(TAG, "playUrl: $url (pos=$posMs) exo=${System.identityHashCode(exo)}")
        if (t != null) {
            track = t
            emitTrackChanged()
        }
        currentUrl = url
        started = true
        val p = ensureExo()
        Log.e(TAG, "playUrl: exo=${System.identityHashCode(p)} items=${p.mediaItemCount} state=${p.playbackState}")
        val same = p.currentMediaItem?.localConfiguration?.uri?.toString() == url
        if (!same) {
            p.clearMediaItems()
            p.addMediaItem(MediaItem.fromUri(Uri.parse(url)))
        }
        // prepare() is a no-op if already prepared; it is required after
        // clearMediaItems() to actually kick off loading of the new item.
        p.prepare()
        p.seekTo(posMs.coerceAtLeast(0))
        p.play()
    }

    /**
     * web: remote([action, orig]) — fetches get_playlist.php, adopts
     * `_my = data.release`, `_next = data`, and plays `_my.trackList[_my.track_ix]`.
     */
    private fun fetchPlaylist(action: String, orig: String, onDone: (Boolean) -> Unit = {}) {
        val st = state
        navJob?.cancel()
        navJob = scope.launch(Dispatchers.IO) {
            busy = true
            try {
                val data = api.getPlaylist(filter, format, q, st?.release ?: "", st?.label ?: "", action, orig)
                val rel = data.release
                if (rel == null || rel.trackIx == -1) {
                    busy = false
                    onDone(false)
                    return@launch
                }
                // web: _my = data.release; _next = data (minus the release key)
                state = rel
                boundaries = mapOf(
                    "+label" to data.plusLabel,
                    "-release" to data.minusRelease,
                    "+release" to data.plusRelease,
                ).filterValues { it != null }.mapValues { it.value!! }
                // web: play_url(_my.trackList[_my.track_ix])
                val t = rel.track ?: rel.tracks.firstOrNull()
                busy = false
                withContext(Dispatchers.Main) {
                    if (t != null) playTrack(t, rel)
                    onDone(true)
                }
            } catch (e: Exception) {
                busy = false
                Log.e(TAG, "fetchPlaylist($action): $e")
                withContext(Dispatchers.Main) { onDone(false) }
            }
        }
    }

    /**
     * web: bail() — resets the hash to a fresh start (track 0, no search,
     * mp3, all) and reloads. Native equivalent: re-fetch from scratch.
     */
    private fun bail() {
        Log.w(TAG, "bail: resetting to a fresh start")
        format = 2
        filter = 1
        q = ""
        state = null
        boundaries = emptyMap()
        track = null
        fetchPlaylist("0", "0")
    }

    // =====================================================================
    // exo
    // =====================================================================

    private fun ensureExo(): ExoPlayer {
        exo?.let { return it }
        val p = ExoPlayer.Builder(ctx)
            .setAudioAttributes(
                AudioAttributes.Builder()
                    .setUsage(C.USAGE_MEDIA)
                    .setContentType(C.AUDIO_CONTENT_TYPE_MUSIC)
                    .build(),
                true,
            )
            .setHandleAudioBecomingNoisy(true)
            .build()
        p.addListener(object : androidx.media3.common.Player.Listener {
            override fun onPlaybackStateChanged(playbackState: Int) {
                when (playbackState) {
                    androidx.media3.common.Player.STATE_ENDED -> onEnded()
                    androidx.media3.common.Player.STATE_IDLE -> {
                        // media3: a failed load ends up in IDLE with playerError set
                        p.playerError?.let { Log.e(TAG, "player error: $it url=$currentUrl") }
                    }
                    else -> Log.e(TAG, "state: ${stateName(playbackState)} url=$currentUrl")
                }
            }

            override fun onIsPlayingChanged(playing: Boolean) {
                Log.e(TAG, "isPlaying: $playing url=$currentUrl")
                isPlaying = playing
                listener.onPlayStateChanged(playing)
            }

            override fun onPlayerError(error: androidx.media3.common.PlaybackException) {
                Log.e(TAG, "onPlayerError: ${error.message} (${error.errorCodeName}) url=$currentUrl")
            }
        })
        p.addAnalyticsListener(androidx.media3.exoplayer.util.EventLogger())
        exo = p
        return p
    }

    private fun stateName(s: Int) = when (s) {
        androidx.media3.common.Player.STATE_IDLE -> "IDLE"
        androidx.media3.common.Player.STATE_BUFFERING -> "BUFFERING"
        androidx.media3.common.Player.STATE_READY -> "READY"
        androidx.media3.common.Player.STATE_ENDED -> "ENDED"
        else -> s.toString()
    }

    fun release() {
        Log.e(TAG, ">>> release() called\n" + Throwable().stackTraceToString())
        navJob?.cancel()
        exo?.release()
        exo = null
        started = false
    }

    // =====================================================================
    // state (re)serialization — used for resuming after process death
    // =====================================================================

    /** Serialize the current state so the service can resume after a kill. */
    fun snapshot(): JSONObject {
        val o = JSONObject()
        o.put("q", q)
        o.put("f", format)
        o.put("fl", filter)
        o.put("loop", if (loop) 1 else 0)
        o.put("my", state?.let { releaseToJson(it) })
        o.put("url", currentUrl ?: "")
        o.put("pos", positionMs())
        o.put("playing", if (isPlaying) 1 else 0)
        o.put("track", track?.let {
            val t = JSONObject()
            t.put("track", it.track)
            t.put("path", it.path)
            t.put("id", it.id)
            t
        })
        return o
    }

    /** Restore from [snapshot]. Plays [posMs] into the current url. */
    fun restore(o: JSONObject) {
        q = o.optString("q", "")
        format = o.optInt("f", 2)
        filter = o.optInt("fl", 1)
        loop = o.optInt("loop", 0) == 1
        state = o.optJSONObject("my")?.let(::parseRelease)
        val t = o.optJSONObject("track")
        track = t?.let {
            Track(
                track = it.optString("track"),
                path = it.optString("path"),
                id = it.optInt("id"),
            )
        }
        val u = o.optString("url", "")
        if (u.isNotEmpty()) {
            playUrl(u, track, o.optLong("pos"))
            if (o.optInt("playing", 0) == 0) pause()
            // fresh process: make sure the session has metadata for the track
            emitTrackChanged()
        }
    }

    private fun emitTrackChanged() {
        val t = track ?: return
        if (t.path == lastEmittedPath) return
        lastEmittedPath = t.path
        val st = state ?: ReleaseState(t.label, t.release, 0, 1, 0, listOf(t))
        listener.onTrackChanged(t, st)
    }

    private fun parseRelease(o: JSONObject): ReleaseState {
        val list = o.optJSONArray("trackList") ?: org.json.JSONArray()
        val tracks = (0 until list.length()).map { i ->
            val t = list.getJSONObject(i)
            Track(
                track = t.optString("track"),
                path = t.optString("path"),
                id = t.optInt("id", i),
                label = t.optString("label"),
                release = t.optString("release"),
            )
        }
        return ReleaseState(
            label = o.optString("label"),
            release = o.optString("release"),
            number = o.optInt("number"),
            count = o.optInt("count"),
            trackIx = o.optInt("track_ix"),
            tracks = tracks,
        )
    }

    private fun parseBoundaries(o: JSONObject): Map<String, ReleaseState> {
        return mapOf(
            "+label" to o.optJSONObject("+label"),
            "-release" to o.optJSONObject("-release"),
            "+release" to o.optJSONObject("+release"),
        ).entries.filter { it.value != null }
            .associate { (k, v) -> k to parseRelease(v!!) }
    }

    companion object {
        private const val TAG = "NativePlayer"

        fun releaseToJson(st: ReleaseState): JSONObject {
            val o = JSONObject()
            o.put("label", st.label)
            o.put("release", st.release)
            o.put("number", st.number)
            o.put("count", st.count)
            o.put("track_ix", st.trackIx)
            val arr = org.json.JSONArray()
            st.tracks.forEach { t ->
                val o2 = JSONObject()
                o2.put("track", t.track)
                o2.put("path", t.path)
                o2.put("id", t.id)
                o2.put("label", t.label)
                o2.put("release", t.release)
                arr.put(o2)
            }
            o.put("trackList", arr)
            return o
        }
    }
}