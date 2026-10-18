package net.mutiny.app

import okhttp3.OkHttpClient
import okhttp3.Request
import org.json.JSONArray
import org.json.JSONObject
import java.util.concurrent.TimeUnit

/** One track as returned by the playlist db. */
data class Track(
    val track: String,
    val path: String,
    val id: Int,
    val label: String = "",
    val release: String = "",
)

/**
 * A release-state payload, exactly the shape the php backend returns for the
 * "release" / "+label" / "-release" / "+release" keys:
 * { label, release, number, count, track_ix, trackList: [Track...] }
 * (the backend also merges the track fields of track_ix into the payload).
 */
data class ReleaseState(
    val label: String,
    val release: String,
    val number: Int,
    val count: Int,
    val trackIx: Int,
    val tracks: List<Track>,
) {
    val track: Track? get() = tracks.getOrNull(trackIx)
}

/** The full get_playlist.php response. */
data class PlaylistData(
    val release: ReleaseState?,
    val plusLabel: ReleaseState?,
    val minusRelease: ReleaseState?,
    val plusRelease: ReleaseState?,
)

class ApiError(override val message: String) : Exception(message)

/**
 * Client for the two php endpoints the web frontend uses
 * (web/scripts.js: remote() -> get_playlist.php, lookup() -> url2mp3.php).
 *
 * The parameters are embedded exactly the way the web page does, so the
 * native player can drive the same state machine as the web app.
 */
class MutinyApi(base: String) {

    var base: String = base.trimEnd('/')

    private val http = OkHttpClient.Builder()
        .connectTimeout(15, TimeUnit.SECONDS)
        .readTimeout(60, TimeUnit.SECONDS)
        .build()

    /**
     * Mirrors web/scripts.js remote():
     *   fetch("get_playlist.php?" + [filter, level, q, release, label, ...append].join("&"))
     *
     * q is the *already encoded* search string (the web's _qstr), embedded raw,
     * just like the web does.
     */
    fun getPlaylist(
        filter: Int,
        level: Int,
        q: String,
        release: String,
        label: String,
        action: String,
        orig: String,
    ): PlaylistData {
        val url = base + "/get_playlist.php?" + buildString {
            append("filter=").append(filter)
            append("&level=").append(level)
            append("&q=").append(q)
            append("&release=").append(release)
            append("&label=").append(label)
            append("&action=").append(action)
            append("&orig=").append(orig)
        }
        val text = get(url) ?: throw ApiError("get_playlist failed: $url")
        val root = JSONObject(text)
        val rel = root.optJSONObject("release")
            ?: throw ApiError("get_playlist: no release in response")
        return PlaylistData(
            release = parseRelease(rel),
            plusLabel = root.optJSONObject("+label")?.let(::parseRelease),
            minusRelease = root.optJSONObject("-release")?.let(::parseRelease),
            plusRelease = root.optJSONObject("+release")?.let(::parseRelease),
        )
    }

    /**
     * Mirrors web/scripts.js lookup():
     *   fetch(`url2mp3.php?q=${_format}&path=${encodeURIComponent(play.path)}&u=${path_to_url(path)}`)
     *
     * @return the playable url (absolute), or null on failure.
     */
    fun trackUrl(level: Int, path: String): String? {
        val url = base + "/url2mp3.php?" + buildString {
            append("q=").append(level)
            append("&path=").append(encode(path))
            append("&u=").append(encode(bandcampUrl(path)))
        }
        return try {
            val t = get(url)?.trim() ?: return null
            when {
                t.isEmpty() -> null
                // absolute (e.g. the bandcamp fallback)
                t.startsWith("http") -> t
                // bandcamp fallback: url2mp3.php prints
                // "/pl/fw.php?u=<mp3 with ? -> %3f, & -> %26>". The live /pl route
                // 404s, so play the underlying bandcamp mp3 directly instead —
                // ExoPlayer fetches it fine without the proxy.
                t.startsWith("/pl/fw.php?u=") -> {
                    val u = t.substringAfter("/pl/fw.php?u=")
                        .replace("%3f", "?").replace("%26", "&")
                        .replace("%3F", "?").replace("%26", "&")
                    if (u.startsWith("http")) u else null
                }
                // relative path: the web escapes only the filename segment because
                // "file names can be really weird". Mirror that exactly.
                else -> {
                    val parts = t.split("/")
                    val fname = parts.last()
                    val escaped = if (fname.isEmpty()) fname else encode(fname)
                    val joined = (parts.dropLast(1) + escaped).joinToString("/")
                    val abs = if (joined.startsWith("/")) joined else "/" + joined
                    base + abs
                }
            }
        } catch (e: Exception) {
            null
        }
    }

    /**
     * The web's shim reports the url its <audio> element *would* have played.
     * For the bandcamp fallback that url is the /pl proxy, which 404s on the
     * live server (and the web escaped its query as a path segment), so
     * unwrap it back to the real media url.
     */
    fun unwrapWebUrl(url: String): String {
        if (!url.contains("fw.php")) return url
        val path = try {
            java.net.URI(url).path // decoded: "/pl/fw.php?u=<url with %3f/%26>"
        } catch (e: Exception) {
            null
        } ?: return url
        val q = path.indexOf("?u=")
        if (q < 0) return url
        val u = path.substring(q + 3)
            .replace("%3f", "?").replace("%3F", "?").replace("%26", "&")
        return if (u.startsWith("http")) u else url
    }

    /** Mirrors path_to_url in web/scripts.js. */
    fun bandcampUrl(path: String): String {
        val m = Regex("([0-9]*)\\.mp3$").find(path) ?: return ""
        return "https://bandcamp.com/EmbeddedPlayer/size=large/bgcol=333333/linkcol=ffffff/transparent=true/track=" +
            m.groupValues[1]
    }

    private fun parseRelease(o: JSONObject): ReleaseState {
        val list = o.optJSONArray("trackList") ?: JSONArray()
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

    private fun get(url: String): String? = try {
        val req = Request.Builder().url(url).header("User-Agent", "mutiny-android/1").build()
        http.newCall(req).execute().use { resp ->
            if (!resp.isSuccessful) return null
            resp.body?.string()
        }
    } catch (e: Exception) {
        null
    }

    private fun encode(s: String): String =
        java.net.URLEncoder.encode(s, "UTF-8").replace("+", "%20")
}