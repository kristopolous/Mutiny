package net.mutiny.app

import android.content.Context
import android.os.Handler
import android.os.Looper
import android.webkit.JavascriptInterface
import org.json.JSONObject

/**
 * The web page -> native channel.
 *
 * The page's <audio id=player> element is stubbed out by injected JS (see
 * [MainActivity]); whenever the page picks a track it calls
 * [onTrack] with the url the (stubbed) element would play plus a snapshot of
 * the page's state machine (_my/_next/q/format/filter/loop). The native
 * player adopts it and plays the real stream.
 */
class WebBridge(private val ctx: Context, private val onTrack: (String, JSONObject) -> Unit) {

    private val main = Handler(Looper.getMainLooper())

    /**
     * @param url    absolute url the web's audio element is set to
     * @param snap   JSON: { q, f, fl, loop, my: _my, next: _next, hash }
     */
    @JavascriptInterface
    @Suppress("UNUSED_PARAMETER")
    fun onTrack(url: String, snap: String) {
        val o = try {
            JSONObject(snap)
        } catch (e: Exception) {
            JSONObject()
        }
        main.post { onTrack(url, o) }
    }
}