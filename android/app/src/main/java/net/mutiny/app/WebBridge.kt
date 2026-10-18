package net.mutiny.app

import android.os.Handler
import android.os.Looper
import android.webkit.JavascriptInterface
import org.json.JSONObject

/**
 * The web page -> native channel.
 *
 * The page plays the music itself through its own <audio id=player>. The
 * script in [MainActivity.HOOK] reports that element's state (playing,
 * position, duration, the page's media-session metadata) so the native
 * service can mirror it in the notification / lock screen.
 */
class WebBridge(private val onState: (JSONObject) -> Unit) {

    private val main = Handler(Looper.getMainLooper())

    @JavascriptInterface
    fun state(json: String) {
        val o = try {
            JSONObject(json)
        } catch (e: Exception) {
            return
        }
        main.post { onState(o) }
    }
}
