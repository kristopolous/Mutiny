package net.mutiny.app

import android.content.Context
import android.util.AttributeSet
import android.view.View
import android.webkit.WebView

/**
 * A WebView that never learns it has been hidden.
 *
 * When the window goes away (screen off, app backgrounded) a normal WebView
 * tells Chromium the page is hidden, and Chromium then throttles / suspends
 * the page and its media. Reporting VISIBLE keeps the page's <audio> playing
 * and its JS (track auto-advance on 'ended') running.
 */
class KeepAliveWebView @JvmOverloads constructor(
    context: Context,
    attrs: AttributeSet? = null,
) : WebView(context, attrs) {

    init {
        // keep the renderer process at foreground importance while hidden
        setRendererPriorityPolicy(RENDERER_PRIORITY_IMPORTANT, false)
    }

    override fun onWindowVisibilityChanged(visibility: Int) {
        super.onWindowVisibilityChanged(View.VISIBLE)
    }
}
