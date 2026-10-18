package net.mutiny.app

import android.Manifest
import android.annotation.SuppressLint
import android.app.AlertDialog
import android.content.ComponentName
import android.content.Intent
import android.content.ServiceConnection
import android.content.pm.PackageManager
import android.os.Build
import android.os.Bundle
import android.os.IBinder
import android.view.KeyEvent
import android.view.View
import android.webkit.ConsoleMessage
import android.webkit.WebChromeClient
import android.webkit.WebResourceRequest
import android.webkit.WebResourceResponse
import android.webkit.WebSettings
import android.webkit.WebView
import android.webkit.WebViewClient
import android.widget.EditText
import android.widget.ImageButton
import android.widget.PopupMenu
import androidx.appcompat.app.AppCompatActivity
import androidx.core.content.ContextCompat
import org.json.JSONObject

/**
 * Hosts the mutiny.website web app in a [WebView] and keeps it in sync with
 * the native [PlayerService].
 *
 * The web page stays the UI and (while it is alive) the driver: the page's
 * `<audio id=player>` element is replaced by a silent, looping stand-in (see
 * [SHIM]), so the page never fetches or plays a real stream and never
 * "ends" a track on its own. Every time the page picks a track it reports it
 * through [WebBridge] and the native player plays the real stream — which
 * keeps playing when this page is frozen, backgrounded or destroyed.
 */
class MainActivity : AppCompatActivity() {

    private lateinit var web: WebView
    private var bound: Boolean = false

    /** the first onTrack may arrive before the service is created; queue it */
    private var pendingTrack: Pair<String, JSONObject>? = null

    private val conn = object : ServiceConnection {
        override fun onServiceConnected(name: ComponentName?, iBinder: IBinder?) {
            bound = true
            PlayerService.instance?.trackListener = { _, _ -> onTrackChanged() }
            pendingTrack?.let { (u, snap) ->
                pendingTrack = null
                PlayerService.instance?.onWebTrack(u, snap)
            }
        }

        override fun onServiceDisconnected(name: ComponentName?) {
            bound = false
        }
    }

    /** Called on every track change in the service (web- or native-driven). */
    private fun onTrackChanged() {
        val h = PlayerService.instance?.currentHash().orEmpty()
        if (h.isNotEmpty()) {
            Prefs.setHash(this, h)
            pushHashToWeb(h)
        }
    }

    @SuppressLint("SetJavaScriptEnabled")
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)
        web = findViewById(R.id.web)
        setupWebView()
        findViewById<ImageButton>(R.id.fab).setOnClickListener { showMenu(it) }
        requestNotificationPermission()
        PlayerService.start(this)
        bindService(Intent(this, PlayerService::class.java), conn, 0)
        loadPage()
    }

    private fun loadPage() {
        web.loadUrl(Prefs.base(this) + Prefs.hash(this))
    }

    // ------------------------------------------------------------------
    // webview
    // ------------------------------------------------------------------

    @SuppressLint("SetJavaScriptEnabled")
    private fun setupWebView() {
        web.settings.apply {
            javaScriptEnabled = true
            domStorageEnabled = true
            mediaPlaybackRequiresUserGesture = false
            allowFileAccess = false
            allowContentAccess = false
            mixedContentMode = WebSettings.MIXED_CONTENT_ALWAYS_ALLOW
            cacheMode = WebSettings.LOAD_DEFAULT
            blockNetworkImage = false
        }
        web.setBackgroundColor(ContextCompat.getColor(this, R.color.mutiny_bg))

        // web -> native: the page reports every track it would play
        web.addJavascriptInterface(
            WebBridge(this) { url, snap ->
                Prefs.setHash(this, snap.optString("hash"))
                val s = PlayerService.instance
                if (s != null) s.onWebTrack(url, snap) else pendingTrack = (url to snap)
            },
            "Mutiny",
        )

        web.webViewClient = object : WebViewClient() {
            override fun onPageFinished(view: WebView?, url: String?) {
                super.onPageFinished(view, url)
                // the page is (re)loaded: re-install the stub
                if (url != null && !url.contains("chrome-error")) {
                    view?.evaluateJavascript(SHIM, null)
                }
            }

            override fun shouldInterceptRequest(
                view: WebView?,
                request: WebResourceRequest?,
            ): WebResourceResponse? {
                val uri = request?.url ?: return null
                val path = uri.path
                if (uri.host == hostOf(Prefs.base(this@MainActivity)) &&
                    (path?.startsWith("/raid/") == true || path?.startsWith("/pl/") == true)
                ) {
                    // only real media: album-art.jpg and other assets under
                    // /raid/ must still reach the page
                    val ext = path?.substringAfterLast('.', "")?.lowercase() ?: ""
                    if (ext in SILENT_EXTS || path.contains("fw.php")) {
                        return silentResponse()
                    }
                }
                return null
            }

            override fun shouldOverrideUrlLoading(
                view: WebView?,
                request: WebResourceRequest?,
            ): Boolean {
                val url = request?.url ?: return false
                return if (url.host == hostOf(Prefs.base(this@MainActivity))) {
                    false // stay in the app for same-site navigation
                } else {
                    try {
                        startActivity(Intent(Intent.ACTION_VIEW, url))
                    } catch (ignored: Exception) {
                    }
                    true
                }
            }
        }

        web.webChromeClient = object : WebChromeClient() {
            override fun onConsoleMessage(cm: ConsoleMessage): Boolean {
                // the web page logs _my/_next on every navigation
                return super.onConsoleMessage(cm)
            }
        }
    }

    private fun hostOf(base: String): String =
        base.removePrefix("https://").removePrefix("http://").substringBefore('/').substringBefore(':')

    /** media extensions the web's <audio> can ever be pointed at */
    private val SILENT_EXTS = setOf(
        "mp3", "opus", "m5a", "m4a", "mp4", "webm", "ogg", "oga", "flac",
    )

    /**
     * 60 s of silence (webm/opus), served for any real media url that slips
     * through before the stub installs. Long enough that a premature 'ended'
     * can only advance the track once, not loop; silent, so never double audio.
     */
    private fun silentResponse(): WebResourceResponse {
        val stream = resources.openRawResource(R.raw.silent_long)
        return WebResourceResponse("audio/webm", "UTF-8", stream)
    }

    /** Push the native player's hash into the page (re-sync after a freeze). */
    private fun pushHashToWeb(h: String) {
        web.evaluateJavascript("window.location.hash = ${JSONObject.quote(h)}; null", null)
    }

    // ------------------------------------------------------------------
    // lifecycle
    // ------------------------------------------------------------------

    override fun onResume() {
        super.onResume()
        // re-sync the web UI with the native player after a freeze/kill
        val h = PlayerService.instance?.currentHash().orEmpty()
        if (h.isNotEmpty()) pushHashToWeb(h)
    }

    override fun onDestroy() {
        PlayerService.instance?.trackListener = null
        if (bound) unbindService(conn)
        web.destroy()
        super.onDestroy()
    }

    /**
     * Media-key key events (phones with hardware buttons; headsets arrive as
     * ACTION_MEDIA_BUTTON broadcasts handled by MediaButtonReceiver).
     */
    override fun onKeyDown(keyCode: Int, event: KeyEvent?): Boolean {
        when (keyCode) {
            KeyEvent.KEYCODE_MEDIA_PLAY, KeyEvent.KEYCODE_MEDIA_PLAY_PAUSE -> {
                PlayerService.toggle(this)
                return true
            }
            KeyEvent.KEYCODE_MEDIA_PAUSE -> {
                PlayerService.instance?.pause()
                return true
            }
            KeyEvent.KEYCODE_MEDIA_NEXT -> {
                PlayerService.instance?.next()
                return true
            }
            KeyEvent.KEYCODE_MEDIA_PREVIOUS -> {
                PlayerService.instance?.prev()
                return true
            }
            KeyEvent.KEYCODE_MEDIA_STOP -> {
                PlayerService.stop(this)
                return true
            }
        }
        return super.onKeyDown(keyCode, event)
    }

    override fun onBackPressed() {
        if (web.canGoBack()) web.goBack() else super.onBackPressed()
    }

    // ------------------------------------------------------------------
    // menu
    // ------------------------------------------------------------------

    private fun showMenu(anchor: View) {
        val pm = PopupMenu(this, anchor)
        pm.menu.add(0, ID_PLAYPAUSE, 0, if (PlayerService.instance?.isPlaying() == true) R.string.menu_pause else R.string.menu_play)
        pm.menu.add(1, ID_NEXT, 1, R.string.menu_next)
        pm.menu.add(1, ID_PREV, 2, R.string.menu_prev)
        pm.menu.add(2, ID_RELOAD, 3, R.string.menu_reload)
        pm.menu.add(2, ID_STOP, 4, R.string.menu_stop)
        pm.menu.add(3, ID_SERVER, 5, R.string.menu_server)
        pm.menu.add(3, ID_ABOUT, 6, R.string.menu_about)
        pm.setOnMenuItemClickListener { item ->
            when (item.itemId) {
                ID_PLAYPAUSE -> { PlayerService.toggle(this); true }
                ID_NEXT -> { PlayerService.instance?.next(); true }
                ID_PREV -> { PlayerService.instance?.prev(); true }
                ID_RELOAD -> { loadPage(); true }
                ID_STOP -> { PlayerService.stop(this); true }
                ID_SERVER -> { serverDialog(); true }
                ID_ABOUT -> { aboutDialog(); true }
                else -> false
            }
        }
        pm.show()
    }

    private fun serverDialog() {
        val input = EditText(this).apply {
            setPadding(48, 24, 48, 0)
            setText(Prefs.base(this@MainActivity))
            setHint(R.string.server_default)
        }
        AlertDialog.Builder(this)
            .setTitle(R.string.menu_server)
            .setView(input)
            .setPositiveButton(R.string.server_save) { _, _ ->
                val url = input.text.toString().trim()
                if (url.isNotEmpty() && (url.trimEnd('/') != Prefs.base(this))) {
                    Prefs.setBase(this, url)
                    PlayerService.instance?.reconfigure(url)
                        ?: PlayerService.start(this)
                    loadPage()
                }
            }
            .setNegativeButton(android.R.string.cancel, null)
            .show()
    }

    private fun aboutDialog() {
        AlertDialog.Builder(this)
            .setTitle(R.string.app_name)
            .setMessage(getString(R.string.about_text))
            .setPositiveButton(android.R.string.ok, null)
            .show()
    }

    private fun requestNotificationPermission() {
        if (Build.VERSION.SDK_INT >= 33 &&
            checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED
        ) {
            requestPermissions(arrayOf(Manifest.permission.POST_NOTIFICATIONS), 1)
        }
    }

    companion object {
        private const val ID_PLAYPAUSE = 100
        private const val ID_NEXT = 101
        private const val ID_PREV = 102
        private const val ID_RELOAD = 103
        private const val ID_STOP = 104
        private const val ID_SERVER = 105
        private const val ID_ABOUT = 106

        /** 10 ms of silence, used by the stubbed web <audio> element. */
        private const val SILENT_WAV_B64 =
            "UklGRsQAAABXQVZFZm10IBAAAAABAAEAQB8AAIA+AAACAAIAZGF0YaAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

        /**
         * Installs the silent stub over the web's audio element and points the
         * page's state machine at the native player.
         */
        private val SHIM: String = """
(function () {
  if (window.__mutinyStub) return;
  window.__mutinyStub = true;
  var SILENT = 'data:audio/wav;base64,$SILENT_WAV_B64';
  function snap() {
    return {
      q: (typeof _qstr !== 'undefined') ? _qstr : '',
      f: (typeof _format !== 'undefined') ? _format : 2,
      fl: (typeof _filter !== 'undefined') ? _filter : 1,
      loop: (typeof _lock !== 'undefined' && _lock && _lock.loop) ? 1 : 0,
      my: (typeof _my !== 'undefined') ? _my : null,
      next: (typeof _next !== 'undefined') ? _next : null,
      hash: location.hash
    };
  }
  function report(abs) {
    try { if (window.Mutiny && abs) window.Mutiny.onTrack(abs, JSON.stringify(snap())); } catch (e) {}
  }
  function toAbs(v) { try { return new URL(v, location.origin).href; } catch (e) { return v; } }
  var p = document.getElementById('player');
  if (!p) return;
  var early = p.getAttribute('src') || '';
  // 1) the page's <audio> becomes a silent, looping stand-in: no network,
  //    no sound, and it never fires 'ended' (the page auto-advances on
  //    'ended'). All audio comes from the native player, which keeps
  //    playing while this page is frozen or in the background.
  try { p.loop = true; p.muted = true; p.volume = 0; } catch (e) {}
  var realUrl = '';
  var proto = Object.getOwnPropertyDescriptor(HTMLMediaElement.prototype, 'src');
  Object.defineProperty(p, 'src', {
    configurable: true,
    get: function () { return realUrl || (proto.get ? proto.get.call(p) : ''); },
    set: function (v) {
      realUrl = v;
      try { proto.set.call(p, SILENT); } catch (e) {}
      report(toAbs(v));
    }
  });
  // 2) the web app registers its own MediaSession handlers for the hardware
  //    keys; drop them so the keys reach the native session instead.
  try {
    var ms = navigator.mediaSession;
    if (ms) {
      ['play', 'pause', 'stop', 'nexttrack', 'previoustrack', 'seekbackward', 'seekforward'].forEach(function (a) {
        try { ms.setActionHandler(a, null); } catch (e) {}
      });
      try { ms.setActionHandler = function () {}; } catch (e) {}
    }
  } catch (e) {}
  // 3) if a src was set before this shim ran, stub it and report it
  if (early) {
    realUrl = early;
    try { proto.set.call(p, SILENT); } catch (e) {}
    report(toAbs(early));
  }
})();
"""
    }
}