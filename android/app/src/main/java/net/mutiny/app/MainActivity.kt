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
import android.webkit.WebResourceRequest
import android.webkit.WebSettings
import android.webkit.WebView
import android.webkit.WebViewClient
import android.widget.EditText
import android.widget.ImageButton
import android.widget.PopupMenu
import androidx.activity.OnBackPressedCallback
import androidx.appcompat.app.AppCompatActivity
import androidx.core.content.ContextCompat
import androidx.webkit.WebViewCompat
import androidx.webkit.WebViewFeature

/**
 * Hosts the mutiny.website web app in a [KeepAliveWebView].
 *
 * The page is the whole player: it plays the music through its own <audio>
 * element exactly as in a browser. The app only keeps it alive in the
 * background ([KeepAliveWebView] + the foreground [PlayerService]) and wires
 * the notification / lock screen / media keys to the page's own controls.
 */
class MainActivity : AppCompatActivity() {

    private lateinit var web: WebView
    private var bound = false

    private val conn = object : ServiceConnection {
        override fun onServiceConnected(name: ComponentName?, b: IBinder?) {
            bound = true
            PlayerService.instance?.controller = controller
        }

        override fun onServiceDisconnected(name: ComponentName?) {
            bound = false
        }
    }

    private val controller = object : PlayerService.Controller {
        override fun action(name: String) {
            web.evaluateJavascript("window.__mutiny && __mutiny.act('$name'); null", null)
        }

        override fun seek(ms: Long) {
            web.evaluateJavascript("window.__mutiny && __mutiny.seek($ms); null", null)
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)
        web = findViewById(R.id.web)
        setupWebView()
        findViewById<ImageButton>(R.id.fab).setOnClickListener { showMenu(it) }
        requestNotificationPermission()
        PlayerService.start(this)
        bindService(Intent(this, PlayerService::class.java), conn, 0)
        onBackPressedDispatcher.addCallback(this, object : OnBackPressedCallback(true) {
            override fun handleOnBackPressed() {
                // never finish: that would destroy the page and the music
                if (web.canGoBack()) web.goBack() else moveTaskToBack(true)
            }
        })
        loadPage()
    }

    private fun loadPage() {
        web.loadUrl(Prefs.base(this) + Prefs.hash(this))
    }

    @SuppressLint("SetJavaScriptEnabled")
    private fun setupWebView() {
        web.settings.apply {
            javaScriptEnabled = true
            domStorageEnabled = true
            mediaPlaybackRequiresUserGesture = false
            mixedContentMode = WebSettings.MIXED_CONTENT_ALWAYS_ALLOW
        }
        web.setBackgroundColor(ContextCompat.getColor(this, R.color.mutiny_bg))

        web.addJavascriptInterface(
            WebBridge { s ->
                Prefs.setHash(this, s.optString("hash"))
                PlayerService.instance?.onWebState(s)
            },
            "Mutiny",
        )
        // must run before the page's own scripts so it sees the page's
        // mediaSession handlers being registered
        if (WebViewFeature.isFeatureSupported(WebViewFeature.DOCUMENT_START_SCRIPT)) {
            WebViewCompat.addDocumentStartJavaScript(web, HOOK, setOf("*"))
        }

        web.webViewClient = object : WebViewClient() {
            override fun shouldOverrideUrlLoading(view: WebView?, request: WebResourceRequest?): Boolean {
                val url = request?.url ?: return false
                if (url.host == hostOf(Prefs.base(this@MainActivity))) return false
                try {
                    startActivity(Intent(Intent.ACTION_VIEW, url))
                } catch (ignored: Exception) {
                }
                return true
            }
        }
    }

    private fun hostOf(base: String): String =
        base.removePrefix("https://").removePrefix("http://").substringBefore('/').substringBefore(':')

    override fun onDestroy() {
        PlayerService.instance?.controller = null
        if (bound) unbindService(conn)
        PlayerService.stop(this)
        web.destroy()
        super.onDestroy()
    }

    /** media keys while the app is in front */
    override fun onKeyDown(keyCode: Int, event: KeyEvent?): Boolean {
        val s = PlayerService.instance
        if (s != null && keyCode in MEDIA_KEYS) {
            s.onKey(keyCode)
            return true
        }
        return super.onKeyDown(keyCode, event)
    }

    // ------------------------------------------------------------------
    // menu
    // ------------------------------------------------------------------

    private fun showMenu(anchor: View) {
        val playing = PlayerService.instance?.isPlaying() == true
        val pm = PopupMenu(this, anchor)
        pm.menu.add(0, ID_PLAYPAUSE, 0, if (playing) R.string.menu_pause else R.string.menu_play)
        pm.menu.add(1, ID_NEXT, 1, R.string.menu_next)
        pm.menu.add(1, ID_PREV, 2, R.string.menu_prev)
        pm.menu.add(2, ID_RELOAD, 3, R.string.menu_reload)
        pm.menu.add(3, ID_SERVER, 4, R.string.menu_server)
        pm.menu.add(3, ID_ABOUT, 5, R.string.menu_about)
        pm.setOnMenuItemClickListener { item ->
            when (item.itemId) {
                ID_PLAYPAUSE -> controller.action(if (playing) "pause" else "play")
                ID_NEXT -> controller.action("nexttrack")
                ID_PREV -> controller.action("previoustrack")
                ID_RELOAD -> loadPage()
                ID_SERVER -> serverDialog()
                ID_ABOUT -> aboutDialog()
                else -> return@setOnMenuItemClickListener false
            }
            true
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
                if (url.isNotEmpty() && url.trimEnd('/') != Prefs.base(this)) {
                    Prefs.setBase(this, url)
                    Prefs.setHash(this, "")
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
        private const val ID_SERVER = 105
        private const val ID_ABOUT = 106

        private val MEDIA_KEYS = setOf(
            KeyEvent.KEYCODE_MEDIA_PLAY,
            KeyEvent.KEYCODE_MEDIA_PAUSE,
            KeyEvent.KEYCODE_MEDIA_PLAY_PAUSE,
            KeyEvent.KEYCODE_MEDIA_NEXT,
            KeyEvent.KEYCODE_MEDIA_PREVIOUS,
        )

        /**
         * Runs at document start. Records the page's own mediaSession
         * handlers so native controls behave exactly like the browser's
         * (multi-tap skip, etc.), and reports the <audio> element's state.
         */
        private val HOOK: String = """
(function () {
  if (window.__mutiny) return;
  var H = {};
  var ms = navigator.mediaSession;
  if (ms && ms.setActionHandler) {
    var orig = ms.setActionHandler.bind(ms);
    ms.setActionHandler = function (a, f) { H[a] = f; try { orig(a, f); } catch (e) {} };
  }
  function P() { return document.getElementById('player'); }
  function report() {
    var p = P();
    if (!p || !window.Mutiny) return;
    var m = (ms && ms.metadata) || {};
    var art = (m.artwork && m.artwork.length) ? m.artwork[m.artwork.length - 1].src : '';
    var title = m.title || '', artist = m.artist || '';
    if (!title && p.currentSrc) {
      title = decodeURIComponent(p.currentSrc.split('/').pop()).replace(/-\d+\.\w+$/, '');
    }
    Mutiny.state(JSON.stringify({
      playing: !p.paused && !p.ended,
      pos: Math.round((p.currentTime || 0) * 1000),
      dur: isFinite(p.duration) ? Math.round(p.duration * 1000) : 0,
      title: title, artist: artist, album: m.album || '', art: art,
      hash: location.hash
    }));
  }
  var last = 0;
  function hook() {
    var p = P();
    if (!p) return;
    ['play', 'playing', 'pause', 'ended', 'seeked', 'loadedmetadata', 'durationchange'].forEach(function (e) {
      p.addEventListener(e, function () { setTimeout(report, 100); });
    });
    p.addEventListener('timeupdate', function () {
      var n = Date.now();
      if (n - last > 5000) { last = n; report(); }
    });
  }
  document.addEventListener('DOMContentLoaded', hook);
  window.__mutiny = {
    act: function (a) {
      if (H[a]) { H[a]({ action: a }); return; }
      var p = P();
      if (!p) return;
      if (a === 'play') p.play();
      else if (a === 'pause') p.pause();
      else if (a === 'nexttrack' && window.d) d('+track');
      else if (a === 'previoustrack' && window.d) d('-track');
    },
    seek: function (ms) { var p = P(); if (p) p.currentTime = ms / 1000; }
  };
})();
"""
    }
}
