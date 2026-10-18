package net.mutiny.app

import android.content.Context

/** Small persistent settings: server url and the last known playback hash. */
object Prefs {
    private const val NAME = "mutiny"
    const val DEFAULT_BASE = "https://mutiny.website"

    private fun p(ctx: Context) = ctx.getSharedPreferences(NAME, Context.MODE_PRIVATE)

    fun base(ctx: Context): String =
        (p(ctx).getString("base", DEFAULT_BASE) ?: DEFAULT_BASE).trimEnd('/')

    fun setBase(ctx: Context, url: String) {
        p(ctx).edit().putString("base", url.trim().trimEnd('/')).apply()
    }

    /** The web app's hash (#label/release/track-ix/q/format/filter). */
    fun hash(ctx: Context): String = p(ctx).getString("hash", "") ?: ""

    fun setHash(ctx: Context, h: String) {
        if (h.isNotEmpty() && h != hash(ctx)) p(ctx).edit().putString("hash", h).apply()
    }

    /** JSON snapshot of the native player, for resuming after process death. */
    fun resume(ctx: Context): String = p(ctx).getString("resume", "") ?: ""

    fun setResume(ctx: Context, json: String) {
        p(ctx).edit().putString("resume", json).apply()
    }
}