package net.mutiny.app

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent

/**
 * Debug aids (trigger from adb):
 *   am broadcast -n net.mutiny.app/.DebugPlayReceiver -a net.mutiny.app.DEBUG_PLAY
 *     -> play a local tone file (no network) through the main player
 *   am broadcast -n net.mutiny.app/.DebugPlayReceiver -a net.mutiny.app.DEBUG_STATE
 *     -> dump the live ExoPlayer state to logcat
 *   am broadcast -n net.mutiny.app/.DebugPlayReceiver -a net.mutiny.app.DEBUG_RAW
 *     -> play the tone through a brand-new standalone ExoPlayer (bisection)
 */
class DebugPlayReceiver : BroadcastReceiver() {
    override fun onReceive(context: Context, intent: Intent?) {
        val svc = PlayerService.instance
        if (svc == null) {
            PlayerService.start(context)
            return
        }
        when (intent?.action) {
            "net.mutiny.app.DEBUG_STATE" -> svc.dumpDebugState()
            "net.mutiny.app.DEBUG_RAW" -> svc.rawTone()
            else -> svc.playDebugTone()
        }
    }
}