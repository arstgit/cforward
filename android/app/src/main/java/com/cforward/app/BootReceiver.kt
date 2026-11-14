package com.cforward.app

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent

/**
 * Starts the relay after a device reboot when "Start on boot" is enabled.
 * BOOT_COMPLETED is an allowed exemption for starting a foreground service
 * from the background on Android 12+.
 */
class BootReceiver : BroadcastReceiver() {
    override fun onReceive(context: Context, intent: Intent) {
        val action = intent.action ?: return
        if (action != Intent.ACTION_BOOT_COMPLETED && action != "android.intent.action.QUICKBOOT_POWERON") {
            return
        }
        val prefs = context.getSharedPreferences("cforward", Context.MODE_PRIVATE)
        if (!prefs.getBoolean("start_on_boot", false)) return
        try {
            context.startForegroundService(Intent(context, ForwardService::class.java))
        } catch (_: Exception) {
            // some ROMs forbid background FGS starts; the user can start manually
        }
    }
}
