package com.cforward.app

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.Service
import android.content.Intent
import android.os.IBinder
import java.io.File

/**
 * Foreground service that runs the cforward Go binary as a child process and
 * restarts it if it dies. The Go binary, cfst and ip.txt live in the app's
 * private files dir (assets are extracted once on first start).
 */
class ForwardService : Service() {

    private var process: Process? = null
    private var stopping = false

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onCreate() {
        super.onCreate()
        val nm = getSystemService(NotificationManager::class.java)
        nm.createNotificationChannel(
            NotificationChannel("cforward", "cforward relay", NotificationManager.IMPORTANCE_LOW)
        )
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        stopping = false
        startForeground(1, buildNotification())
        extractAssets()
        writeConfig()
        startProcess()
        return START_STICKY
    }

    override fun onDestroy() {
        stopping = true
        process?.destroy()
        process = null
        super.onDestroy()
    }

    private fun buildNotification(): Notification {
        return Notification.Builder(this, "cforward")
            .setSmallIcon(R.drawable.ic_launcher_foreground)
            .setContentTitle("cforward")
            .setContentText(if (process?.isAlive == true) "relaying" else "starting…")
            .setOngoing(true)
            .build()
    }

    /** Copies bundled binaries/data from assets to filesDir on first start. */
    private fun extractAssets() {
        val pairs = listOf(
            "bin/cforward" to File(File(filesDir, "bin"), "cforward"),
            "cfst/cfst" to File(File(filesDir, "cfst"), "cfst"),
            "cfst/ip.txt" to File(File(filesDir, "cfst"), "ip.txt"),
        )
        for ((asset, out) in pairs) {
            if (out.exists() && out.length() > 0) continue
            out.parentFile?.mkdirs()
            assets.open(asset).use { input -> out.outputStream().use { input.copyTo(it) } }
            out.setExecutable(true, false)
        }
    }

    /** Writes config.json from the current UI settings. */
    private fun writeConfig() {
        val prefs = getSharedPreferences("cforward", MODE_PRIVATE)
        val listenPort = prefs.getInt("listen_port", 1234)
        val cfg = """
        {
          "listen": "127.0.0.1:$listenPort",
          "target_port": ${prefs.getInt("target_port", 443)},
          "pool_file": "${File(filesDir, "pool.json").absolutePath}",
          "pool_min": ${prefs.getInt("pool_min", 100)},
          "pool_max": 200,
          "cfst_dir": "${File(filesDir, "cfst").absolutePath}",
          "cfst_dd": ${prefs.getBoolean("cfst_dd", true)},
          "dead_skip": "60s",
          "admin_addr": "127.0.0.1:18080"
        }
        """.trimIndent()
        File(filesDir, "config.json").writeText(cfg)
    }

    private fun startProcess() {
        if (process?.isAlive == true) return
        val bin = File(filesDir, "bin/cforward").absolutePath
        val cfg = File(filesDir, "config.json").absolutePath
        val log = File(filesDir, "cforward.log")
        val pb = ProcessBuilder(bin, "-config", cfg)
        pb.redirectErrorStream(true)
        pb.redirectOutput(ProcessBuilder.Redirect.appendTo(log))
        process = pb.start()
        monitor()
    }

    /** Waits for the process; restarts it (with a short backoff) unless we are stopping. */
    private fun monitor() {
        val p = process ?: return
        Thread {
            try {
                p.waitFor()
            } catch (_: InterruptedException) {
                return@Thread
            }
            if (stopping) return@Thread
            Thread.sleep(3000)
            if (!stopping) startProcess()
        }.apply { isDaemon = true; start() }
    }
}
