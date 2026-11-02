package com.cforward.app

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.Service
import android.content.Intent
import android.content.pm.ServiceInfo
import android.os.Build
import android.os.IBinder
import android.util.Log
import java.io.File
import java.io.IOException

/**
 * Foreground service that runs the cforward Go binary as a child process and
 * restarts it if it dies. The Go binary, cfst and ip.txt live in the app's
 * private files dir (assets are extracted once on first start).
 */
class ForwardService : Service() {

    companion object {
        private const val TAG = "cforward"
    }

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
        try {
            // Android 14+ (API 34) REQUIRES startForeground with a foreground
            // service type matching the manifest, otherwise the app crashes with
            // MissingForegroundServiceTypeException.
            when {
                Build.VERSION.SDK_INT >= 34 ->
                    startForeground(1, buildNotification(),
                        ServiceInfo.FOREGROUND_SERVICE_TYPE_SPECIAL_USE)
                Build.VERSION.SDK_INT >= 29 ->
                    startForeground(1, buildNotification(), 0)
                else ->
                    startForeground(1, buildNotification())
            }
            extractAssets()
            writeConfig()
            startProcess()
        } catch (e: Exception) {
            Log.e(TAG, "start failed", e)
            writeLog("start failed: ${e}")
        }
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
            try {
                if (!out.exists() || out.length() == 0L) {
                    out.parentFile?.mkdirs()
                    assets.open(asset).use { input -> out.outputStream().use { input.copyTo(it) } }
                }
                // always force the executable bit (owner + group + other)
                out.setExecutable(true, true)
                out.setReadable(true, false)
            } catch (e: Exception) {
                Log.e(TAG, "extract $asset failed", e)
                writeLog("extract $asset failed: $e")
            }
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
          "debug": ${prefs.getBoolean("debug", false)},
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
        val args = listOf("-config", cfg)

        // Some hardened ROMs block execve() of binaries in the app data dir
        // (SELinux: no execute_no_trans on app_data_file). Fall back to loading
        // the static PIE via /system/bin/linker64, which only needs read+mmap.
        process = startWithFallback(bin, args, log)
        if (process == null) {
            val diag = StringBuilder("exec failed (direct + linker64)\n")
            diag.append("canExecute=").append(File(bin).canExecute())
            diag.append(" perms=").append(permsOf(File(bin)))
            diag.append("\n").append(lsLZ(File(bin)))
            writeLog(diag.toString())
            return
        }
        monitor()
    }

    private fun startWithFallback(bin: String, args: List<String>, log: File): Process? {
        // 1) direct execve
        try {
            val pb = ProcessBuilder(listOf(bin) + args)
            pb.redirectErrorStream(true)
            pb.redirectOutput(ProcessBuilder.Redirect.appendTo(log))
            return pb.start()
        } catch (e: IOException) {
            writeLog("direct exec failed (${e}); retrying via linker64")
        }
        // 2) linker64 manual load (bypasses execute_no_trans)
        try {
            val pb = ProcessBuilder(listOf("/system/bin/linker64", bin) + args)
            pb.redirectErrorStream(true)
            pb.redirectOutput(ProcessBuilder.Redirect.appendTo(log))
            val p = pb.start()
            writeLog("started via /system/bin/linker64")
            return p
        } catch (e: IOException) {
            Log.e(TAG, "linker64 exec failed", e)
        }
        return null
    }

    private fun permsOf(f: File): String {
        return try {
            java.nio.file.Files.getPosixFilePermissions(f.toPath()).toString()
        } catch (e: Exception) {
            "n/a (${e})"
        }
    }

    private fun lsLZ(f: File): String {
        return try {
            val p = Runtime.getRuntime().exec(
                arrayOf("/system/bin/ls", "-lZ", f.absolutePath)
            )
            p.inputStream.bufferedReader().readText().trim()
        } catch (e: Exception) {
            "ls -lZ failed: ${e}"
        }
    }

    private fun writeLog(msg: String) {
        try {
            File(filesDir, "cforward.log").appendText(msg + "\n")
        } catch (_: Exception) {
        }
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
