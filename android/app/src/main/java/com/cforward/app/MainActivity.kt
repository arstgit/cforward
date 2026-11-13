package com.cforward.app

import android.Manifest
import android.app.Activity
import android.content.Intent
import android.content.pm.PackageManager
import android.content.res.ColorStateList
import android.graphics.Color
import android.os.Build
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.widget.Button
import android.widget.CheckBox
import android.widget.EditText
import android.widget.TextView
import android.widget.Toast
import org.json.JSONObject
import java.io.File
import java.net.HttpURLConnection
import java.net.URL

/**
 * Single-screen control panel for the cforward relay.
 * Polls the Go core's admin endpoint (127.0.0.1:18080/status) every 2 seconds.
 */
class MainActivity : Activity() {

    private val handler = Handler(Looper.getMainLooper())

    private val pill by lazy { findViewById<TextView>(R.id.pill_state) }
    private val btnToggle by lazy { findViewById<Button>(R.id.btn_toggle) }
    private val statPool by lazy { findViewById<TextView>(R.id.stat_pool) }
    private val statVerified by lazy { findViewById<TextView>(R.id.stat_verified) }
    private val statConns by lazy { findViewById<TextView>(R.id.stat_conns) }
    private val statCfst by lazy { findViewById<TextView>(R.id.stat_cfst) }
    private val topText by lazy { findViewById<TextView>(R.id.top_text) }
    private val portInput by lazy { findViewById<EditText>(R.id.input_port) }
    private val targetInput by lazy { findViewById<EditText>(R.id.input_target) }
    private val poolMinInput by lazy { findViewById<EditText>(R.id.input_pool_min) }
    private val ddCheck by lazy { findViewById<CheckBox>(R.id.check_dd) }
    private val debugCheck by lazy { findViewById<CheckBox>(R.id.check_debug) }
    private val btnApply by lazy { findViewById<Button>(R.id.btn_apply) }
    private val btnShare by lazy { findViewById<TextView>(R.id.btn_share) }
    private val logText by lazy { findViewById<TextView>(R.id.log_text) }

    private var running = false

    private val poller = object : Runnable {
        override fun run() {
            pollStatus()
            pollLog()
            handler.postDelayed(this, 2000)
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)

        val prefs = getSharedPreferences("cforward", MODE_PRIVATE)
        portInput.setText(prefs.getInt("listen_port", 1234).toString())
        targetInput.setText(prefs.getInt("target_port", 443).toString())
        poolMinInput.setText(prefs.getInt("pool_min", 100).toString())
        ddCheck.isChecked = prefs.getBoolean("cfst_dd", true)
        debugCheck.isChecked = prefs.getBoolean("debug", false)

        btnToggle.setOnClickListener {
            if (running) {
                stopService(Intent(this, ForwardService::class.java))
                setRunningUi(false)
            } else {
                saveSettings()
                startForegroundService(Intent(this, ForwardService::class.java))
                Toast.makeText(this, "Starting…", Toast.LENGTH_SHORT).show()
            }
        }

        btnApply.setOnClickListener {
            saveSettings()
            if (running) {
                stopService(Intent(this, ForwardService::class.java))
                startForegroundService(Intent(this, ForwardService::class.java))
                Toast.makeText(this, "Settings applied, relay restarted", Toast.LENGTH_SHORT).show()
            } else {
                Toast.makeText(this, "Settings saved", Toast.LENGTH_SHORT).show()
            }
        }

        btnShare.setOnClickListener { shareLog() }

        if (Build.VERSION.SDK_INT >= 33 &&
            checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED
        ) {
            requestPermissions(arrayOf(Manifest.permission.POST_NOTIFICATIONS), 1)
        }

        setRunningUi(false)
    }

    override fun onResume() {
        super.onResume()
        handler.post(poller)
    }

    override fun onPause() {
        super.onPause()
        handler.removeCallbacks(poller)
    }

    private fun saveSettings() {
        getSharedPreferences("cforward", MODE_PRIVATE).edit()
            .putInt("listen_port", portInput.text.toString().toIntOrNull() ?: 1234)
            .putInt("target_port", targetInput.text.toString().toIntOrNull() ?: 443)
            .putInt("pool_min", poolMinInput.text.toString().toIntOrNull() ?: 100)
            .putBoolean("cfst_dd", ddCheck.isChecked)
            .putBoolean("debug", debugCheck.isChecked)
            .apply()
    }

    // ---- UI state ----

    private fun setRunningUi(r: Boolean) {
        running = r
        if (r) {
            pill.text = "RUNNING"
            pill.backgroundTintList = ColorStateList.valueOf(getColor(R.color.ok))
            btnToggle.text = "Stop relay"
            btnToggle.backgroundTintList = ColorStateList.valueOf(getColor(R.color.danger))
        } else {
            pill.text = "STOPPED"
            pill.backgroundTintList = ColorStateList.valueOf(getColor(R.color.gray))
            btnToggle.text = "Start relay"
            btnToggle.backgroundTintList = ColorStateList.valueOf(getColor(R.color.accent))
            statPool.text = "—"
            statVerified.text = "—"
            statVerified.setTextColor(getColor(R.color.text))
            statConns.text = "—"
            statCfst.text = "—"
            topText.text = "—"
        }
        btnToggle.isEnabled = true
    }

    private fun applyStatus(j: JSONObject) {
        val pool = j.getJSONObject("pool")
        val cfst = j.getJSONObject("cfst")

        setRunningUi(true)

        statPool.text = String.format("%d IPs (min %d)", pool.getInt("size"), pool.getInt("min"))
        val verified = pool.getInt("verified")
        statVerified.text = verified.toString()
        statVerified.setTextColor(getColor(if (verified > 0) R.color.ok else R.color.text))
        statConns.text = j.getInt("active_conns").toString()
        statCfst.text = if (cfst.getBoolean("running")) "running" else "idle"

        val top = pool.getJSONArray("top")
        val sb = StringBuilder()
        if (top.length() == 0) {
            sb.append("(pool empty, waiting for the first cfst round…)")
        } else {
            sb.append(String.format("%-3s %-16s %5s %6s %6s\n", "#", "IP", "score", "lat", "speed"))
            val n = minOf(top.length(), 8)
            for (i in 0 until n) {
                val e = top.getJSONObject(i)
                val mark = if (e.getBoolean("verified")) " *" else ""
                sb.append(String.format("%-3d %-16s %5.1f %5.0fms %5.1fM%s\n",
                    i + 1, e.getString("ip"), e.getDouble("score"),
                    e.getDouble("lat"), e.getDouble("speed"), mark))
            }
            if (sb.toString().contains(" *")) {
                sb.append("* = verified (served real traffic)")
            }
        }
        topText.text = sb.toString()
    }

    // ---- data polling ----

    private fun pollStatus() {
        // Network on the UI thread is forbidden on Android, so use a background thread.
        Thread {
            try {
                val conn = URL("http://127.0.0.1:18080/status").openConnection() as HttpURLConnection
                conn.connectTimeout = 2000
                conn.readTimeout = 2000
                val body = conn.inputStream.bufferedReader().readText()
                conn.disconnect()
                val j = JSONObject(body)
                runOnUiThread { applyStatus(j) }
            } catch (e: Exception) {
                runOnUiThread { setRunningUi(false) }
            }
        }.start()
    }

    private fun pollLog() {
        if (logText.hasSelection()) return // don't disturb copying
        val log = File(filesDir, "cforward.log")
        if (!log.exists()) return
        val lines = log.readLines()
        if (lines.isEmpty()) return
        val tail = lines.takeLast(40).joinToString("\n")
        if (tail != logText.text.toString()) logText.text = tail
    }

    /** Shares the relay log as plain text. */
    private fun shareLog() {
        val log = File(filesDir, "cforward.log")
        val body = if (log.exists()) {
            val lines = log.readLines()
            if (lines.size > 300) lines.takeLast(300).joinToString("\n") else lines.joinToString("\n")
        } else {
            "no log yet"
        }
        val send = Intent(Intent.ACTION_SEND).apply {
            type = "text/plain"
            putExtra(Intent.EXTRA_SUBJECT, "cforward log")
            putExtra(Intent.EXTRA_TEXT, body)
        }
        startActivity(Intent.createChooser(send, "Share cforward log"))
    }
}
