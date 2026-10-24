package com.cforward.app

import android.Manifest
import android.app.Activity
import android.content.Intent
import android.content.pm.PackageManager
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
 * Minimal status + control screen. Polls the Go core's admin endpoint
 * (127.0.0.1:18080/status) every 2 seconds and shows the pool and live relays.
 */
class MainActivity : Activity() {

    private val handler = Handler(Looper.getMainLooper())
    private val statusText by lazy { findViewById<TextView>(R.id.status_text) }
    private val logText by lazy { findViewById<TextView>(R.id.log_text) }
    private val btnStart by lazy { findViewById<Button>(R.id.btn_start) }
    private val btnStop by lazy { findViewById<Button>(R.id.btn_stop) }
    private val btnApply by lazy { findViewById<Button>(R.id.btn_apply) }
    private val portInput by lazy { findViewById<EditText>(R.id.input_port) }
    private val targetInput by lazy { findViewById<EditText>(R.id.input_target) }
    private val poolMinInput by lazy { findViewById<EditText>(R.id.input_pool_min) }
    private val ddCheck by lazy { findViewById<CheckBox>(R.id.check_dd) }

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

        btnStart.setOnClickListener {
            saveSettings()
            startForegroundService(Intent(this, ForwardService::class.java))
            Toast.makeText(this, "cforward starting", Toast.LENGTH_SHORT).show()
        }
        btnStop.setOnClickListener {
            stopService(Intent(this, ForwardService::class.java))
            Toast.makeText(this, "cforward stopped", Toast.LENGTH_SHORT).show()
        }
        btnApply.setOnClickListener {
            saveSettings()
            stopService(Intent(this, ForwardService::class.java))
            startForegroundService(Intent(this, ForwardService::class.java))
            Toast.makeText(this, "settings applied, relay restarted", Toast.LENGTH_SHORT).show()
        }

        if (Build.VERSION.SDK_INT >= 33 &&
            checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED
        ) {
            requestPermissions(arrayOf(Manifest.permission.POST_NOTIFICATIONS), 1)
        }
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
            .apply()
    }

    private fun pollStatus() {
        val text = try {
            val conn = URL("http://127.0.0.1:18080/status").openConnection() as HttpURLConnection
            conn.connectTimeout = 2000
            conn.readTimeout = 2000
            val body = conn.inputStream.bufferedReader().readText()
            conn.disconnect()
            formatStatus(JSONObject(body))
        } catch (e: Exception) {
            "⏸ relay not running (admin endpoint unreachable)"
        }
        statusText.text = text
    }

    private fun formatStatus(j: JSONObject): String {
        val pool = j.getJSONObject("pool")
        val sb = StringBuilder()
        sb.append("pool: ").append(pool.getInt("size")).append(" IPs")
            .append("  proven: ").append(pool.getInt("proven"))
            .append("  dead-skipped: ").append(pool.getInt("dead_skipped"))
            .append("\nactive conns: ").append(j.getInt("active_conns"))
        val cfst = j.getJSONObject("cfst")
        sb.append("  cfst: ").append(if (cfst.getBoolean("running")) "running" else "idle")
        sb.append("\n\n")
        val top = pool.getJSONArray("top")
        if (top.length() == 0) {
            sb.append("(pool empty, waiting for first cfst round…)\n")
        } else {
            for (i in 0 until minOf(top.length(), 8)) {
                val e = top.getJSONObject(i)
                sb.append(String.format("%-3d %-16s score=%5.1f lat=%4.0fms speed=%5.1fM\n",
                    i + 1, e.getString("ip"), e.getDouble("score"),
                    e.getDouble("lat"), e.getDouble("speed")))
            }
        }
        return sb.toString()
    }

    private fun pollLog() {
        val log = File(filesDir, "cforward.log")
        if (!log.exists()) return
        val lines = log.readLines()
        if (lines.isEmpty()) return
        val tail = lines.takeLast(12).joinToString("\n")
        if (tail != logText.text.toString()) logText.text = tail
    }
}
