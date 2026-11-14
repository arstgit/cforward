package com.cforward.app

import android.Manifest
import android.content.Intent
import android.content.pm.PackageManager
import android.content.res.ColorStateList
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.os.PowerManager
import android.provider.Settings
import android.widget.Toast
import androidx.activity.viewModels
import androidx.appcompat.app.AppCompatActivity
import androidx.core.content.ContextCompat
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.lifecycleScope
import androidx.lifecycle.repeatOnLifecycle
import com.cforward.app.databinding.ActivityMainBinding
import kotlinx.coroutines.launch
import java.util.Locale

/**
 * Single-screen control panel for the cforward relay.
 * Rendering is driven by MainViewModel's UiState; this class only wires
 * user actions (start/stop/apply/share) and maps state to views.
 */
class MainActivity : AppCompatActivity() {

    private lateinit var binding: ActivityMainBinding
    private val vm: MainViewModel by viewModels()

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        binding = ActivityMainBinding.inflate(layoutInflater)
        setContentView(binding.root)

        loadSettings()
        wireActions()
        observeState()
        requestNotificationPermissionIfNeeded()
    }

    // ---- setup ----

    private fun loadSettings() {
        val prefs = getSharedPreferences(PREFS, MODE_PRIVATE)
        binding.inputPort.setText(prefs.getInt("listen_port", 1234).toString())
        binding.inputTarget.setText(prefs.getInt("target_port", 443).toString())
        binding.inputPoolMin.setText(prefs.getInt("pool_min", 100).toString())
        binding.switchDd.isChecked = prefs.getBoolean("cfst_dd", true)
        binding.switchDebug.isChecked = prefs.getBoolean("debug", false)
        binding.switchBoot.isChecked = prefs.getBoolean("start_on_boot", false)
    }

    private fun wireActions() {
        binding.btnToggle.setOnClickListener {
            if (vm.state.value.running) stopRelay() else startRelay()
        }
        binding.btnApply.setOnClickListener {
            saveSettings()
            if (vm.state.value.running) {
                stopRelay()
                startRelay()
                toast(getString(R.string.toast_settings_applied))
            } else {
                toast(getString(R.string.toast_settings_saved))
            }
        }
        binding.btnBattery.setOnClickListener { requestBatteryExemption() }
        binding.btnShare.setOnClickListener { shareLog() }
    }

    private fun observeState() {
        lifecycleScope.launch {
            repeatOnLifecycle(Lifecycle.State.STARTED) {
                vm.state.collect { render(it) }
            }
        }
    }

    // ---- actions ----

    private fun startRelay() {
        saveSettings()
        ContextCompat.startForegroundService(this, Intent(this, ForwardService::class.java))
        toast(getString(R.string.toast_starting))
    }

    private fun stopRelay() {
        stopService(Intent(this, ForwardService::class.java))
    }

    private fun saveSettings() {
        getSharedPreferences(PREFS, MODE_PRIVATE).edit()
            .putInt("listen_port", binding.inputPort.text.toString().toIntOrNull() ?: 1234)
            .putInt("target_port", binding.inputTarget.text.toString().toIntOrNull() ?: 443)
            .putInt("pool_min", binding.inputPoolMin.text.toString().toIntOrNull() ?: 100)
            .putBoolean("cfst_dd", binding.switchDd.isChecked)
            .putBoolean("debug", binding.switchDebug.isChecked)
            .putBoolean("start_on_boot", binding.switchBoot.isChecked)
            .apply()
    }

    private fun requestBatteryExemption() {
        val pm = getSystemService(PowerManager::class.java)
        if (pm != null && pm.isIgnoringBatteryOptimizations(packageName)) {
            toast(getString(R.string.toast_battery_already))
            return
        }
        try {
            startActivity(
                Intent(Settings.ACTION_REQUEST_IGNORE_BATTERY_OPTIMIZATIONS)
                    .setData(Uri.parse("package:$packageName"))
            )
        } catch (e: Exception) {
            try {
                startActivity(Intent(Settings.ACTION_IGNORE_BATTERY_OPTIMIZATION_SETTINGS))
            } catch (e2: Exception) {
                toast(getString(R.string.toast_battery_manual))
            }
        }
    }

    private fun shareLog() {
        val log = java.io.File(filesDir, "cforward.log")
        val body = if (log.exists()) {
            runCatching {
                val lines = log.readLines()
                if (lines.size > 300) lines.takeLast(300).joinToString("\n") else lines.joinToString("\n")
            }.getOrDefault(getString(R.string.log_empty))
        } else {
            getString(R.string.log_empty)
        }
        val send = Intent(Intent.ACTION_SEND).apply {
            type = "text/plain"
            putExtra(Intent.EXTRA_SUBJECT, getString(R.string.share_subject))
            putExtra(Intent.EXTRA_TEXT, body)
        }
        startActivity(Intent.createChooser(send, getString(R.string.share_chooser)))
    }

    private fun requestNotificationPermissionIfNeeded() {
        if (Build.VERSION.SDK_INT >= 33 &&
            checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED
        ) {
            requestPermissions(arrayOf(Manifest.permission.POST_NOTIFICATIONS), 1)
        }
    }

    // ---- rendering ----

    private fun render(s: UiState) {
        renderPillAndButton(s.running)

        val snap = s.snapshot
        if (snap == null) {
            renderStoppedStats()
        } else {
            renderStats(snap)
            renderTop(snap)
        }
        renderLog(s.logTail)
    }

    private fun renderPillAndButton(running: Boolean) {
        if (running) {
            binding.pillState.text = getString(R.string.status_running)
            binding.pillState.backgroundTintList = ColorStateList.valueOf(color(R.color.ok))
            binding.btnToggle.text = getString(R.string.btn_stop)
            binding.btnToggle.backgroundTintList = ColorStateList.valueOf(color(R.color.error))
        } else {
            binding.pillState.text = getString(R.string.status_stopped)
            binding.pillState.backgroundTintList = ColorStateList.valueOf(color(R.color.gray))
            binding.btnToggle.text = getString(R.string.btn_start)
            binding.btnToggle.backgroundTintList = ColorStateList.valueOf(color(R.color.brand))
        }
    }

    private fun renderStoppedStats() {
        val dash = getString(R.string.value_dash)
        binding.statPool.text = dash
        binding.statVerified.text = dash
        binding.statVerified.setTextColor(color(R.color.on_surface))
        binding.statConns.text = dash
        binding.statCfst.text = dash
        val port = getSharedPreferences(PREFS, MODE_PRIVATE).getInt("listen_port", 1234)
        binding.statListen.text = "127.0.0.1:$port"
        binding.topText.text = dash
    }

    private fun renderStats(snap: StatusSnapshot) {
        binding.statPool.text = getString(R.string.pool_format, snap.poolSize, snap.poolMin)
        binding.statVerified.text = snap.verified.toString()
        binding.statVerified.setTextColor(color(if (snap.verified > 0) R.color.ok else R.color.on_surface))
        binding.statConns.text = snap.conns.toString()
        binding.statCfst.text =
            getString(if (snap.cfstRunning) R.string.cfst_running else R.string.cfst_idle)
        binding.statListen.text = snap.listen.ifEmpty { getString(R.string.value_dash) }
    }

    private fun renderTop(snap: StatusSnapshot) {
        if (snap.top.isEmpty()) {
            binding.topText.text = getString(R.string.top_empty)
            return
        }
        val sb = StringBuilder(getString(R.string.top_header))
        var anyVerified = false
        for ((i, e) in snap.top.take(8).withIndex()) {
            if (e.verified) anyVerified = true
            sb.append(
                String.format(
                    Locale.US, "%-3d %-16s %6.1f %5.0fms %5.1fM%s\n",
                    i + 1, e.ip, e.score, e.lat, e.speed, if (e.verified) " *" else ""
                )
            )
        }
        if (anyVerified) sb.append(getString(R.string.verified_legend))
        binding.topText.text = sb.toString()
    }

    private fun renderLog(tail: String) {
        if (binding.logText.hasSelection()) return // don't disturb copying
        val text = tail.ifEmpty { getString(R.string.log_empty) }
        if (text != binding.logText.text.toString()) binding.logText.text = text
    }

    // ---- helpers ----

    private fun color(id: Int): Int = ContextCompat.getColor(this, id)

    private fun toast(msg: String) = Toast.makeText(this, msg, Toast.LENGTH_SHORT).show()

    companion object {
        private const val PREFS = "cforward"
    }
}
