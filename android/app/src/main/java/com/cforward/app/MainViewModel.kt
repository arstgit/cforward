package com.cforward.app

import android.app.Application
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.io.File

/** Everything the single screen renders. */
data class UiState(
    val running: Boolean = false,
    val snapshot: StatusSnapshot? = null,
    val logTail: String = "",
)

/**
 * Polls the relay core every 2 seconds and exposes an immutable UiState.
 * Lives across configuration changes; polling stops with the ViewModel.
 */
class MainViewModel(app: Application) : AndroidViewModel(app) {

    private val repo = StatusRepository()
    private val logFile = File(app.filesDir, "cforward.log")

    private val _state = MutableStateFlow(UiState())
    val state: StateFlow<UiState> = _state.asStateFlow()

    init {
        viewModelScope.launch {
            while (isActive) {
                val snapshot = repo.fetch()
                val log = readLogTail()
                _state.value = UiState(
                    running = snapshot != null,
                    snapshot = snapshot,
                    logTail = log,
                )
                delay(POLL_INTERVAL_MS)
            }
        }
    }

    private suspend fun readLogTail(): String = withContext(Dispatchers.IO) {
        try {
            if (logFile.exists()) logFile.readLines().takeLast(40).joinToString("\n") else ""
        } catch (e: Exception) {
            ""
        }
    }

    companion object {
        private const val POLL_INTERVAL_MS = 2000L
    }
}
