package com.cforward.app

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.json.JSONObject
import java.net.HttpURLConnection
import java.net.URL

/** One row of the admin endpoint's IP ranking. */
data class TopIp(
    val ip: String,
    val score: Double,
    val lat: Double,
    val speed: Double,
    val verified: Boolean,
)

/** Snapshot of the Go core's admin /status response. */
data class StatusSnapshot(
    val poolSize: Int,
    val poolMin: Int,
    val verified: Int,
    val conns: Int,
    val cfstRunning: Boolean,
    val listen: String,
    val top: List<TopIp>,
)

/**
 * Reads the relay core's admin endpoint (127.0.0.1:18080/status).
 * Returns null when the core is not running (endpoint unreachable).
 */
class StatusRepository {

    suspend fun fetch(): StatusSnapshot? = withContext(Dispatchers.IO) {
        try {
            val conn = (URL(ENDPOINT).openConnection() as HttpURLConnection).apply {
                connectTimeout = 2000
                readTimeout = 2000
            }
            val body = conn.inputStream.bufferedReader().use { it.readText() }
            conn.disconnect()
            parse(JSONObject(body))
        } catch (e: Exception) {
            null
        }
    }

    private fun parse(j: JSONObject): StatusSnapshot {
        val pool = j.getJSONObject("pool")
        val topArr = pool.getJSONArray("top")
        val top = ArrayList<TopIp>(topArr.length())
        for (i in 0 until topArr.length()) {
            val e = topArr.getJSONObject(i)
            top.add(
                TopIp(
                    ip = e.getString("ip"),
                    score = e.getDouble("score"),
                    lat = e.getDouble("lat"),
                    speed = e.getDouble("speed"),
                    verified = e.optBoolean("verified"),
                )
            )
        }
        return StatusSnapshot(
            poolSize = pool.getInt("size"),
            poolMin = pool.optInt("min"),
            verified = pool.optInt("verified"),
            conns = j.optInt("active_conns"),
            cfstRunning = j.getJSONObject("cfst").optBoolean("running"),
            listen = j.optString("listen"),
            top = top,
        )
    }

    companion object {
        private const val ENDPOINT = "http://127.0.0.1:18080/status"
    }
}
