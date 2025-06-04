package com.example.storage

import com.example.*
import org.flywaydb.core.Flyway
import java.net.URI
import java.sql.Connection
import java.sql.DriverManager

/** Simple ClickHouse implementation of [MetricStore]. */
class ClickHouseMetricStore(private val config: ClickHouseConfig) : MetricStore {
    override val name: String = "clickhouse"
    private val connection: Connection

    init {
        val jdbcUrl = if (config.dsn.startsWith("clickhouse://")) {
            "jdbc:" + config.dsn
        } else config.dsn
        val uri = URI(config.dsn)
        val creds = uri.userInfo?.split(":", limit = 2) ?: emptyList()
        val user = creds.getOrNull(0)
        val password = creds.getOrNull(1)

        Flyway.configure()
            .dataSource(jdbcUrl, user, password)
            .locations("classpath:db/migration")
            .placeholders(mapOf("database" to config.database))
            .load()
            .migrate()

        connection = DriverManager.getConnection(jdbcUrl)
    }

    override suspend fun store(metrics: List<Metric>) {
        if (metrics.isEmpty()) return
        val sql = """
            INSERT INTO ${'$'}{config.database}.metrics (timestamp, metric_name, metric_unit, qty)
            VALUES (?, ?, ?, ?)
        """.trimIndent()
        connection.prepareStatement(sql).use { stmt ->
            for (m in metrics) {
                for (s in m.samples) {
                    val ts = s.date ?: continue
                    val qty = s.qty ?: 0.0
                    stmt.setString(1, ts)
                    stmt.setString(2, m.name)
                    stmt.setString(3, m.unit)
                    stmt.setDouble(4, qty)
                    stmt.addBatch()
                }
            }
            stmt.executeBatch()
        }
    }

    override suspend fun storeWorkouts(workouts: List<Workout>) { /* no-op */ }
    override suspend fun storeStateOfMind(stateOfMind: List<StateOfMind>) { /* no-op */ }
    override suspend fun storeEcg(ecg: List<ECG>) { /* no-op */ }
    override suspend fun optimizeTables() { /* no-op */ }

    override fun close() { connection.close() }
}

data class ClickHouseConfig(
    val dsn: String,
    val database: String,
)
