package me.centralhardware.healthImportServer.storage

import me.centralhardware.healthImportServer.request.*
import org.flywaydb.core.Flyway
import java.net.URI
import java.sql.Connection
import java.sql.DriverManager

/** Simple ClickHouse implementation of a metric store. */
class ClickHouseMetricStore(private val config: ClickHouseConfig) : AutoCloseable {
    val name: String = "clickhouse"
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
            .locations("classpath:migration")
            .placeholders(mapOf("database" to config.database))
            .load()
            .migrate()

        connection = DriverManager.getConnection(jdbcUrl)
    }

    suspend fun store(metrics: List<Metric>) {
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

    suspend fun storeWorkouts(workouts: List<Workout>) {
        if (workouts.isEmpty()) return
        val sql = """
            INSERT INTO ${'$'}{config.database}.workouts (id, name, start, end)
            VALUES (?, ?, ?, ?)
        """.trimIndent()
        connection.prepareStatement(sql).use { stmt ->
            for (w in workouts) {
                val id = w.id ?: continue
                val start = w.start ?: continue
                val end = w.end ?: continue
                stmt.setString(1, id)
                stmt.setString(2, w.name ?: "")
                stmt.setString(3, start)
                stmt.setString(4, end)
                stmt.addBatch()
            }
            stmt.executeBatch()
        }
    }

    suspend fun storeStateOfMind(stateOfMind: List<StateOfMind>) {
        if (stateOfMind.isEmpty()) return
        val sql = """
            INSERT INTO ${'$'}{config.database}.state_of_mind
            (id, start, end, valence, valence_classification, kind, labels, associations)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """.trimIndent()
        connection.prepareStatement(sql).use { stmt ->
            for (s in stateOfMind) {
                val id = s.id ?: continue
                val start = s.start ?: continue
                val end = s.end ?: continue
                stmt.setString(1, id)
                stmt.setString(2, start)
                stmt.setString(3, end)
                stmt.setDouble(4, s.valence ?: 0.0)
                stmt.setString(5, s.valenceClassification ?: "")
                stmt.setString(6, s.kind ?: "")
                val labels = s.labels.joinToString(prefix = "[", postfix = "]", separator = ",") { "'${'$'}it'" }
                val assoc = s.associations.joinToString(prefix = "[", postfix = "]", separator = ",") { "'${'$'}it'" }
                stmt.setString(7, labels)
                stmt.setString(8, assoc)
                stmt.addBatch()
            }
            stmt.executeBatch()
        }
    }

    suspend fun storeEcg(ecg: List<ECG>) {
        if (ecg.isEmpty()) return
        val sql = """
            INSERT INTO ${'$'}{config.database}.ecg
            (id, classification, source, average_heart_rate, start, end, number_of_voltage_measurements, sampling_frequency)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """.trimIndent()
        val voltageSql = """
            INSERT INTO ${'$'}{config.database}.ecg_voltage
            (ecg_id, sample_index, timestamp, voltage, units)
            VALUES (?, ?, ?, ?, ?)
        """.trimIndent()

        connection.prepareStatement(sql).use { ecgStmt ->
            connection.prepareStatement(voltageSql).use { voltStmt ->
                for (e in ecg) {
                    val start = e.start ?: continue
                    val end = e.end ?: continue
                    val id = java.util.UUID.randomUUID().toString()

                    ecgStmt.setString(1, id)
                    ecgStmt.setString(2, e.classification ?: "")
                    ecgStmt.setString(3, e.source ?: "")
                    ecgStmt.setDouble(4, e.averageHeartRate ?: 0.0)
                    ecgStmt.setString(5, start)
                    ecgStmt.setString(6, end)
                    ecgStmt.setInt(7, e.numberOfVoltageMeasurements ?: e.voltageMeasurements.size)
                    ecgStmt.setInt(8, e.samplingFrequency ?: 0)
                    ecgStmt.addBatch()

                    var idx = 0
                    for (v in e.voltageMeasurements) {
                        val ts = v.date ?: continue
                        val volt = v.voltage ?: continue
                        voltStmt.setString(1, id)
                        voltStmt.setInt(2, idx++)
                        voltStmt.setString(3, ts)
                        voltStmt.setDouble(4, volt)
                        voltStmt.setString(5, v.units ?: "")
                        voltStmt.addBatch()
                    }
                }
                ecgStmt.executeBatch()
                voltStmt.executeBatch()
            }
        }
    }

    suspend fun optimizeTables() {
        val tables = listOf(
            "metrics",
            "workouts",
            "workout_routes",
            "workout_heart_rate_data",
            "workout_heart_rate_recovery",
            "workout_step_count_log",
            "workout_walking_running_distance",
            "workout_active_energy",
            "ecg",
            "ecg_voltage",
            "state_of_mind"
        )
        connection.createStatement().use { stmt ->
            for (table in tables) {
                stmt.addBatch("OPTIMIZE TABLE ${'$'}{config.database}." + table)
            }
            stmt.executeBatch()
        }
    }

    override fun close() { connection.close() }
}

data class ClickHouseConfig(
    val dsn: String,
    val database: String,
)
