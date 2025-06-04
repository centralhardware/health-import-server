package me.centralhardware.healthImportServer.storage

import me.centralhardware.healthImportServer.request.*
import org.flywaydb.core.Flyway
import java.net.URI
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Timestamp
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

/** Simple ClickHouse implementation of a metric store. */
class ClickHouseMetricStore(private val config: ClickHouseConfig) : AutoCloseable {
    private val zonedTsFmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss Z")
    private val localTsFmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    private val dateFmt = DateTimeFormatter.ofPattern("yyyy-MM-dd")

    private fun parseTs(value: String): Timestamp {
        return try {
            val odt = OffsetDateTime.parse(value, zonedTsFmt)
            Timestamp.from(odt.toInstant())
        } catch (e: Exception) {
            try {
                val ldt = LocalDateTime.parse(value, localTsFmt)
                Timestamp.valueOf(ldt)
            } catch (e2: Exception) {
                val d = LocalDate.parse(value, dateFmt)
                Timestamp.valueOf(d.atStartOfDay())
            }
        }
    }
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
            INSERT INTO ${config.database}.metrics (timestamp, metric_name, metric_unit, qty)
            VALUES (?, ?, ?, ?)
        """.trimIndent()
        connection.prepareStatement(sql).use { stmt ->
            var count = 0
            for (m in metrics) {
                for (s in m.data) {
                    val ts = s.date ?: continue
                    val qty = s.qty ?: 0.0
                    println("Batching metric: ${m.name} (${m.units}) $ts -> $qty")
                    stmt.setTimestamp(1, parseTs(ts))
                    stmt.setString(2, m.name)
                    stmt.setString(3, m.units)
                    stmt.setDouble(4, qty)
                    stmt.addBatch()
                    count++
                }
            }
            println("Executing metric batch with $count rows")
            stmt.executeBatch()
        }
    }

    suspend fun storeWorkouts(workouts: List<Workout>) {
        if (workouts.isEmpty()) return
        val sql = """
            INSERT INTO ${config.database}.workouts (id, name, start, end)
            VALUES (?, ?, ?, ?)
        """.trimIndent()
        connection.prepareStatement(sql).use { stmt ->
            var count = 0
            for (w in workouts) {
                val id = w.id ?: continue
                val start = w.start ?: continue
                val end = w.end ?: continue
                println("Batching workout: $w")
                stmt.setString(1, id)
                stmt.setString(2, w.name ?: "")
                stmt.setTimestamp(3, parseTs(start))
                stmt.setTimestamp(4, parseTs(end))
                stmt.addBatch()
                count++
            }
            println("Executing workout batch with $count rows")
            stmt.executeBatch()
        }
    }

    suspend fun storeStateOfMind(stateOfMind: List<StateOfMind>) {
        if (stateOfMind.isEmpty()) return
        val sql = """
            INSERT INTO ${config.database}.state_of_mind
            (id, start, end, valence, valence_classification, kind, labels, associations)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """.trimIndent()
        connection.prepareStatement(sql).use { stmt ->
            var count = 0
            for (s in stateOfMind) {
                val id = s.id ?: continue
                val start = s.start ?: continue
                val end = s.end ?: continue
                println("Batching state of mind: $s")
                stmt.setString(1, id)
                stmt.setTimestamp(2, parseTs(start))
                stmt.setTimestamp(3, parseTs(end))
                stmt.setDouble(4, s.valence ?: 0.0)
                stmt.setString(5, s.valenceClassification ?: "")
                stmt.setString(6, s.kind ?: "")
                val labels = s.labels.joinToString(prefix = "[", postfix = "]", separator = ",") { "'$it'" }
                val assoc = s.associations.joinToString(prefix = "[", postfix = "]", separator = ",") { "'$it'" }
                stmt.setString(7, labels)
                stmt.setString(8, assoc)
                stmt.addBatch()
                count++
            }
            println("Executing state of mind batch with $count rows")
            stmt.executeBatch()
        }
    }

    suspend fun storeEcg(ecg: List<ECG>) {
        if (ecg.isEmpty()) return
        val sql = """
            INSERT INTO ${config.database}.ecg
            (id, classification, source, average_heart_rate, start, end, number_of_voltage_measurements, sampling_frequency)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """.trimIndent()
        val voltageSql = """
            INSERT INTO ${config.database}.ecg_voltage
            (ecg_id, sample_index, timestamp, voltage, units)
            VALUES (?, ?, ?, ?, ?)
        """.trimIndent()

        connection.prepareStatement(sql).use { ecgStmt ->
            connection.prepareStatement(voltageSql).use { voltStmt ->
                var ecgCount = 0
                var voltCount = 0
                for (e in ecg) {
                    val start = e.start ?: continue
                    val end = e.end ?: continue
                    val id = java.util.UUID.randomUUID().toString()

                    println("Batching ECG: $e as id $id")
                    ecgStmt.setString(1, id)
                    ecgStmt.setString(2, e.classification ?: "")
                    ecgStmt.setString(3, e.source ?: "")
                    ecgStmt.setDouble(4, e.averageHeartRate ?: 0.0)
                    ecgStmt.setTimestamp(5, parseTs(start))
                    ecgStmt.setTimestamp(6, parseTs(end))
                    ecgStmt.setInt(7, e.numberOfVoltageMeasurements ?: e.voltageMeasurements.size)
                    ecgStmt.setInt(8, e.samplingFrequency ?: 0)
                    ecgStmt.addBatch()
                    ecgCount++

                    var idx = 0
                    for (v in e.voltageMeasurements) {
                        val ts = v.date ?: continue
                        val volt = v.voltage ?: continue
                        println("Batching ECG voltage for $id: $v")
                        voltStmt.setString(1, id)
                        voltStmt.setInt(2, idx++)
                        voltStmt.setTimestamp(3, parseTs(ts))
                        voltStmt.setDouble(4, volt)
                        voltStmt.setString(5, v.units ?: "")
                        voltStmt.addBatch()
                        voltCount++
                    }
                }
                println("Executing ECG batch with $ecgCount entries and $voltCount voltage rows")
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
                stmt.addBatch("OPTIMIZE TABLE ${config.database}." + table)
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
