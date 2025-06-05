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
            Timestamp.from(java.time.Instant.parse(value))
        } catch (_: Exception) {
            try {
                val odt = OffsetDateTime.parse(value, zonedTsFmt)
                Timestamp.from(odt.toInstant())
            } catch (_: Exception) {
                try {
                    val ldt = LocalDateTime.parse(value, localTsFmt)
                    Timestamp.valueOf(ldt)
                } catch (_: Exception) {
                    val d = LocalDate.parse(value, dateFmt)
                    Timestamp.valueOf(d.atStartOfDay())
                }
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
            INSERT INTO ${config.database}.workouts
            (id, name, start, end,
             active_energy_qty, active_energy_units,
             distance_qty, distance_units,
             intensity_qty, intensity_units,
             humidity_qty, humidity_units,
             temperature_qty, temperature_units)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                stmt.setDouble(5, w.activeEnergyBurned?.qty ?: 0.0)
                stmt.setString(6, w.activeEnergyBurned?.units ?: "")
                stmt.setDouble(7, w.distance?.qty ?: 0.0)
                stmt.setString(8, w.distance?.units ?: "")
                stmt.setDouble(9, w.intensity?.qty ?: 0.0)
                stmt.setString(10, w.intensity?.units ?: "")
                stmt.setDouble(11, w.humidity?.qty ?: 0.0)
                stmt.setString(12, w.humidity?.units ?: "")
                stmt.setDouble(13, w.temperature?.qty ?: 0.0)
                stmt.setString(14, w.temperature?.units ?: "")
                stmt.addBatch()
                count++
            }
            println("Executing workout batch with $count rows")
            stmt.executeBatch()
        }

        storeWorkoutRoutes(workouts)
        storeWorkoutHeartRateData(workouts)
        storeWorkoutHeartRateRecovery(workouts)
        storeWorkoutStepCountLog(workouts)
        storeWorkoutWalkingRunningDistance(workouts)
        storeWorkoutActiveEnergy(workouts)
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
                    val base = listOf(
                        start,
                        end,
                        e.classification ?: "",
                        e.source ?: "",
                        (e.averageHeartRate ?: 0.0).toString(),
                        (e.numberOfVoltageMeasurements ?: e.voltageMeasurements.size).toString(),
                        (e.samplingFrequency ?: 0).toString()
                    ).joinToString("|")
                    val id = java.util.UUID.nameUUIDFromBytes(base.toByteArray()).toString()

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
                        val instant = java.time.Instant.ofEpochMilli((ts * 1000).toLong())
                        voltStmt.setTimestamp(3, java.sql.Timestamp.from(instant))
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

    private fun storeWorkoutRoutes(workouts: List<Workout>) {
        val sql = """
            INSERT INTO ${config.database}.workout_routes
            (workout_id, timestamp, lat, lon, altitude, course, vertical_accuracy,
             horizontal_accuracy, course_accuracy, speed, speed_accuracy)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """.trimIndent()
        connection.prepareStatement(sql).use { stmt ->
            var count = 0
            for (w in workouts) {
                val id = w.id ?: continue
                val start = w.start ?: continue
                for (r in w.route) {
                    val ts = r.timestamp ?: start
                    println("Batching workout route for $id: $r")
                    stmt.setString(1, id)
                    stmt.setTimestamp(2, parseTs(ts))
                    stmt.setDouble(3, r.latitude ?: 0.0)
                    stmt.setDouble(4, r.longitude ?: 0.0)
                    stmt.setDouble(5, r.altitude ?: 0.0)
                    stmt.setDouble(6, r.course ?: 0.0)
                    stmt.setDouble(7, r.verticalAccuracy ?: 0.0)
                    stmt.setDouble(8, r.horizontalAccuracy ?: 0.0)
                    stmt.setDouble(9, r.courseAccuracy ?: 0.0)
                    stmt.setDouble(10, r.speed ?: 0.0)
                    stmt.setDouble(11, r.speedAccuracy ?: 0.0)
                    stmt.addBatch()
                    count++
                }
            }
            if (count > 0) {
                println("Executing workout route batch with $count rows")
                stmt.executeBatch()
            }
        }
    }

    private fun storeWorkoutHeartRateData(workouts: List<Workout>) {
        val sql = """
            INSERT INTO ${config.database}.workout_heart_rate_data
            (workout_id, timestamp, qty, min, max, avg, units, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """.trimIndent()
        connection.prepareStatement(sql).use { stmt ->
            var count = 0
            for (w in workouts) {
                val id = w.id ?: continue
                val start = w.start ?: continue
                for (h in w.heartRateData) {
                    val ts = h.date ?: start
                    println("Batching workout heart rate data for $id: $h")
                    stmt.setString(1, id)
                    stmt.setTimestamp(2, parseTs(ts))
                    stmt.setDouble(3, h.qty ?: 0.0)
                    stmt.setDouble(4, h.min ?: 0.0)
                    stmt.setDouble(5, h.max ?: 0.0)
                    stmt.setDouble(6, h.avg ?: 0.0)
                    stmt.setString(7, h.units ?: "")
                    stmt.setString(8, h.source ?: "")
                    stmt.addBatch()
                    count++
                }
            }
            if (count > 0) {
                println("Executing workout heart rate data batch with $count rows")
                stmt.executeBatch()
            }
        }
    }

    private fun storeWorkoutHeartRateRecovery(workouts: List<Workout>) {
        val sql = """
            INSERT INTO ${config.database}.workout_heart_rate_recovery
            (workout_id, timestamp, qty, min, max, avg, units, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """.trimIndent()
        connection.prepareStatement(sql).use { stmt ->
            var count = 0
            for (w in workouts) {
                val id = w.id ?: continue
                val start = w.start ?: continue
                for (h in w.heartRateRecovery) {
                    val ts = h.date ?: start
                    println("Batching workout heart rate recovery for $id: $h")
                    stmt.setString(1, id)
                    stmt.setTimestamp(2, parseTs(ts))
                    stmt.setDouble(3, h.qty ?: 0.0)
                    stmt.setDouble(4, h.min ?: 0.0)
                    stmt.setDouble(5, h.max ?: 0.0)
                    stmt.setDouble(6, h.avg ?: 0.0)
                    stmt.setString(7, h.units ?: "")
                    stmt.setString(8, h.source ?: "")
                    stmt.addBatch()
                    count++
                }
            }
            if (count > 0) {
                println("Executing workout heart rate recovery batch with $count rows")
                stmt.executeBatch()
            }
        }
    }

    private fun storeWorkoutStepCountLog(workouts: List<Workout>) {
        val sql = """
            INSERT INTO ${config.database}.workout_step_count_log
            (workout_id, timestamp, qty, units, source)
            VALUES (?, ?, ?, ?, ?)
        """.trimIndent()
        connection.prepareStatement(sql).use { stmt ->
            var count = 0
            for (w in workouts) {
                val id = w.id ?: continue
                val start = w.start ?: continue
                for (s in w.stepCount) {
                    val ts = s.date ?: start
                    println("Batching step count log for $id: $s")
                    stmt.setString(1, id)
                    stmt.setTimestamp(2, parseTs(ts))
                    stmt.setDouble(3, s.qty ?: 0.0)
                    stmt.setString(4, s.units ?: "")
                    stmt.setString(5, s.source ?: "")
                    stmt.addBatch()
                    count++
                }
            }
            if (count > 0) {
                println("Executing workout step count batch with $count rows")
                stmt.executeBatch()
            }
        }
    }

    private fun storeWorkoutWalkingRunningDistance(workouts: List<Workout>) {
        val sql = """
            INSERT INTO ${config.database}.workout_walking_running_distance
            (workout_id, timestamp, qty, units, source)
            VALUES (?, ?, ?, ?, ?)
        """.trimIndent()
        connection.prepareStatement(sql).use { stmt ->
            var count = 0
            for (w in workouts) {
                val id = w.id ?: continue
                val start = w.start ?: continue
                for (s in w.walkingAndRunningDistance) {
                    val ts = s.date ?: start
                    println("Batching walking/running distance for $id: $s")
                    stmt.setString(1, id)
                    stmt.setTimestamp(2, parseTs(ts))
                    stmt.setDouble(3, s.qty ?: 0.0)
                    stmt.setString(4, s.units ?: "")
                    stmt.setString(5, s.source ?: "")
                    stmt.addBatch()
                    count++
                }
            }
            if (count > 0) {
                println("Executing workout walking/running distance batch with $count rows")
                stmt.executeBatch()
            }
        }
    }

    private fun storeWorkoutActiveEnergy(workouts: List<Workout>) {
        val sql = """
            INSERT INTO ${config.database}.workout_active_energy
            (workout_id, timestamp, qty, units, source)
            VALUES (?, ?, ?, ?, ?)
        """.trimIndent()
        connection.prepareStatement(sql).use { stmt ->
            var count = 0
            for (w in workouts) {
                val id = w.id ?: continue
                val start = w.start ?: continue
                for (s in w.activeEnergy) {
                    val ts = s.date ?: start
                    println("Batching workout active energy for $id: $s")
                    stmt.setString(1, id)
                    stmt.setTimestamp(2, parseTs(ts))
                    stmt.setDouble(3, s.qty ?: 0.0)
                    stmt.setString(4, s.units ?: "")
                    stmt.setString(5, s.source ?: "")
                    stmt.addBatch()
                    count++
                }
            }
            if (count > 0) {
                println("Executing workout active energy batch with $count rows")
                stmt.executeBatch()
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
