package me.centralhardware.healthImportServer

import io.ktor.server.application.*
import io.ktor.server.request.receiveText
import io.ktor.server.response.respondText
import me.centralhardware.healthImportServer.request.RequestParser
import me.centralhardware.healthImportServer.storage.ClickHouseMetricStore
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory

class ImportHandler(private val metricStore: ClickHouseMetricStore) {
    val log = LoggerFactory.getLogger(ImportHandler::class.java)

    suspend fun handle(call: ApplicationCall) {
        val export = RequestParser.parse(call.receiveText())
        val metrics = export.populatedMetrics()
        val responseMsg = "Processing request. Received ${export.metrics.size} metrics " +
                "(${metrics.size} populated), ${export.totalSamples()} samples, " +
                "${export.workouts.size} workouts, ${export.stateOfMind.size} state of mind entries " +
                "and ${export.ecg.size} ECG recordings."

        call.respondText(responseMsg)

        val workouts = export.workouts
        val stateOfMind = export.stateOfMind
        val ecg = export.ecg

        call.application.launch {
            log.info("Starting upload to ClickHouse")

            metrics.takeIf { it.isNotEmpty() }?.let { localMetrics ->
                metricStore.store(localMetrics)
                val samples = localMetrics.sumOf { it.data.size }
                log.info("Saved ${localMetrics.size} metrics with $samples samples")
            }
            ecg.takeIf { it.isNotEmpty() }?.let { localEcg ->
                metricStore.storeEcg(localEcg)
                val voltages = localEcg.sumOf { it.voltageMeasurements.size }
                log.info("Saved ${localEcg.size} ECG entries with $voltages voltage measurements")
            }
            workouts.takeIf { it.isNotEmpty() }?.let { localWorkouts ->
                metricStore.storeWorkouts(localWorkouts)
                log.info("Saved ${localWorkouts.size} workouts")
            }
            stateOfMind.takeIf { it.isNotEmpty() }?.let { localStateOfMind ->
                metricStore.storeStateOfMind(localStateOfMind)
                log.info("Saved ${localStateOfMind.size} state of mind entries")
            }

            metricStore.optimizeTables()
            log.info("Finished upload to clickhouse and optimized tables.")
        }
    }
}
