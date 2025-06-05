package me.centralhardware.healthImportServer

import io.ktor.server.application.*
import io.ktor.server.request.receiveText
import io.ktor.server.response.respondText
import me.centralhardware.healthImportServer.request.RequestParser
import me.centralhardware.healthImportServer.storage.ClickHouseMetricStore
import kotlinx.coroutines.launch

/**
 * Kotlin implementation of the import handler found in handler.go
 */
class ImportHandler(private val metricStore: ClickHouseMetricStore) {

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
            println("Starting upload to metric store \"${metricStore.name}\".")

            metrics.takeIf { it.isNotEmpty() }?.let { localMetrics ->
                metricStore.store(localMetrics)
                val samples = localMetrics.sumOf { it.data.size }
                println("Saved ${localMetrics.size} metrics with $samples samples")
            }
            ecg.takeIf { it.isNotEmpty() }?.let { localEcg ->
                metricStore.storeEcg(localEcg)
                val voltages = localEcg.sumOf { it.voltageMeasurements.size }
                println("Saved ${localEcg.size} ECG entries with $voltages voltage measurements")
            }
            workouts.takeIf { it.isNotEmpty() }?.let { localWorkouts ->
                metricStore.storeWorkouts(localWorkouts)
                println("Saved ${localWorkouts.size} workouts")
            }
            stateOfMind.takeIf { it.isNotEmpty() }?.let { localStateOfMind ->
                metricStore.storeStateOfMind(localStateOfMind)
                println("Saved ${localStateOfMind.size} state of mind entries")
            }

            metricStore.optimizeTables()
            println("Finished upload to metric store \"${metricStore.name}\" and optimized tables.")
        }
    }
}
