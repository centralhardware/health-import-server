package me.centralhardware.healthImportServer

import io.ktor.server.application.*
import io.ktor.server.request.receiveText
import io.ktor.server.response.respondText
import me.centralhardware.healthImportServer.request.RequestParser
import me.centralhardware.healthImportServer.storage.ClickHouseMetricStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch

/**
 * Kotlin implementation of the import handler found in handler.go
 */
class ImportHandler(private val metricStore: ClickHouseMetricStore) {

    suspend fun handle(call: ApplicationCall) {
        val body = call.receiveText()
        val export = RequestParser.parse(body)
        val metrics = export.populatedMetrics()
        val responseMsg = "Processing request. Received ${export.metrics.size} metrics " +
                "(${metrics.size} populated), ${export.totalSamples()} samples, " +
                "${export.workouts.size} workouts, ${export.stateOfMind.size} state of mind entries " +
                "and ${export.ecg.size} ECG recordings."

        call.respondText(responseMsg)

        CoroutineScope(Dispatchers.Default).launch {
            val localMetrics = metrics
            val localWorkouts = export.workouts
            val localStateOfMind = export.stateOfMind
            val localEcg = export.ecg

            println("Starting upload to metric store \"${metricStore.name}\".")

            if (localMetrics.isNotEmpty()) metricStore.store(localMetrics)
            if (localEcg.isNotEmpty()) metricStore.storeEcg(localEcg)
            if (localWorkouts.isNotEmpty()) metricStore.storeWorkouts(localWorkouts)
            if (localStateOfMind.isNotEmpty()) metricStore.storeStateOfMind(localStateOfMind)

            metricStore.optimizeTables()
            println("Finished upload to metric store \"${metricStore.name}\" and optimized tables.")
        }
    }
}
