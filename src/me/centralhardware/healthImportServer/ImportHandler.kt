package me.centralhardware.healthImportServer

import io.ktor.application.*
import io.ktor.request.receiveText
import io.ktor.response.respondText
import com.example.request.RequestParser
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch

/**
 * Kotlin implementation of the import handler found in handler.go
 */
class ImportHandler(private val metricStores: List<MetricStore>) {

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

            for (store in metricStores) {
                println("Starting upload to metric store \"${store.name}\".")

                if (localMetrics.isNotEmpty()) store.store(localMetrics)
                if (localEcg.isNotEmpty()) store.storeEcg(localEcg)
                if (localWorkouts.isNotEmpty()) store.storeWorkouts(localWorkouts)
                if (localStateOfMind.isNotEmpty()) store.storeStateOfMind(localStateOfMind)

                store.optimizeTables()
                println("Finished upload to metric store \"${store.name}\" and optimized tables.")
            }
        }
    }
}
