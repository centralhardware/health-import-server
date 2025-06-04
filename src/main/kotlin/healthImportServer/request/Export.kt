package me.centralhardware.healthImportServer.request

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.decodeFromString

@Serializable
data class ExportWrapper(val data: Export)

@Serializable
data class Export(
    val metrics: List<Metric> = emptyList(),
    val workouts: List<Workout> = emptyList(),
    val stateOfMind: List<StateOfMind> = emptyList(),
    val ecg: List<ECG> = emptyList()
) {
    fun populatedMetrics(): List<Metric> = metrics.filter { it.samples.isNotEmpty() }
    fun totalSamples(): Int = metrics.sumOf { it.samples.size }
}

@Serializable
data class Metric(
    val name: String,
    val units: String,
    val samples: List<Sample> = emptyList()
)

@Serializable
data class Sample(
    val date: String? = null,
    val qty: Double? = null,
    val max: Double? = null,
    val min: Double? = null,
    val avg: Double? = null,
    val asleep: Double? = null,
    val inBed: Double? = null,
    val sleepSource: String? = null,
    val inBedSource: String? = null
)

@Serializable
data class Workout(
    val id: String? = null,
    val name: String? = null,
    val start: String? = null,
    val end: String? = null
)

@Serializable
data class StateOfMind(
    val id: String? = null,
    val valence: Double? = null,
    val valenceClassification: String? = null,
    val labels: List<String> = emptyList(),
    val associations: List<String> = emptyList(),
    val start: String? = null,
    val end: String? = null,
    val kind: String? = null
)

@Serializable
data class ECG(
    val classification: String? = null,
    val voltageMeasurements: List<ECGVoltage> = emptyList(),
    val source: String? = null,
    val averageHeartRate: Double? = null,
    val start: String? = null,
    val numberOfVoltageMeasurements: Int? = null,
    val samplingFrequency: Int? = null,
    val end: String? = null
)

@Serializable
data class ECGVoltage(
    val date: String? = null,
    val voltage: Double? = null,
    val units: String? = null
)

object RequestParser {
    private val json = Json { ignoreUnknownKeys = true }

    fun parse(body: String): Export {
        val wrapper = json.decodeFromString<ExportWrapper>(body)
        return wrapper.data
    }
}
