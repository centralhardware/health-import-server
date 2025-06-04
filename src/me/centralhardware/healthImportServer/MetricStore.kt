package me.centralhardware.healthImportServer

import me.centralhardware.healthImportServer.request.*

interface MetricStore : AutoCloseable {
    val name: String

    suspend fun store(metrics: List<Metric>)
    suspend fun storeWorkouts(workouts: List<Workout>)
    suspend fun storeStateOfMind(stateOfMind: List<StateOfMind>)
    suspend fun storeEcg(ecg: List<ECG>)
    suspend fun optimizeTables()
}
