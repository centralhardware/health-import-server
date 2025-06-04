package me.centralhardware.healthImportServer

import com.example.request.Metric
import com.example.request.Workout
import com.example.request.StateOfMind
import com.example.request.ECG

interface MetricStore {
    val name: String
    suspend fun store(metrics: List<Metric>)
    suspend fun storeWorkouts(workouts: List<Workout>)
    suspend fun storeStateOfMind(stateOfMind: List<StateOfMind>)
    suspend fun storeEcg(ecg: List<ECG>)
    suspend fun optimizeTables()
    fun close()
}
