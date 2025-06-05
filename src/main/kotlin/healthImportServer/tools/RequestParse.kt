package me.centralhardware.healthImportServer.tools

import me.centralhardware.healthImportServer.request.RequestParser
import java.nio.file.Files
import java.nio.file.Paths

/**
 * Parses `request.json` and prints the parsed data as JSON to stdout.
 */
fun main() {
    val body = Files.readString(Paths.get("request.json"))
    val export = RequestParser.parse(body)
    println("Metrics:")
    for (metric in export.metrics) {
        for (sample in metric.data) {
            println("\t${metric.name} (${metric.units}): $sample")
        }
    }

    println("\nWorkouts:")
    for (workout in export.workouts) {
        println("\t$workout")
        for (route in workout.route) println("\t\tRoute: $route")
        for (hr in workout.heartRateData) println("\t\tHeartRateData: $hr")
        for (hr in workout.heartRateRecovery) println("\t\tHeartRateRecovery: $hr")
        for (sc in workout.stepCount) println("\t\tStepCount: $sc")
        for (dist in workout.walkingAndRunningDistance) println("\t\tWalkingRunningDistance: $dist")
        for (ae in workout.activeEnergy) println("\t\tActiveEnergy: $ae")
    }

    if (export.stateOfMind.isNotEmpty()) {
        println("\nState of mind:")
        for (s in export.stateOfMind) println("\t$s")
    }

    if (export.ecg.isNotEmpty()) {
        println("\nECG:")
        for (e in export.ecg) {
            println("\t$e")
            for (v in e.voltageMeasurements) println("\t\t$v")
        }
    }
}

