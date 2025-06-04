package me.centralhardware.healthImportServer.tools

import me.centralhardware.healthImportServer.request.RequestParser
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.nio.file.Files
import java.nio.file.Paths

/**
 * Parses `request.json` and prints the parsed data as JSON to stdout.
 */
fun main(args: Array<String>) {
    val logJson = args.firstOrNull() != "--no-log"
    val body = Files.readString(Paths.get("request.json"))
    val export = RequestParser.parse(body)
    if (logJson) {
        val json = Json { prettyPrint = true }
        println("Metrics:")
        println(json.encodeToString(export.metrics))
        println("\nWorkouts:")
        println(json.encodeToString(export.workouts))
    }
}

