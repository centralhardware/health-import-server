package me.centralhardware.healthImportServer.tools

import io.ktor.server.application.*
import io.ktor.server.request.receiveText
import io.ktor.server.response.respondText
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import java.nio.file.Files
import java.nio.file.Paths

/**
 * Utility server that dumps incoming requests to `request.json`.
 */
fun main() {
    val addr = System.getenv("ADDR") ?: "0.0.0.0:8080"
    val host = addr.substringBefore(":")
    val port = addr.substringAfter(":").toInt()

    embeddedServer(Netty, host = host, port = port) {
        routing {
            post("/") {
                val body = call.receiveText()
                Files.writeString(Paths.get("request.json"), body)
                call.respondText("Written to request.json")
            }
        }
    }.start(wait = true)
}
