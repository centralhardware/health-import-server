package me.centralhardware.healthImportServer

import io.ktor.server.application.*
import io.ktor.server.routing.*
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import me.centralhardware.healthImportServer.storage.ClickHouseConfig
import me.centralhardware.healthImportServer.storage.ClickHouseMetricStore

/**
 * Simple Ktor server similar to cmd/server/main.go
 */
fun main() {
    val metricStore = loadMetricStore()
    val handler = ImportHandler(metricStore)

    embeddedServer(Netty, port = 8080) {
        routing {
            post("/upload") {
                handler.handle(call)
            }
        }
    }.start(wait = true)
}

fun loadMetricStore(): ClickHouseMetricStore {
    val dsn = System.getenv("CLICKHOUSE_DSN")
        ?: error("CLICKHOUSE_DSN must be set")
    val db = System.getenv("CLICKHOUSE_DATABASE")
        ?: error("CLICKHOUSE_DATABASE must be set")
    val store = ClickHouseMetricStore(ClickHouseConfig(dsn, db))
    return store
}

