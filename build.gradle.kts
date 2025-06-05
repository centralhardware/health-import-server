plugins {
    application
    kotlin("jvm") version "2.1.21"
    kotlin("plugin.serialization") version "2.1.21"
}

application {
    mainClass.set("me.centralhardware.healthImportServer.ServerKt")
}

repositories {
    mavenCentral()
}

val ktorVersion = "3.1.3"
dependencies {
    implementation("io.ktor:ktor-server-netty:$ktorVersion")
    implementation("io.ktor:ktor-server-core:$ktorVersion")
    implementation("io.ktor:ktor-server-content-negotiation:$ktorVersion")
    implementation("io.ktor:ktor-serialization-kotlinx-json:$ktorVersion")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
    implementation("com.clickhouse:clickhouse-jdbc:0.8.6")
    implementation("org.flywaydb:flyway-core:11.9.0")
    implementation("org.flywaydb:flyway-database-clickhouse:10.18.0")
    implementation("org.slf4j:slf4j-simple:2.0.17")
    testImplementation(kotlin("test"))
}

kotlin {
    jvmToolchain(21)
}
