plugins {
    application
    kotlin("jvm") version "2.0.21"
    kotlin("plugin.serialization") version "2.0.21"
}

application {
    mainClass.set("me.centralhardware.healthImportServer.ServerKt")
}

sourceSets {
    main {
        kotlin.srcDir("src")
        resources.srcDir("migration")
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("io.ktor:ktor-server-netty:2.3.7")
    implementation("io.ktor:ktor-server-core:2.3.7")
    implementation("io.ktor:ktor-server-content-negotiation:2.3.7")
    implementation("io.ktor:ktor-serialization-kotlinx-json:2.3.7")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.0")
    implementation("com.clickhouse:clickhouse-jdbc:0.8.6")
    implementation("org.flywaydb:flyway-core:10.14.0")
    implementation("org.slf4j:slf4j-simple:2.0.12")
    testImplementation(kotlin("test"))
}

kotlin {
    jvmToolchain(21)
}
