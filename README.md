# Health Import Server
Storage backend for https://www.healthexportapp.com

Official documentation on the JSON format that the request submodule parses can be found here: https://github.com/Lybron/health-auto-export/wiki/API-Export---JSON-Format

The server is implemented in Kotlin using Ktor and stores metrics in ClickHouse. ECG voltages include an additional `sample_index` column to avoid deduplication when timestamps repeat and use `DateTime64` to preserve sub-second precision. Each ECG entry id is generated deterministically from the record data so repeated uploads replace existing rows.
Database schema migrations are managed with Flyway and run automatically when the server starts. Migration scripts live under `src/main/resources/db/migration` and create all eleven tables (`metrics`, `workouts`, `state_of_mind`, `workout_routes`, `workout_heart_rate_data`, `workout_heart_rate_recovery`, `workout_step_count_log`, `workout_walking_running_distance`, `workout_active_energy`, `ecg`, and `ecg_voltage`).

## Flyway and ClickHouse
Flyway requires a ClickHouse extension in order to recognize `jdbc:clickhouse` URLs. The Gradle build includes this dependency:

```kotlin
implementation("org.flywaydb:flyway-database-clickhouse:10.18.0")
```

If this dependency is missing you'll encounter `No database found to handle jdbc:clickhouse` during startup.

## Configuration
You can configure the application using environment variables:
- `CLICKHOUSE_DSN`: The DSN (Data Source Name) for connecting to ClickHouse
- `CLICKHOUSE_DATABASE`: The database in ClickHouse to store metrics


## Running in docker
The image can be built with this command (not on dockerhub yet):
```
docker build -t health-import:latest
```

You can run the container with environment variables:
```
docker run -e CLICKHOUSE_DSN=clickhouse://username:password@hostname:9000/database -e CLICKHOUSE_DATABASE=health health-import:latest
```

Or using docker-compose:
```yaml
version: '3'
services:
  health-import:
    image: health-import:latest
    environment:
      - CLICKHOUSE_DSN=clickhouse://username:password@hostname:9000/database
      - CLICKHOUSE_DATABASE=health
```

## How to use this with Health Export App (aka. API Export)
1. Run the server on a machine on your local home network.
2. Configure the API Export to point to the server.
3. Enable automatic syncing 

## Kotlin Server
The server is written in Kotlin using Ktor. Upload requests are accepted on `/upload`.
Run the application locally with Gradle:

```bash
gradle run
```


