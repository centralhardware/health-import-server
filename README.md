# Health Import Server
Storage backend for https://www.healthexportapp.com

Official documentation on the JSON format that the request submodule parses can be found here: https://github.com/Lybron/health-auto-export/wiki/API-Export---JSON-Format

Currently supports storing metrics, workouts, state of mind entries and ECG recordings in ClickHouse. ECG voltages include an additional `sample_index` column to avoid deduplication when timestamps repeat and use `DateTime64` to preserve sub-second precision. More storage backends may be supported in the future.

## Configuration
You can configure the application using environment variables:
- `CLICKHOUSE_DSN`: The DSN (Data Source Name) for connecting to ClickHouse
- `CLICKHOUSE_DATABASE`: The database in ClickHouse to store metrics
- `CLICKHOUSE_METRICS_TABLE`: The table in ClickHouse to store metrics


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

## What the metrics look like
See this file: [sample.go](/request/sample.go)

## How to use this with Health Export App (aka. API Export)
1. Run the server on a machine on your local home network.
2. Configure the API Export to point to the server.
3. Enable automatic syncing 
