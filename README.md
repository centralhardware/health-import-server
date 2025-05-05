# Health Import Server
Storage backend for https://www.healthexportapp.com

Official documentation on the JSON format that the request submodule parses can be found here: https://github.com/Lybron/health-auto-export/wiki/API-Export---JSON-Format

Currently supports storing metrics in ClickHouse. More storage backends (and storing workout data) may be supported in the future.

## Config file
You'll need provide a json config file with the details on how to connect to your database instance:
```
[
	{
		"type": "clickhouse",
		"dsn": "clickhouse://username:password@hostname:9000/database?dial_timeout=10s",
		"database": "health",
		"metrics_table": "metrics",
		"create_tables": true
	}
]
```


## Running in docker
The image can be built with this command (not on dockerhub yet):
```
docker build -t health-import:latest
```

To provide the config file to the application you need to place it here: /config/config.json.

Alternatively, you can configure the application using environment variables:
- `CLICKHOUSE_DSN`: The DSN (Data Source Name) for connecting to ClickHouse
- `CLICKHOUSE_DATABASE`: The database in ClickHouse to store metrics
- `CLICKHOUSE_METRICS_TABLE`: The table in ClickHouse to store metrics
- `CLICKHOUSE_CREATE_TABLES`: Whether to create tables if they don't exist (true/false)

You can either do this with a bind mount e.g.
```
docker run -v $(PWD)/config:/config health-import:latest
```

Or making an image which extends the base image:
```
FROM health-import:latest
ADD config.json /config/config.json
```
(docker-compose works well with this approach)

## What the metrics look like
See this file: [sample.go](/request/sample.go)

## How to use this with Health Export App (aka. API Export)
1. Run the server on a machine on your local home network.
2. Configure the API Export to point to the server.
3. Enable automatic syncing 
