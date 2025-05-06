package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/joeecarter/health-import-server/request"
	"time"
)

type ClickHouseConfig struct {
	DSN           string `json:"dsn"`
	Database      string `json:"database"`
	MetricsTable  string `json:"metrics_table"`
	WorkoutsTable string `json:"workouts_table"`
	CreateTables  bool   `json:"create_tables"`
}

type ClickHouseMetricStore struct {
	db                     *sql.DB
	database               string
	metricsTable           string
	workoutsTable          string
	routesTable            string
	heartRateDataTable     string
	heartRateRecoveryTable string
}

func NewClickHouseMetricStore(config ClickHouseConfig) (*ClickHouseMetricStore, error) {
	db, err := sql.Open("clickhouse", config.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	store := &ClickHouseMetricStore{
		db:                     db,
		database:               config.Database,
		metricsTable:           config.MetricsTable,
		workoutsTable:          config.WorkoutsTable,
		routesTable:            "workout_routes",
		heartRateDataTable:     "workout_heart_rate_data",
		heartRateRecoveryTable: "workout_heart_rate_recovery",
	}

	if config.CreateTables {
		if err := store.createTablesIfNotExist(); err != nil {
			return nil, fmt.Errorf("failed to create tables: %w", err)
		}
	}

	return store, nil
}

func (store *ClickHouseMetricStore) Name() string {
	return "clickhouse"
}

func (store *ClickHouseMetricStore) Store(metrics []request.Metric) error {
	if len(metrics) == 0 {
		return nil
	}

	ctx := context.Background()

	// Process all metrics and insert them one by one
	for _, metric := range metrics {
		metricType := request.LookupMetricType(metric.Name)
		for _, sample := range metric.Samples {
			// Handle nil timestamp
			var timestamp time.Time
			if ts := sample.GetTimestamp(); ts != nil {
				timestamp = ts.ToTime()
			} else {
				// Use current timestamp if timestamp is missing
				timestamp = time.Now()
			}

			// Default values
			var qty, max, min, avg, asleep, inBed float64
			var sleepSource, inBedSource string

			// Set values based on sample type
			switch s := sample.(type) {
			case *request.QtySample:
				qty = s.Qty
			case *request.MinMaxAvgSample:
				max = s.Max
				min = s.Min
				avg = s.Avg
			case *request.SleepSample:
				asleep = s.Asleep
				inBed = s.InBed
				sleepSource = s.SleepSource
				inBedSource = s.InBedSource
			}

			// Build the query for a single metric
			query := fmt.Sprintf(`
				INSERT INTO %s.%s 
				(timestamp, metric_name, metric_unit, metric_type, qty, max, min, avg, asleep, in_bed, sleep_source, in_bed_source) 
				VALUES 
				(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				SETTINGS async_insert=true
			`, store.database, store.metricsTable)

			// Execute the insert for a single metric
			_, err := store.db.ExecContext(ctx, query,
				timestamp,
				metric.Name,
				metric.Unit,
				metricType,
				qty,
				max,
				min,
				avg,
				asleep,
				inBed,
				sleepSource,
				inBedSource,
			)
			if err != nil {
				return fmt.Errorf("failed to insert metric: %w", err)
			}
		}
	}

	return nil
}

func (store *ClickHouseMetricStore) createTablesIfNotExist() error {
	// Set compatibility setting for AUTO_INCREMENT
	_, err := store.db.Exec(`SET compatibility_ignore_auto_increment_in_create_table = 1`)
	if err != nil {
		return fmt.Errorf("failed to set compatibility setting: %w", err)
	}

	// Create database if not exists
	_, err = store.db.Exec(fmt.Sprintf(`
		CREATE DATABASE IF NOT EXISTS %s
	`, store.database))
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	// Create metrics table if not exists
	_, err = store.db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			timestamp DateTime,
			metric_name String,
			metric_unit String,
			metric_type String,
			qty Float64 DEFAULT 0,
			max Float64 DEFAULT 0,
			min Float64 DEFAULT 0,
			avg Float64 DEFAULT 0,
			asleep Float64 DEFAULT 0,
			in_bed Float64 DEFAULT 0,
			sleep_source String DEFAULT '',
			in_bed_source String DEFAULT '',
			PRIMARY KEY (timestamp, metric_name)
		) ENGINE = MergeTree()
	`, store.database, store.metricsTable))
	if err != nil {
		return fmt.Errorf("failed to create metrics table: %w", err)
	}

	// Create workouts table if not exists
	_, err = store.db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			id UInt64 NOT NULL AUTO_INCREMENT,
			name String,
			start DateTime,
			end DateTime,
			total_energy_qty Float64 DEFAULT 0,
			total_energy_units String DEFAULT '',
			active_energy_qty Float64 DEFAULT 0,
			active_energy_units String DEFAULT '',
			avg_heart_rate_qty Float64 DEFAULT 0,
			avg_heart_rate_units String DEFAULT '',
			max_heart_rate_qty Float64 DEFAULT 0,
			max_heart_rate_units String DEFAULT '',
			distance_qty Float64 DEFAULT 0,
			distance_units String DEFAULT '',
			step_count_qty Float64 DEFAULT 0,
			step_count_units String DEFAULT '',
			step_cadence_qty Float64 DEFAULT 0,
			step_cadence_units String DEFAULT '',
			speed_qty Float64 DEFAULT 0,
			speed_units String DEFAULT '',
			swim_cadence_qty Float64 DEFAULT 0,
			swim_cadence_units String DEFAULT '',
			intensity_qty Float64 DEFAULT 0,
			intensity_units String DEFAULT '',
			humidity_qty Float64 DEFAULT 0,
			humidity_units String DEFAULT '',
			total_swimming_stroke_count_qty Float64 DEFAULT 0,
			total_swimming_stroke_count_units String DEFAULT '',
			flights_climbed_qty Float64 DEFAULT 0,
			flights_climbed_units String DEFAULT '',
			temperature_qty Float64 DEFAULT 0,
			temperature_units String DEFAULT '',
			elevation_ascent Float64 DEFAULT 0,
			elevation_descent Float64 DEFAULT 0,
			elevation_units String DEFAULT '',
			PRIMARY KEY (id)
		) ENGINE = MergeTree()
	`, store.database, store.workoutsTable))
	if err != nil {
		return fmt.Errorf("failed to create workouts table: %w", err)
	}

	// Create routes table if not exists
	_, err = store.db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			workout_name String,
			workout_start DateTime,
			timestamp DateTime,
			lat Float64,
			lon Float64,
			altitude Float64,
			PRIMARY KEY (workout_name, workout_start, timestamp)
		) ENGINE = MergeTree()
	`, store.database, store.routesTable))
	if err != nil {
		return fmt.Errorf("failed to create routes table: %w", err)
	}

	// Create heart rate data table if not exists
	_, err = store.db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			workout_name String,
			workout_start DateTime,
			timestamp DateTime,
			qty Float64,
			units String,
			PRIMARY KEY (workout_name, workout_start, timestamp)
		) ENGINE = MergeTree()
	`, store.database, store.heartRateDataTable))
	if err != nil {
		return fmt.Errorf("failed to create heart rate data table: %w", err)
	}

	// Create heart rate recovery table if not exists
	_, err = store.db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			workout_name String,
			workout_start DateTime,
			timestamp DateTime,
			qty Float64,
			units String,
			PRIMARY KEY (workout_name, workout_start, timestamp)
		) ENGINE = MergeTree()
	`, store.database, store.heartRateRecoveryTable))
	if err != nil {
		return fmt.Errorf("failed to create heart rate recovery table: %w", err)
	}

	return nil
}

func (store *ClickHouseMetricStore) StoreWorkouts(workouts []request.Workout) error {
	if len(workouts) == 0 {
		return nil
	}

	ctx := context.Background()

	// Process all workouts and insert them one by one
	for _, workout := range workouts {
		// Handle nil timestamps
		var startTime, endTime time.Time
		if workout.Start != nil {
			startTime = workout.Start.ToTime()
		} else {
			startTime = time.Now()
		}
		if workout.End != nil {
			endTime = workout.End.ToTime()
		} else {
			endTime = time.Now()
		}

		// Format timestamps
		start := startTime.Format("2006-01-02 15:04:05")
		end := endTime.Format("2006-01-02 15:04:05")

		// Insert workout
		workoutQuery := fmt.Sprintf(`
			INSERT INTO %s.%s 
			(name, start, end, total_energy_qty, total_energy_units, active_energy_qty, active_energy_units, 
			avg_heart_rate_qty, avg_heart_rate_units, max_heart_rate_qty, max_heart_rate_units, 
			distance_qty, distance_units, step_count_qty, step_count_units,
			step_cadence_qty, step_cadence_units, speed_qty, speed_units,
			swim_cadence_qty, swim_cadence_units, intensity_qty, intensity_units,
			humidity_qty, humidity_units, total_swimming_stroke_count_qty, total_swimming_stroke_count_units,
			flights_climbed_qty, flights_climbed_units, temperature_qty, temperature_units,
			elevation_ascent, elevation_descent, elevation_units) 
			VALUES 
			('%s', toDateTime('%s'), toDateTime('%s'), %f, '%s', %f, '%s', %f, '%s', %f, '%s', %f, '%s', %f, '%s', %f, '%s', %f, '%s', %f, '%s', %f, '%s', %f, '%s', %f, '%s', %f, '%s', %f, '%s', %f, %f, '%s')
			SETTINGS async_insert=true
		`, store.database, store.workoutsTable,
			workout.Name,
			start,
			end,
			float64(workout.TotalEnergy.Qty),
			workout.TotalEnergy.Units,
			float64(workout.ActiveEnergy.Qty),
			workout.ActiveEnergy.Units,
			float64(workout.AvgHeartRate.Qty),
			workout.AvgHeartRate.Units,
			float64(workout.MaxHeartRate.Qty),
			workout.MaxHeartRate.Units,
			float64(workout.Distance.Qty),
			workout.Distance.Units,
			float64(workout.StepCount.Qty),
			workout.StepCount.Units,
			float64(workout.StepCadence.Qty),
			workout.StepCadence.Units,
			float64(workout.Speed.Qty),
			workout.Speed.Units,
			float64(workout.SwimCadence.Qty),
			workout.SwimCadence.Units,
			float64(workout.Intensity.Qty),
			workout.Intensity.Units,
			float64(workout.Humidity.Qty),
			workout.Humidity.Units,
			float64(workout.TotalSwimmingStrokeCount.Qty),
			workout.TotalSwimmingStrokeCount.Units,
			float64(workout.FlightsClimbed.Qty),
			workout.FlightsClimbed.Units,
			float64(workout.Temperature.Qty),
			workout.Temperature.Units,
			float64(workout.Elevation.Ascent),
			float64(workout.Elevation.Descent),
			workout.Elevation.Units)

		// Execute the workout insert
		_, err := store.db.ExecContext(ctx, workoutQuery)
		if err != nil {
			return fmt.Errorf("failed to insert workout: %w", err)
		}

		// Process route data
		for _, routePoint := range workout.Route {
			var routeTimestamp time.Time
			if routePoint.Timestamp != nil {
				routeTimestamp = routePoint.Timestamp.ToTime()
			} else {
				// Use workout start time if timestamp is missing
				routeTimestamp = startTime
			}

			// Format timestamp
			timestamp := routeTimestamp.Format("2006-01-02 15:04:05")

			// Insert route point
			routeQuery := fmt.Sprintf(`
				INSERT INTO %s.%s
				(workout_name, workout_start, timestamp, lat, lon, altitude)
				VALUES 
				('%s', toDateTime('%s'), toDateTime('%s'), %f, %f, %f)
				SETTINGS async_insert=true
			`, store.database, store.routesTable,
				workout.Name,
				start,
				timestamp,
				float64(routePoint.Lat),
				float64(routePoint.Lon),
				float64(routePoint.Altitude))

			// Execute the route point insert
			_, err := store.db.ExecContext(ctx, routeQuery)
			if err != nil {
				return fmt.Errorf("failed to insert route point: %w", err)
			}
		}

		// Process heart rate data
		for _, heartRatePoint := range workout.HeartRateData {
			var heartRateTimestamp time.Time
			if heartRatePoint.Date != nil {
				heartRateTimestamp = heartRatePoint.Date.ToTime()
			} else {
				// Use workout start time if timestamp is missing
				heartRateTimestamp = startTime
			}

			// Format timestamp
			timestamp := heartRateTimestamp.Format("2006-01-02 15:04:05")

			// Insert heart rate data point
			heartRateQuery := fmt.Sprintf(`
				INSERT INTO %s.%s
				(workout_name, workout_start, timestamp, qty, units)
				VALUES 
				('%s', toDateTime('%s'), toDateTime('%s'), %f, '%s')
				SETTINGS async_insert=true
			`, store.database, store.heartRateDataTable,
				workout.Name,
				start,
				timestamp,
				float64(heartRatePoint.Qty),
				heartRatePoint.Units)

			// Execute the heart rate data point insert
			_, err := store.db.ExecContext(ctx, heartRateQuery)
			if err != nil {
				return fmt.Errorf("failed to insert heart rate data point: %w", err)
			}
		}

		// Process heart rate recovery data
		for _, heartRateRecoveryPoint := range workout.HeartRateRecovery {
			var heartRateRecoveryTimestamp time.Time
			if heartRateRecoveryPoint.Date != nil {
				heartRateRecoveryTimestamp = heartRateRecoveryPoint.Date.ToTime()
			} else {
				// Use workout start time if timestamp is missing
				heartRateRecoveryTimestamp = startTime
			}

			// Format timestamp
			timestamp := heartRateRecoveryTimestamp.Format("2006-01-02 15:04:05")

			// Insert heart rate recovery data point
			heartRateRecoveryQuery := fmt.Sprintf(`
				INSERT INTO %s.%s
				(workout_name, workout_start, timestamp, qty, units)
				VALUES 
				('%s', toDateTime('%s'), toDateTime('%s'), %f, '%s')
				SETTINGS async_insert=true
			`, store.database, store.heartRateRecoveryTable,
				workout.Name,
				start,
				timestamp,
				float64(heartRateRecoveryPoint.Qty),
				heartRateRecoveryPoint.Units)

			// Execute the heart rate recovery data point insert
			_, err := store.db.ExecContext(ctx, heartRateRecoveryQuery)
			if err != nil {
				return fmt.Errorf("failed to insert heart rate recovery data point: %w", err)
			}
		}
	}

	return nil
}

func (store *ClickHouseMetricStore) Close() error {
	return store.db.Close()
}
