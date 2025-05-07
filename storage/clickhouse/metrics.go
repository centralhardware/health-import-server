package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/joeecarter/health-import-server/request"
	"log"

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
	db                             *sql.DB
	database                       string
	metricsTable                   string
	workoutsTable                  string
	routesTable                    string
	heartRateDataTable             string
	heartRateRecoveryTable         string
	stepCountLogTable              string
	walkingAndRunningDistanceTable string
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
		db:                             db,
		database:                       config.Database,
		metricsTable:                   config.MetricsTable,
		workoutsTable:                  config.WorkoutsTable,
		routesTable:                    "workout_routes",
		heartRateDataTable:             "workout_heart_rate_data",
		heartRateRecoveryTable:         "workout_heart_rate_recovery",
		stepCountLogTable:              "workout_step_count_log",
		walkingAndRunningDistanceTable: "workout_walking_running_distance",
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

	log.Printf("Inserting %d metrics into ClickHouse", len(metrics))
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
				SETTINGS async_insert=1, wait_for_async_insert=0
				VALUES 
				(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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

			log.Printf("Inserted metric: %s (%s) at %s", metric.Name, metric.Unit, timestamp.Format(time.RFC3339))
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
			metric_name LowCardinality(String),
			metric_unit LowCardinality(String),
			metric_type LowCardinality(String),
			qty Float64 DEFAULT 0,
			max Float64 DEFAULT 0,
			min Float64 DEFAULT 0,
			avg Float64 DEFAULT 0,
			asleep Float64 DEFAULT 0,
			in_bed Float64 DEFAULT 0,
			sleep_source LowCardinality(String) DEFAULT '',
			in_bed_source LowCardinality(String) DEFAULT '',
			PRIMARY KEY (timestamp, metric_name)
  ) ENGINE = ReplacingMergeTree(timestamp)
	`, store.database, store.metricsTable))
	if err != nil {
		return fmt.Errorf("failed to create metrics table: %w", err)
	}

	// Create workouts table if not exists
	_, err = store.db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			id UInt64 NOT NULL AUTO_INCREMENT,
			name LowCardinality(String),
			start DateTime,
			end DateTime,
			total_energy_qty Float64 DEFAULT 0,
			total_energy_units LowCardinality(String) DEFAULT '',
			active_energy_qty Float64 DEFAULT 0,
			active_energy_units LowCardinality(String) DEFAULT '',
			avg_heart_rate_qty Float64 DEFAULT 0,
			avg_heart_rate_units LowCardinality(String) DEFAULT '',
			max_heart_rate_qty Float64 DEFAULT 0,
			max_heart_rate_units LowCardinality(String) DEFAULT '',
			distance_qty Float64 DEFAULT 0,
			distance_units LowCardinality(String) DEFAULT '',
			step_count_qty Float64 DEFAULT 0,
			step_count_units LowCardinality(String) DEFAULT '',
			step_cadence_qty Float64 DEFAULT 0,
			step_cadence_units LowCardinality(String) DEFAULT '',
			speed_qty Float64 DEFAULT 0,
			speed_units LowCardinality(String) DEFAULT '',
			swim_cadence_qty Float64 DEFAULT 0,
			swim_cadence_units LowCardinality(String) DEFAULT '',
			intensity_qty Float64 DEFAULT 0,
			intensity_units LowCardinality(String) DEFAULT '',
			humidity_qty Float64 DEFAULT 0,
			humidity_units LowCardinality(String) DEFAULT '',
			total_swimming_stroke_count_qty Float64 DEFAULT 0,
			total_swimming_stroke_count_units LowCardinality(String) DEFAULT '',
			flights_climbed_qty Float64 DEFAULT 0,
			flights_climbed_units LowCardinality(String) DEFAULT '',
			temperature_qty Float64 DEFAULT 0,
			temperature_units LowCardinality(String) DEFAULT '',
			elevation_ascent Float64 DEFAULT 0,
			elevation_descent Float64 DEFAULT 0,
			elevation_units LowCardinality(String) DEFAULT '',
			PRIMARY KEY (start, end)
  ) ENGINE = ReplacingMergeTree(start, end)
	`, store.database, store.workoutsTable))
	if err != nil {
		return fmt.Errorf("failed to create workouts table: %w", err)
	}

	// Create routes table if not exists
	_, err = store.db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			workout_name LowCardinality(String),
			workout_start DateTime,
			timestamp DateTime,
			lat Float64,
			lon Float64,
			altitude Float64,
			course Float64 DEFAULT 0,
			vertical_accuracy Float64 DEFAULT 0,
			horizontal_accuracy Float64 DEFAULT 0,
			course_accuracy Float64 DEFAULT 0,
			speed Float64 DEFAULT 0,
			speed_accuracy Float64 DEFAULT 0,
			PRIMARY KEY (workout_name, workout_start, timestamp)
  ) ENGINE = ReplacingMergeTree(workout_start, timestamp)
	`, store.database, store.routesTable))
	if err != nil {
		return fmt.Errorf("failed to create routes table: %w", err)
	}

	// Create heart rate data table if not exists
	_, err = store.db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			workout_name LowCardinality(String),
			workout_start DateTime,
			timestamp DateTime,
			qty Float64,
			min Float64,
			max Float64,
			avg Float64,
			units LowCardinality(String),
			source LowCardinality(String),
			PRIMARY KEY (workout_name, workout_start, timestamp)
  ) ENGINE = ReplacingMergeTree(workout_start, timestamp)
	`, store.database, store.heartRateDataTable))
	if err != nil {
		return fmt.Errorf("failed to create heart rate data table: %w", err)
	}

	// Create heart rate recovery table if not exists
	_, err = store.db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			workout_name LowCardinality(String),
			workout_start DateTime,
			timestamp DateTime,
			qty Float64,
			min Float64,
			max Float64,
			avg Float64,
			units LowCardinality(String),
			source LowCardinality(String),
			PRIMARY KEY (workout_name, workout_start, timestamp)
  ) ENGINE = ReplacingMergeTree(workout_start, timestamp)
	`, store.database, store.heartRateRecoveryTable))
	if err != nil {
		return fmt.Errorf("failed to create heart rate recovery table: %w", err)
	}

	// Create step count log table if not exists
	_, err = store.db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			workout_name LowCardinality(String),
			workout_start DateTime,
			timestamp DateTime,
			qty Float64,
			units LowCardinality(String),
			source LowCardinality(String),
			PRIMARY KEY (workout_name, workout_start, timestamp)
  ) ENGINE = ReplacingMergeTree(workout_start, timestamp)
	`, store.database, store.stepCountLogTable))
	if err != nil {
		return fmt.Errorf("failed to create step count log table: %w", err)
	}

	// Create walking and running distance table if not exists
	_, err = store.db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			workout_name LowCardinality(String),
			workout_start DateTime,
			timestamp DateTime,
			qty Float64,
			units LowCardinality(String),
			source LowCardinality(String),
			PRIMARY KEY (workout_name, workout_start, timestamp)
  ) ENGINE = ReplacingMergeTree(workout_start, timestamp)
	`, store.database, store.walkingAndRunningDistanceTable))
	if err != nil {
		return fmt.Errorf("failed to create walking and running distance table: %w", err)
	}

	return nil
}

func (store *ClickHouseMetricStore) StoreWorkouts(workouts []request.Workout) error {
	if len(workouts) == 0 {
		return nil
	}

	log.Printf("Inserting %d workouts into ClickHouse", len(workouts))
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

		// Insert workout using parameterized query
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
			SETTINGS async_insert=1, wait_for_async_insert=0
			VALUES 
			(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, store.database, store.workoutsTable)

		// Execute the workout insert with parameters
		_, err := store.db.ExecContext(ctx, workoutQuery,
			workout.Name,
			startTime,
			endTime,
			workout.TotalEnergy.Qty,
			workout.TotalEnergy.Units,
			workout.ActiveEnergy.Qty,
			workout.ActiveEnergy.Units,
			workout.AvgHeartRate.Qty,
			workout.AvgHeartRate.Units,
			workout.MaxHeartRate.Qty,
			workout.MaxHeartRate.Units,
			workout.Distance.Qty,
			workout.Distance.Units,
			func() float64 {
				if len(workout.StepCount) > 0 {
					return workout.StepCount[0].Qty
				}
				return 0
			}(),
			func() string {
				if len(workout.StepCount) > 0 {
					return workout.StepCount[0].Units
				}
				return ""
			}(),
			workout.StepCadence.Qty,
			workout.StepCadence.Units,
			workout.Speed.Qty,
			workout.Speed.Units,
			workout.SwimCadence.Qty,
			workout.SwimCadence.Units,
			workout.Intensity.Qty,
			workout.Intensity.Units,
			workout.Humidity.Qty,
			workout.Humidity.Units,
			workout.TotalSwimmingStrokeCount.Qty,
			workout.TotalSwimmingStrokeCount.Units,
			workout.FlightsClimbed.Qty,
			workout.FlightsClimbed.Units,
			workout.Temperature.Qty,
			workout.Temperature.Units,
			workout.Elevation.Ascent,
			workout.Elevation.Descent,
			workout.Elevation.Units)
		if err != nil {
			return fmt.Errorf("failed to insert workout: %w", err)
		}

		log.Printf("Inserted workout: %s (start: %s, end: %s)", workout.Name, startTime.Format(time.RFC3339), endTime.Format(time.RFC3339))

		// Process route data
		for _, routePoint := range workout.Route {
			var routeTimestamp time.Time
			if routePoint.Timestamp != nil {
				routeTimestamp = routePoint.Timestamp.ToTime()
			} else {
				// Use workout start time if timestamp is missing
				routeTimestamp = startTime
			}

			// Insert route point using parameterized query
			routeQuery := fmt.Sprintf(`
				INSERT INTO %s.%s
				(workout_name, workout_start, timestamp, lat, lon, altitude, course, vertical_accuracy, horizontal_accuracy, course_accuracy, speed, speed_accuracy)
				SETTINGS async_insert=1, wait_for_async_insert=0
				VALUES 
				(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			`, store.database, store.routesTable)

			// Execute the route point insert with parameters
			_, err := store.db.ExecContext(ctx, routeQuery,
				workout.Name,
				startTime,
				routeTimestamp,
				routePoint.Lat,
				routePoint.Lon,
				routePoint.Altitude,
				routePoint.Course,
				routePoint.VerticalAccuracy,
				routePoint.HorizontalAccuracy,
				routePoint.CourseAccuracy,
				routePoint.Speed,
				routePoint.SpeedAccuracy)
			if err != nil {
				return fmt.Errorf("failed to insert route point: %w", err)
			}
			log.Printf("Inserted route point for workout '%s': lat=%f, lon=%f at %s",
				workout.Name, routePoint.Lat, routePoint.Lon, routeTimestamp.Format(time.RFC3339))
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

			// Insert heart rate data point using parameterized query
			heartRateQuery := fmt.Sprintf(`
				INSERT INTO %s.%s
				(workout_name, workout_start, timestamp, qty, min, max, avg, units, source)
				SETTINGS async_insert=1, wait_for_async_insert=0
				VALUES 
				(?, ?, ?, ?, ?, ?, ?, ?, ?)
			`, store.database, store.heartRateDataTable)

			// Execute the heart rate data point insert with parameters
			_, err := store.db.ExecContext(ctx, heartRateQuery,
				workout.Name,
				startTime,
				heartRateTimestamp,
				heartRatePoint.Qty,
				heartRatePoint.Min,
				heartRatePoint.Max,
				heartRatePoint.Avg,
				heartRatePoint.Units,
				heartRatePoint.Source)
			if err != nil {
				return fmt.Errorf("failed to insert heart rate data point: %w", err)
			}
			log.Printf("Inserted heart rate data for workout '%s': min=%v, max=%v, avg=%v %s at %s",
				workout.Name, heartRatePoint.Min, heartRatePoint.Max, heartRatePoint.Avg,
				heartRatePoint.Units, heartRateTimestamp.Format(time.RFC3339))
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

			// Insert heart rate recovery data point using parameterized query
			heartRateRecoveryQuery := fmt.Sprintf(`
				INSERT INTO %s.%s
				(workout_name, workout_start, timestamp, qty, min, max, avg, units, source)
				SETTINGS async_insert=1, wait_for_async_insert=0
				VALUES 
				(?, ?, ?, ?, ?, ?, ?, ?, ?)
			`, store.database, store.heartRateRecoveryTable)

			// Execute the heart rate recovery data point insert with parameters
			_, err := store.db.ExecContext(ctx, heartRateRecoveryQuery,
				workout.Name,
				startTime,
				heartRateRecoveryTimestamp,
				heartRateRecoveryPoint.Qty,
				heartRateRecoveryPoint.Min,
				heartRateRecoveryPoint.Max,
				heartRateRecoveryPoint.Avg,
				heartRateRecoveryPoint.Units,
				heartRateRecoveryPoint.Source)
			if err != nil {
				return fmt.Errorf("failed to insert heart rate recovery data point: %w", err)
			}
			log.Printf("Inserted heart rate recovery data for workout '%s': min=%v, max=%v, avg=%v %s at %s",
				workout.Name, heartRateRecoveryPoint.Min, heartRateRecoveryPoint.Max, heartRateRecoveryPoint.Avg,
				heartRateRecoveryPoint.Units, heartRateRecoveryTimestamp.Format(time.RFC3339))
		}

		// Process step count log data
		for _, stepCountPoint := range workout.StepCount {
			var stepCountTimestamp time.Time
			if stepCountPoint.Date != nil {
				stepCountTimestamp = stepCountPoint.Date.ToTime()
			} else {
				// Use workout start time if timestamp is missing
				stepCountTimestamp = startTime
			}

			// Insert step count log data point using parameterized query
			stepCountQuery := fmt.Sprintf(`
				INSERT INTO %s.%s
				(workout_name, workout_start, timestamp, qty, units, source)
				SETTINGS async_insert=1, wait_for_async_insert=0
				VALUES 
				(?, ?, ?, ?, ?, ?)
			`, store.database, store.stepCountLogTable)

			// Execute the step count log data point insert with parameters
			_, err := store.db.ExecContext(ctx, stepCountQuery,
				workout.Name,
				startTime,
				stepCountTimestamp,
				stepCountPoint.Qty,
				stepCountPoint.Units,
				stepCountPoint.Source)
			if err != nil {
				return fmt.Errorf("failed to insert step count log data point: %w", err)
			}
			log.Printf("Inserted step count data for workout '%s': %v %s at %s",
				workout.Name, stepCountPoint.Qty, stepCountPoint.Units, stepCountTimestamp.Format(time.RFC3339))
		}

		// Process walking and running distance data
		for _, walkingRunningPoint := range workout.WalkingAndRunningDistance {
			var walkingRunningTimestamp time.Time
			if walkingRunningPoint.Date != nil {
				walkingRunningTimestamp = walkingRunningPoint.Date.ToTime()
			} else {
				// Use workout start time if timestamp is missing
				walkingRunningTimestamp = startTime
			}

			// Insert walking and running distance data point using parameterized query
			walkingRunningQuery := fmt.Sprintf(`
				INSERT INTO %s.%s
				(workout_name, workout_start, timestamp, qty, units, source)
				SETTINGS async_insert=1, wait_for_async_insert=0
				VALUES 
				(?, ?, ?, ?, ?, ?)
			`, store.database, store.walkingAndRunningDistanceTable)

			// Execute the walking and running distance data point insert with parameters
			_, err := store.db.ExecContext(ctx, walkingRunningQuery,
				workout.Name,
				startTime,
				walkingRunningTimestamp,
				walkingRunningPoint.Qty,
				walkingRunningPoint.Units,
				walkingRunningPoint.Source)
			if err != nil {
				return fmt.Errorf("failed to insert walking and running distance data point: %w", err)
			}
			log.Printf("Inserted walking/running distance data for workout '%s': %v %s at %s",
				workout.Name, walkingRunningPoint.Qty, walkingRunningPoint.Units, walkingRunningTimestamp.Format(time.RFC3339))
		}
	}

	return nil
}

func (store *ClickHouseMetricStore) Close() error {
	return store.db.Close()
}
