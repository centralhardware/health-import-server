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
	db            *sql.DB
	database      string
	metricsTable  string
	workoutsTable string
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
		db:            db,
		database:      config.Database,
		metricsTable:  config.MetricsTable,
		workoutsTable: config.WorkoutsTable,
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
	ctx := context.Background()
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Prepare statement for inserting metrics
	stmt, err := tx.PrepareContext(ctx, fmt.Sprintf(`
		INSERT INTO %s.%s 
		(timestamp, metric_name, metric_unit, metric_type, qty, max, min, avg, asleep, in_bed, sleep_source, in_bed_source) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, store.database, store.metricsTable))
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	// Insert metrics
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

			_, err = stmt.ExecContext(ctx,
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

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (store *ClickHouseMetricStore) createTablesIfNotExist() error {
	// Create database if not exists
	_, err := store.db.Exec(fmt.Sprintf(`
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
		CREATE TABLE IF NOT EXISTS %s.workout_routes (
			workout_id UInt64,
			timestamp DateTime,
			lat Float64,
			lon Float64,
			altitude Float64,
			PRIMARY KEY (workout_id, timestamp)
		) ENGINE = MergeTree()
	`, store.database))
	if err != nil {
		return fmt.Errorf("failed to create routes table: %w", err)
	}

	// Create heart rate data table if not exists
	_, err = store.db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.workout_heart_rate_data (
			workout_id UInt64,
			timestamp DateTime,
			qty Float64,
			units String,
			PRIMARY KEY (workout_id, timestamp)
		) ENGINE = MergeTree()
	`, store.database))
	if err != nil {
		return fmt.Errorf("failed to create heart rate data table: %w", err)
	}

	// Create heart rate recovery table if not exists
	_, err = store.db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.workout_heart_rate_recovery (
			workout_id UInt64,
			timestamp DateTime,
			qty Float64,
			units String,
			PRIMARY KEY (workout_id, timestamp)
		) ENGINE = MergeTree()
	`, store.database))
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
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Prepare statement for inserting workouts
	stmt, err := tx.PrepareContext(ctx, fmt.Sprintf(`
		INSERT INTO %s.%s 
		(name, start, end, total_energy_qty, total_energy_units, active_energy_qty, active_energy_units, 
		avg_heart_rate_qty, avg_heart_rate_units, max_heart_rate_qty, max_heart_rate_units, 
		distance_qty, distance_units, step_count_qty, step_count_units,
		step_cadence_qty, step_cadence_units, speed_qty, speed_units,
		swim_cadence_qty, swim_cadence_units, intensity_qty, intensity_units,
		humidity_qty, humidity_units, total_swimming_stroke_count_qty, total_swimming_stroke_count_units,
		flights_climbed_qty, flights_climbed_units, temperature_qty, temperature_units,
		elevation_ascent, elevation_descent, elevation_units) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		RETURNING id
	`, store.database, store.workoutsTable))
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	// Prepare statement for inserting route data
	routeStmt, err := tx.PrepareContext(ctx, fmt.Sprintf(`
		INSERT INTO %s.workout_routes
		(workout_id, timestamp, lat, lon, altitude)
		VALUES (?, ?, ?, ?, ?)
	`, store.database))
	if err != nil {
		return fmt.Errorf("failed to prepare route statement: %w", err)
	}
	defer routeStmt.Close()

	// Prepare statement for inserting heart rate data
	heartRateDataStmt, err := tx.PrepareContext(ctx, fmt.Sprintf(`
		INSERT INTO %s.workout_heart_rate_data
		(workout_id, timestamp, qty, units)
		VALUES (?, ?, ?, ?)
	`, store.database))
	if err != nil {
		return fmt.Errorf("failed to prepare heart rate data statement: %w", err)
	}
	defer heartRateDataStmt.Close()

	// Prepare statement for inserting heart rate recovery data
	heartRateRecoveryStmt, err := tx.PrepareContext(ctx, fmt.Sprintf(`
		INSERT INTO %s.workout_heart_rate_recovery
		(workout_id, timestamp, qty, units)
		VALUES (?, ?, ?, ?)
	`, store.database))
	if err != nil {
		return fmt.Errorf("failed to prepare heart rate recovery statement: %w", err)
	}
	defer heartRateRecoveryStmt.Close()

	// Insert workouts
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

		// No need to convert heart rate data to JSON anymore as we'll store it in separate tables

		// Insert workout data and get the ID
		var workoutID string
		err = stmt.QueryRowContext(ctx,
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
			workout.StepCount.Qty,
			workout.StepCount.Units,
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
			workout.Elevation.Units,
		).Scan(&workoutID)
		if err != nil {
			return fmt.Errorf("failed to insert workout: %w", err)
		}

		// Insert route data
		for _, routePoint := range workout.Route {
			var routeTimestamp time.Time
			if routePoint.Timestamp != nil {
				routeTimestamp = routePoint.Timestamp.ToTime()
			} else {
				// Use workout start time if timestamp is missing
				routeTimestamp = startTime
			}

			_, err = routeStmt.ExecContext(ctx,
				workoutID,
				routeTimestamp,
				routePoint.Lat,
				routePoint.Lon,
				routePoint.Altitude,
			)
			if err != nil {
				return fmt.Errorf("failed to insert route point: %w", err)
			}
		}

		// Insert heart rate data
		for _, heartRatePoint := range workout.HeartRateData {
			var heartRateTimestamp time.Time
			if heartRatePoint.Date != nil {
				heartRateTimestamp = heartRatePoint.Date.ToTime()
			} else {
				// Use workout start time if timestamp is missing
				heartRateTimestamp = startTime
			}

			_, err = heartRateDataStmt.ExecContext(ctx,
				workoutID,
				heartRateTimestamp,
				heartRatePoint.Qty,
				heartRatePoint.Units,
			)
			if err != nil {
				return fmt.Errorf("failed to insert heart rate data point: %w", err)
			}
		}

		// Insert heart rate recovery data
		for _, heartRateRecoveryPoint := range workout.HeartRateRecovery {
			var heartRateRecoveryTimestamp time.Time
			if heartRateRecoveryPoint.Date != nil {
				heartRateRecoveryTimestamp = heartRateRecoveryPoint.Date.ToTime()
			} else {
				// Use workout start time if timestamp is missing
				heartRateRecoveryTimestamp = startTime
			}

			_, err = heartRateRecoveryStmt.ExecContext(ctx,
				workoutID,
				heartRateRecoveryTimestamp,
				heartRateRecoveryPoint.Qty,
				heartRateRecoveryPoint.Units,
			)
			if err != nil {
				return fmt.Errorf("failed to insert heart rate recovery data point: %w", err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (store *ClickHouseMetricStore) Close() error {
	return store.db.Close()
}
