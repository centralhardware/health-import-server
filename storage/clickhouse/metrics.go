package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/joeecarter/health-import-server/request"
	"strings"
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
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Collect all metric data
	var metricValues []interface{}
	var metricPlaceholders []string

	// Process all metrics and collect data for batch insertion
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

			// Add metric data to the batch
			metricValues = append(metricValues,
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
			metricPlaceholders = append(metricPlaceholders, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		}
	}

	// Insert metrics in batch
	if len(metricPlaceholders) > 0 {
		metricQuery := fmt.Sprintf(`
			INSERT INTO %s.%s 
			(timestamp, metric_name, metric_unit, metric_type, qty, max, min, avg, asleep, in_bed, sleep_source, in_bed_source) 
			VALUES %s
		`, store.database, store.metricsTable, joinPlaceholders(metricPlaceholders))

		_, err = tx.ExecContext(ctx, metricQuery, metricValues...)
		if err != nil {
			return fmt.Errorf("failed to insert metrics batch: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
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
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Collect all workout data
	var workoutValues []interface{}
	var workoutPlaceholders []string

	// Collect all route data
	var routeValues []interface{}
	var routePlaceholders []string

	// Collect all heart rate data
	var heartRateDataValues []interface{}
	var heartRateDataPlaceholders []string

	// Collect all heart rate recovery data
	var heartRateRecoveryValues []interface{}
	var heartRateRecoveryPlaceholders []string

	// Process all workouts and collect data for batch insertion
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

		// Add workout data to the batch
		workoutValues = append(workoutValues,
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
		)
		workoutPlaceholders = append(workoutPlaceholders, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

		// Process route data
		for _, routePoint := range workout.Route {
			var routeTimestamp time.Time
			if routePoint.Timestamp != nil {
				routeTimestamp = routePoint.Timestamp.ToTime()
			} else {
				// Use workout start time if timestamp is missing
				routeTimestamp = startTime
			}

			// Add route data to the batch
			routeValues = append(routeValues,
				workout.Name,
				startTime,
				routeTimestamp,
				routePoint.Lat,
				routePoint.Lon,
				routePoint.Altitude,
			)
			routePlaceholders = append(routePlaceholders, "(?, ?, ?, ?, ?, ?)")
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

			// Add heart rate data to the batch
			heartRateDataValues = append(heartRateDataValues,
				workout.Name,
				startTime,
				heartRateTimestamp,
				heartRatePoint.Qty,
				heartRatePoint.Units,
			)
			heartRateDataPlaceholders = append(heartRateDataPlaceholders, "(?, ?, ?, ?, ?)")
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

			// Add heart rate recovery data to the batch
			heartRateRecoveryValues = append(heartRateRecoveryValues,
				workout.Name,
				startTime,
				heartRateRecoveryTimestamp,
				heartRateRecoveryPoint.Qty,
				heartRateRecoveryPoint.Units,
			)
			heartRateRecoveryPlaceholders = append(heartRateRecoveryPlaceholders, "(?, ?, ?, ?, ?)")
		}
	}

	// Insert workouts in batch
	if len(workoutPlaceholders) > 0 {
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
			VALUES %s
		`, store.database, store.workoutsTable, joinPlaceholders(workoutPlaceholders))

		_, err = tx.ExecContext(ctx, workoutQuery, workoutValues...)
		if err != nil {
			return fmt.Errorf("failed to insert workouts batch: %w", err)
		}
	}

	// Insert route data in batch
	if len(routePlaceholders) > 0 {
		routeQuery := fmt.Sprintf(`
			INSERT INTO %s.%s
			(workout_name, workout_start, timestamp, lat, lon, altitude)
			VALUES %s
		`, store.database, store.routesTable, joinPlaceholders(routePlaceholders))

		_, err = tx.ExecContext(ctx, routeQuery, routeValues...)
		if err != nil {
			return fmt.Errorf("failed to insert route points batch: %w", err)
		}
	}

	// Insert heart rate data in batch
	if len(heartRateDataPlaceholders) > 0 {
		heartRateDataQuery := fmt.Sprintf(`
			INSERT INTO %s.%s
			(workout_name, workout_start, timestamp, qty, units)
			VALUES %s
		`, store.database, store.heartRateDataTable, joinPlaceholders(heartRateDataPlaceholders))

		_, err = tx.ExecContext(ctx, heartRateDataQuery, heartRateDataValues...)
		if err != nil {
			return fmt.Errorf("failed to insert heart rate data points batch: %w", err)
		}
	}

	// Insert heart rate recovery data in batch
	if len(heartRateRecoveryPlaceholders) > 0 {
		heartRateRecoveryQuery := fmt.Sprintf(`
			INSERT INTO %s.%s
			(workout_name, workout_start, timestamp, qty, units)
			VALUES %s
		`, store.database, store.heartRateRecoveryTable, joinPlaceholders(heartRateRecoveryPlaceholders))

		_, err = tx.ExecContext(ctx, heartRateRecoveryQuery, heartRateRecoveryValues...)
		if err != nil {
			return fmt.Errorf("failed to insert heart rate recovery data points batch: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// joinPlaceholders joins the placeholders with commas for use in a multi-value INSERT statement
func joinPlaceholders(placeholders []string) string {
	return strings.Join(placeholders, ", ")
}

func (store *ClickHouseMetricStore) Close() error {
	return store.db.Close()
}
