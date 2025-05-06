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

	// Process all metrics and collect data for insertion
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
		}
	}

	// Insert metrics in batch
	if len(metricValues) > 0 {
		// Prepare the batch insert query
		batchSize := 1000 // Adjust based on your needs
		numMetrics := len(metricValues) / 12

		for batchStart := 0; batchStart < numMetrics; batchStart += batchSize {
			batchEnd := batchStart + batchSize
			if batchEnd > numMetrics {
				batchEnd = numMetrics
			}

			// Build the query with multiple value sets
			var query strings.Builder
			query.WriteString(fmt.Sprintf(`
				INSERT INTO %s.%s 
				(timestamp, metric_name, metric_unit, metric_type, qty, max, min, avg, asleep, in_bed, sleep_source, in_bed_source) 
				VALUES 
			`, store.database, store.metricsTable))

			// Add placeholders for each row in the batch
			for i := batchStart; i < batchEnd; i++ {
				if i > batchStart {
					query.WriteString(", ")
				}
				query.WriteString("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
			}

			// Extract the values for this batch
			batchValues := make([]interface{}, 0, (batchEnd-batchStart)*12)
			for i := batchStart; i < batchEnd; i++ {
				startIdx := i * 12
				for j := 0; j < 12; j++ {
					batchValues = append(batchValues, metricValues[startIdx+j])
				}
			}

			// Execute the batch insert
			_, err = tx.ExecContext(ctx, query.String(), batchValues...)
			if err != nil {
				return fmt.Errorf("failed to insert metrics batch: %w", err)
			}
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

	// Collect all route data
	var routeValues []interface{}

	// Collect all heart rate data
	var heartRateDataValues []interface{}

	// Collect all heart rate recovery data
	var heartRateRecoveryValues []interface{}

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
		// Ensure all float64 values are properly formatted for ClickHouse
		// Use a temporary variable for step count to ensure proper Float64 formatting
		stepCountQty := float64(workout.StepCount.Qty)

		workoutValues = append(workoutValues,
			workout.Name,
			startTime,
			endTime,
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
			stepCountQty,
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
			workout.Elevation.Units,
		)

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
			// Ensure all float64 values are properly formatted for ClickHouse
			routeValues = append(routeValues,
				workout.Name,
				startTime,
				routeTimestamp,
				float64(routePoint.Lat),
				float64(routePoint.Lon),
				float64(routePoint.Altitude),
			)
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
			// Ensure all float64 values are properly formatted for ClickHouse
			heartRateDataValues = append(heartRateDataValues,
				workout.Name,
				startTime,
				heartRateTimestamp,
				float64(heartRatePoint.Qty),
				heartRatePoint.Units,
			)
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
			// Ensure all float64 values are properly formatted for ClickHouse
			heartRateRecoveryValues = append(heartRateRecoveryValues,
				workout.Name,
				startTime,
				heartRateRecoveryTimestamp,
				float64(heartRateRecoveryPoint.Qty),
				heartRateRecoveryPoint.Units,
			)
		}
	}

	// Insert workouts in batch
	if len(workoutValues) > 0 {
		// Prepare the batch insert query
		batchSize := 100 // Adjust based on your needs
		numWorkouts := len(workoutValues) / 33

		for batchStart := 0; batchStart < numWorkouts; batchStart += batchSize {
			batchEnd := batchStart + batchSize
			if batchEnd > numWorkouts {
				batchEnd = numWorkouts
			}

			// Build the query with multiple value sets
			var query strings.Builder
			query.WriteString(fmt.Sprintf(`
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
			`, store.database, store.workoutsTable))

			// Add placeholders for each row in the batch
			for i := batchStart; i < batchEnd; i++ {
				if i > batchStart {
					query.WriteString(", ")
				}
				query.WriteString("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
			}

			// Extract the values for this batch
			batchValues := make([]interface{}, 0, (batchEnd-batchStart)*33)
			for i := batchStart; i < batchEnd; i++ {
				startIdx := i * 33
				for j := 0; j < 33; j++ {
					batchValues = append(batchValues, workoutValues[startIdx+j])
				}
			}

			// Execute the batch insert
			_, err = tx.ExecContext(ctx, query.String(), batchValues...)
			if err != nil {
				return fmt.Errorf("failed to insert workouts batch: %w", err)
			}
		}
	}

	// Insert route data in batch
	if len(routeValues) > 0 {
		// Prepare the batch insert query
		batchSize := 1000 // Adjust based on your needs
		numRoutePoints := len(routeValues) / 6

		for batchStart := 0; batchStart < numRoutePoints; batchStart += batchSize {
			batchEnd := batchStart + batchSize
			if batchEnd > numRoutePoints {
				batchEnd = numRoutePoints
			}

			// Build the query with multiple value sets
			var query strings.Builder
			query.WriteString(fmt.Sprintf(`
				INSERT INTO %s.%s
				(workout_name, workout_start, timestamp, lat, lon, altitude)
				VALUES 
			`, store.database, store.routesTable))

			// Add placeholders for each row in the batch
			for i := batchStart; i < batchEnd; i++ {
				if i > batchStart {
					query.WriteString(", ")
				}
				query.WriteString("(?, ?, ?, ?, ?, ?)")
			}

			// Extract the values for this batch
			batchValues := make([]interface{}, 0, (batchEnd-batchStart)*6)
			for i := batchStart; i < batchEnd; i++ {
				startIdx := i * 6
				for j := 0; j < 6; j++ {
					batchValues = append(batchValues, routeValues[startIdx+j])
				}
			}

			// Execute the batch insert
			_, err = tx.ExecContext(ctx, query.String(), batchValues...)
			if err != nil {
				return fmt.Errorf("failed to insert route points batch: %w", err)
			}
		}
	}

	// Insert heart rate data in batch
	if len(heartRateDataValues) > 0 {
		// Prepare the batch insert query
		batchSize := 1000 // Adjust based on your needs
		numHeartRatePoints := len(heartRateDataValues) / 5

		for batchStart := 0; batchStart < numHeartRatePoints; batchStart += batchSize {
			batchEnd := batchStart + batchSize
			if batchEnd > numHeartRatePoints {
				batchEnd = numHeartRatePoints
			}

			// Build the query with multiple value sets
			var query strings.Builder
			query.WriteString(fmt.Sprintf(`
				INSERT INTO %s.%s
				(workout_name, workout_start, timestamp, qty, units)
				VALUES 
			`, store.database, store.heartRateDataTable))

			// Add placeholders for each row in the batch
			for i := batchStart; i < batchEnd; i++ {
				if i > batchStart {
					query.WriteString(", ")
				}
				query.WriteString("(?, ?, ?, ?, ?)")
			}

			// Extract the values for this batch
			batchValues := make([]interface{}, 0, (batchEnd-batchStart)*5)
			for i := batchStart; i < batchEnd; i++ {
				startIdx := i * 5
				for j := 0; j < 5; j++ {
					batchValues = append(batchValues, heartRateDataValues[startIdx+j])
				}
			}

			// Execute the batch insert
			_, err = tx.ExecContext(ctx, query.String(), batchValues...)
			if err != nil {
				return fmt.Errorf("failed to insert heart rate data points batch: %w", err)
			}
		}
	}

	// Insert heart rate recovery data in batch
	if len(heartRateRecoveryValues) > 0 {
		// Prepare the batch insert query
		batchSize := 1000 // Adjust based on your needs
		numHeartRateRecoveryPoints := len(heartRateRecoveryValues) / 5

		for batchStart := 0; batchStart < numHeartRateRecoveryPoints; batchStart += batchSize {
			batchEnd := batchStart + batchSize
			if batchEnd > numHeartRateRecoveryPoints {
				batchEnd = numHeartRateRecoveryPoints
			}

			// Build the query with multiple value sets
			var query strings.Builder
			query.WriteString(fmt.Sprintf(`
				INSERT INTO %s.%s
				(workout_name, workout_start, timestamp, qty, units)
				VALUES 
			`, store.database, store.heartRateRecoveryTable))

			// Add placeholders for each row in the batch
			for i := batchStart; i < batchEnd; i++ {
				if i > batchStart {
					query.WriteString(", ")
				}
				query.WriteString("(?, ?, ?, ?, ?)")
			}

			// Extract the values for this batch
			batchValues := make([]interface{}, 0, (batchEnd-batchStart)*5)
			for i := batchStart; i < batchEnd; i++ {
				startIdx := i * 5
				for j := 0; j < 5; j++ {
					batchValues = append(batchValues, heartRateRecoveryValues[startIdx+j])
				}
			}

			// Execute the batch insert
			_, err = tx.ExecContext(ctx, query.String(), batchValues...)
			if err != nil {
				return fmt.Errorf("failed to insert heart rate recovery data points batch: %w", err)
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
