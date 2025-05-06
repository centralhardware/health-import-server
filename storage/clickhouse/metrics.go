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
	DSN          string `json:"dsn"`
	Database     string `json:"database"`
	MetricsTable string `json:"metrics_table"`
	CreateTables bool   `json:"create_tables"`
}

type ClickHouseMetricStore struct {
	db           *sql.DB
	database     string
	metricsTable string
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
		db:           db,
		database:     config.Database,
		metricsTable: config.MetricsTable,
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

	return nil
}

func (store *ClickHouseMetricStore) Close() error {
	return store.db.Close()
}
