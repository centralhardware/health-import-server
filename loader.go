package server

import (
	"fmt"
	"os"
	"strings"

	"github.com/joeecarter/health-import-server/storage/clickhouse"
)

const CLICKHOUSE_DSN = "CLICKHOUSE_DSN"
const CLICKHOUSE_DATABASE = "CLICKHOUSE_DATABASE"

func LoadMetricStores() ([]MetricStore, error) {
	return LoadMetricStoresFromEnvironment()
}

func LoadMetricStoresFromEnvironment() ([]MetricStore, error) {
	clickhouseStore, err := loadClickHouseMetricStoreFromEnvironment()
	if err != nil {
		return nil, err
	}

	var metricStores []MetricStore
	if clickhouseStore != nil {
		metricStores = append(metricStores, clickhouseStore)
	}

	return metricStores, nil
}

func loadClickHouseMetricStoreFromEnvironment() (MetricStore, error) {
	dsn, dsnSet := os.LookupEnv(CLICKHOUSE_DSN)
	database, databaseSet := os.LookupEnv(CLICKHOUSE_DATABASE)

	if !dsnSet && !databaseSet {
		return nil, nil
	}

	missingVariables := make([]string, 0)
	if !dsnSet {
		missingVariables = append(missingVariables, CLICKHOUSE_DSN)
	}
	if !databaseSet {
		missingVariables = append(missingVariables, CLICKHOUSE_DATABASE)
	}

	if len(missingVariables) > 0 {
		return nil, missingEnvironmentError{missingVariables}
	}

	config := clickhouse.ClickHouseConfig{
		DSN:      dsn,
		Database: database,
	}

	store, err := clickhouse.NewClickHouseMetricStore(config)
	if err != nil {
		return nil, err
	}
	return store, nil
}

type missingEnvironmentError struct {
	missingVariables []string
}

func (err missingEnvironmentError) Error() string {
	return fmt.Sprintf("Missing the following environment variables: [ %s ]", strings.Join(err.missingVariables, ", "))
}
