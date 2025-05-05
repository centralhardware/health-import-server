package server

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/joeecarter/health-import-server/storage/clickhouse"
)

const CLICKHOUSE_DSN = "CLICKHOUSE_DSN"
const CLICKHOUSE_DATABASE = "CLICKHOUSE_DATABASE"
const CLICKHOUSE_METRICS_TABLE = "CLICKHOUSE_METRICS_TABLE"
const CLICKHOUSE_CREATE_TABLES = "CLICKHOUSE_CREATE_TABLES"

type metricStoreLoader func(json.RawMessage) (MetricStore, error)

var metricStoreLoaders = map[string]metricStoreLoader{
	"clickhouse": loadClickHouseMetricStoreFromConfig,
}

type configType struct {
	Type string `json:"type"`
}

func LoadMetricStores(filename string) ([]MetricStore, error) {
	fromConfig, err := LoadMetricStoresFromConfig(filename)
	if err != nil {
		return nil, err
	}

	fromEnvironment, err := LoadMetricStoresFromEnvironment()
	if err != nil {
		return nil, err
	}

	return append(fromConfig, fromEnvironment...), nil
}

func LoadMetricStoresFromConfig(filename string) ([]MetricStore, error) {
	configs := make([]json.RawMessage, 0)
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	err = json.Unmarshal(b, &configs)
	if err != nil {
		return nil, err
	}

	metricStores := make([]MetricStore, len(configs))
	for i, config := range configs {
		loaderType, err := getConfigType(config)
		if err != nil {
			return nil, err
		}

		loader, ok := metricStoreLoaders[loaderType]
		if !ok {
			logUnknownLoaderType(loaderType, config)
			continue
		}

		metricStore, err := loader(config)
		if err != nil {
			return nil, err
		}

		metricStores[i] = metricStore
	}

	return metricStores, nil
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

func loadClickHouseMetricStoreFromConfig(msg json.RawMessage) (MetricStore, error) {
	var config clickhouse.ClickHouseConfig
	if err := json.Unmarshal(msg, &config); err != nil {
		return nil, err
	}
	store, err := clickhouse.NewClickHouseMetricStore(config)
	if err != nil {
		return nil, err
	}
	return store, nil
}

func getConfigType(msg json.RawMessage) (string, error) {
	var config configType
	if err := json.Unmarshal(msg, &config); err != nil {
		return "", err
	}
	return config.Type, nil
}

func loadClickHouseMetricStoreFromEnvironment() (MetricStore, error) {
	dsn, dsnSet := os.LookupEnv(CLICKHOUSE_DSN)
	database, databaseSet := os.LookupEnv(CLICKHOUSE_DATABASE)
	metricsTable, metricsTableSet := os.LookupEnv(CLICKHOUSE_METRICS_TABLE)
	createTablesStr, createTablesSet := os.LookupEnv(CLICKHOUSE_CREATE_TABLES)

	if !dsnSet && !databaseSet && !metricsTableSet {
		return nil, nil
	}

	missingVariables := make([]string, 0)
	if !dsnSet {
		missingVariables = append(missingVariables, CLICKHOUSE_DSN)
	}
	if !databaseSet {
		missingVariables = append(missingVariables, CLICKHOUSE_DATABASE)
	}
	if !metricsTableSet {
		missingVariables = append(missingVariables, CLICKHOUSE_METRICS_TABLE)
	}

	if len(missingVariables) > 0 {
		return nil, missingEnvironmentError{missingVariables}
	}

	createTables := false
	if createTablesSet && (createTablesStr == "true" || createTablesStr == "1" || createTablesStr == "yes") {
		createTables = true
	}

	config := clickhouse.ClickHouseConfig{
		DSN:          dsn,
		Database:     database,
		MetricsTable: metricsTable,
		CreateTables: createTables,
	}

	store, err := clickhouse.NewClickHouseMetricStore(config)
	if err != nil {
		return nil, err
	}
	return store, nil
}

func logUnknownLoaderType(loaderType string, config json.RawMessage) {
	if strings.TrimSpace(loaderType) == "" {
		log.Printf("Encountered an empty loader type. This config will be skipped: %s\n", minifyJson(config))
	} else {
		log.Printf("Encountered an unknown loader type \"%s\". This config will be skipped: %s\n", loaderType, minifyJson(config))
	}
}

// attempts to minfiy the input json swallowing the error if there is one
func minifyJson(b []byte) []byte {
	obj := make(map[string]interface{})
	if err := json.Unmarshal(b, &obj); err != nil {
		return b
	}

	if minified, err := json.Marshal(&obj); err == nil {
		return minified
	}
	return b
}

type missingEnvironmentError struct {
	missingVariables []string
}

func (err missingEnvironmentError) Error() string {
	return fmt.Sprintf("Missing the following environment variables: [ %s ]", strings.Join(err.missingVariables, ", "))
}
