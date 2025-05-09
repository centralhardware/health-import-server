package server

import "github.com/joeecarter/health-import-server/request"

// MetricStore encapsulates a storage backend for the metrics, workouts, and state of mind data provided by the Auto Export app.
// There is a possibility of the same metrics arriving twice so all MetricStores must not store
// duplicate metrics.
type MetricStore interface {
	Name() string
	Store(metrics []request.Metric) error
	StoreWorkouts(workouts []request.Workout) error
	StoreStateOfMind(stateOfMind []request.StateOfMind) error
	OptimizeTables() error
	Close() error
}
