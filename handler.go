package server

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/joeecarter/health-import-server/request"
)

type ImportHandler struct {
	MetricStores []MetricStore
}

func NewImportHandler(metricStores []MetricStore) *ImportHandler {
	return &ImportHandler{metricStores}
}

func (handler *ImportHandler) ServeHTTP(wr http.ResponseWriter, req *http.Request) {
	msg, err := handler.handle(req)
	if err == nil {
		wr.WriteHeader(200)
		wr.Write([]byte(msg + "\n"))
	} else {
		wr.WriteHeader(500)
		wr.Write([]byte("ERROR: " + err.Error() + "\n"))
	}
}

func (handler *ImportHandler) handle(req *http.Request) (string, error) {
	log.Printf("Received request with User-Agent: '%s'\n", req.Header.Get("User-Agent"))

	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		panic("Failed to get body, err =" + err.Error())
	}

	export, err := request.Parse(b)
	if err != nil {
		return "", err
	}

	populatedMetrics := export.PopulatedMetrics()
	totalMetrics := len(export.Metrics)
	populatedMetricsCount := len(populatedMetrics)
	totalSamples := export.TotalSamples()
	totalWorkouts := len(export.Workouts)

	log.Printf("Total metrics: %d (%d populated) Total samples %d Total workouts: %d\n",
		totalMetrics, populatedMetricsCount, totalSamples, totalWorkouts)

	// Create a detailed response message immediately
	responseMsg := fmt.Sprintf("Processing request. Received %d metrics (%d populated), %d samples, and %d workouts.",
		totalMetrics, populatedMetricsCount, totalSamples, totalWorkouts)

	// Process data in the background
	go func() {
		// Copy data to avoid race conditions
		localPopulatedMetrics := populatedMetrics
		localWorkouts := export.Workouts

		for _, metricStore := range handler.MetricStores {
			log.Printf("Starting upload to metric store \"%s\".", metricStore.Name())

			// Store metrics
			if len(localPopulatedMetrics) > 0 {
				if err := metricStore.Store(localPopulatedMetrics); err != nil {
					log.Printf("Failed upload metrics to metric store \"%s\" with error: %s.", metricStore.Name(), err.Error())
					continue
				}
			}

			// Store workouts
			if len(localWorkouts) > 0 {
				if err := metricStore.StoreWorkouts(localWorkouts); err != nil {
					log.Printf("Failed upload workouts to metric store \"%s\" with error: %s.", metricStore.Name(), err.Error())
					continue
				}
			}

			// Optimize tables after data is stored
			if err := metricStore.OptimizeTables(); err != nil {
				log.Printf("Failed to optimize tables in metric store \"%s\" with error: %s.", metricStore.Name(), err.Error())
				continue
			}

			log.Printf("Finished upload to metric store \"%s\" and optimized tables.", metricStore.Name())
		}
	}()

	return responseMsg, nil
}
