package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	server "github.com/joeecarter/health-import-server"
)

var Version = "0.0.0"

var addr string

var metricStores []server.MetricStore

func init() {
	flag.StringVar(&addr, "addr", ":8080", "The address to start the server on e.g. ':8080'")
	flag.Parse()

	var err error
	metricStores, err = server.LoadMetricStores()
	if err != nil {
		fmt.Printf("Failed to load metric stores: %s.\n", err.Error())
		os.Exit(1)
	}

	if len(metricStores) == 0 {
		printConfigurationExplanation()
		os.Exit(1)
	}
}

func main() {

	http.Handle("/upload", server.NewImportHandler(metricStores))

	log.Printf("Starting health-import-server v%s with addr '%s'...\n", Version, addr)
	log.Printf("Point Auto Export to /upload")
	err := http.ListenAndServe(addr, nil)
	if err != nil {
		fmt.Printf("ERROR: %s\n", err)
	}
}

func printConfigurationExplanation() {
	fmt.Printf("You have no metric stores configured.\n\n")

	fmt.Printf("Configure the database by setting environment variables:\n")
	fmt.Println("- CLICKHOUSE_DSN")
	fmt.Println("- CLICKHOUSE_DATABASE")
}
