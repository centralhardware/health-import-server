package main

import (
	"fmt"
	"io/ioutil"
	"log"

	"github.com/joeecarter/health-import-server/request"
)

func main() {
	// Read the workouts.json file
	jsonData, err := ioutil.ReadFile("workouts.json")
	if err != nil {
		log.Fatalf("Failed to read workouts.json: %v", err)
	}

	// Parse the JSON data
	req, err := request.Parse(jsonData)
	if err != nil {
		log.Fatalf("Failed to parse request: %v", err)
	}

	// Print information about the parsed data
	fmt.Printf("Total workouts: %d\n", len(req.Workouts))

	// Check if there are any workouts
	if len(req.Workouts) > 0 {
		workout := req.Workouts[0]
		fmt.Printf("Workout name: %s\n", workout.Name)
		fmt.Printf("Workout location: %s\n", workout.Location)

		// Check for ActiveEnergyBurned field
		if workout.ActiveEnergyBurned.Qty != 0 {
			fmt.Printf("ActiveEnergyBurned: %f %s\n", workout.ActiveEnergyBurned.Qty, workout.ActiveEnergyBurned.Units)
		} else if len(workout.ActiveEnergy) > 0 {
			fmt.Printf("ActiveEnergy: %f %s\n", workout.ActiveEnergy[0].Qty, workout.ActiveEnergy[0].Units)
		} else {
			fmt.Println("No active energy data found")
		}

		// Check for ElevationUp field
		if workout.ElevationUp.Qty != 0 {
			fmt.Printf("ElevationUp: %f %s\n", workout.ElevationUp.Qty, workout.ElevationUp.Units)
		} else {
			fmt.Println("No elevation data found")
		}

		// Check for Duration field
		if workout.Duration != 0 {
			fmt.Printf("Duration: %f\n", workout.Duration)
		} else {
			fmt.Println("No duration data found")
		}
	}

	fmt.Println("Request parsed successfully!")
}
