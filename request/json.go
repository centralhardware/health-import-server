package request

import (
	"encoding/json"
)

// jsonRequest is the schema of the original JSON request before any tranformations
type jsonRequest struct {
	Data jsonData
}

type jsonData struct {
	Metrics     []jsonMetric  `json:"metrics,omitempty"`
	Workouts    []Workout     `json:"workouts,omitempty"`
	StateOfMind []StateOfMind `json:"stateOfMind,omitempty"`
}

type jsonMetric struct {
	Name  string
	Units string
	Data  []json.RawMessage
}

func parseJSONRequest(b []byte) (*jsonRequest, error) {
	req := &jsonRequest{}
	err := json.Unmarshal(b, req)
	return req, err
}
