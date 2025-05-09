package request

import (
	"encoding/json"
	"time"
)

// StateOfMind represents a state of mind entry from the health data
type StateOfMind struct {
	ID                    string     `json:"id,omitempty"`
	Valence               float64    `json:"valence"`
	ValenceClassification string     `json:"valenceClassification"`
	Labels                []string   `json:"labels"`
	Associations          []string   `json:"associations"`
	Start                 *Timestamp `json:"start"`
	End                   *Timestamp `json:"end"`
	Kind                  string     `json:"kind"`
}

// UnmarshalJSON implements custom JSON unmarshaling for StateOfMind
// to handle the ISO 8601 timestamp format with 'Z' suffix
func (s *StateOfMind) UnmarshalJSON(data []byte) error {
	// Define an alias type to avoid infinite recursion
	type StateOfMindAlias StateOfMind

	// Create a temporary struct with custom timestamp fields
	temp := struct {
		Start string `json:"start"`
		End   string `json:"end"`
		*StateOfMindAlias
	}{
		StateOfMindAlias: (*StateOfMindAlias)(s),
	}

	// Unmarshal the JSON data into the temporary struct
	if err := json.Unmarshal(data, &temp); err != nil {
		return err
	}

	// Parse the start timestamp
	if temp.Start != "" {
		startTime, err := time.Parse(time.RFC3339, temp.Start)
		if err != nil {
			return err
		}
		s.Start = &Timestamp{t: startTime}
	}

	// Parse the end timestamp
	if temp.End != "" {
		endTime, err := time.Parse(time.RFC3339, temp.End)
		if err != nil {
			return err
		}
		s.End = &Timestamp{t: endTime}
	}

	return nil
}
