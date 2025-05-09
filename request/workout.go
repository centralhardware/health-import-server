package request

import (
	"encoding/json"
)

type Workout struct {
	Name                      string         `json:"name,omitempty"`
	Location                  string         `json:"location,omitempty"`
	Start                     *Timestamp     `json:"start,omitempty"`
	End                       *Timestamp     `json:"end,omitempty"`
	ActiveEnergy              []StepCountLog `json:"activeEnergy,omitempty"`
	ActiveEnergyBurned        QtyUnit        `json:"activeEnergyBurned,omitempty"`
	Intensity                 QtyUnit        `json:"intensity,omitempty"` // Fixed typo in json tag
	Humidity                  QtyUnit        `json:"humidity,omitempty"`
	Distance                  QtyUnit        `json:"distance,omitempty"`
	Duration                  float64        `json:"duration,omitempty"`
	StepCount                 []StepCountLog `json:"stepCount,omitempty"`
	WalkingAndRunningDistance []StepCountLog `json:"walkingAndRunningDistance,omitempty"`
	Temperature               QtyUnit        `json:"temperature,omitempty"`
	ElevationUp               QtyUnit        `json:"elevationUp,omitempty"`
	Route                     []GPSLog       `json:"route,omitempty"`
	HeartRateData             []HeartRateLog `json:"heartRateData,omitempty"`
	HeartRateRecovery         []HeartRateLog `json:"heartRateRecovery,omitempty"`
	ID                        string         `json:"id,omitempty"`
}

type QtyUnit struct {
	Units string  `json:"units"`
	Qty   float64 `json:"qty"`
}

// UnmarshalJSON implements the json.Unmarshaler interface
func (q *QtyUnit) UnmarshalJSON(data []byte) error {
	// Try to unmarshal as a single object first
	type QtyUnitAlias QtyUnit
	var singleObj QtyUnitAlias

	if err := json.Unmarshal(data, &singleObj); err == nil {
		*q = QtyUnit(singleObj)
		return nil
	}

	// If that fails, try to unmarshal as an array and use the first element
	var arrayObj []QtyUnitAlias
	if err := json.Unmarshal(data, &arrayObj); err == nil && len(arrayObj) > 0 {
		*q = QtyUnit(arrayObj[0])
		return nil
	}

	// If both attempts fail, return the original error
	return json.Unmarshal(data, (*QtyUnitAlias)(q))
}

type GPSLog struct {
	Lat                float64    `json:"latitude"`
	Lon                float64    `json:"longitude"`
	Altitude           float64    `json:"altitude"`
	Timestamp          *Timestamp `json:"timestamp"`
	Course             float64    `json:"course,omitempty"`
	VerticalAccuracy   float64    `json:"verticalAccuracy,omitempty"`
	HorizontalAccuracy float64    `json:"horizontalAccuracy,omitempty"`
	CourseAccuracy     float64    `json:"courseAccuracy,omitempty"`
	Speed              float64    `json:"speed,omitempty"`
	SpeedAccuracy      float64    `json:"speedAccuracy,omitempty"`
}

type StepCountLog struct {
	Qty    float64    `json:"qty"`
	Source string     `json:"source"`
	Units  string     `json:"units"`
	Date   *Timestamp `json:"date"`
}

type HeartRateLog struct {
	Min    float64    `json:"Min,omitempty"`
	Max    float64    `json:"Max,omitempty"`
	Avg    float64    `json:"Avg,omitempty"`
	Units  string     `json:"units"`
	Source string     `json:"source,omitempty"`
	Date   *Timestamp `json:"date"`
	Qty    float64    `json:"qty,omitempty"`
}
