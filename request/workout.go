package request

import (
	"encoding/json"
)

type Workout struct {
	Name                     string         `json:"name"`
	Start                    *Timestamp     `json:"start"`
	End                      *Timestamp     `json:"end"`
	TotalEnergy              QtyUnit        `json:"totalEnergy"`
	ActiveEnergy             QtyUnit        `json:"activeEnergy"`
	AvgHeartRate             QtyUnit        `json:"avgHeartRate"`
	StepCadence              QtyUnit        `json:"stepCadence"`
	Speed                    QtyUnit        `json:"speed"`
	SwimCadence              QtyUnit        `json:"swimCadence"`
	Intensity                QtyUnit        `json:"intesity"`
	Humidity                 QtyUnit        `json:"humidity"`
	TotalSwimmingStrokeCount QtyUnit        `json:"totalSwimmingStrokeCount"`
	FlightsClimbed           QtyUnit        `json:"flightsClimbed"`
	MaxHeartRate             QtyUnit        `json:"maxHeartRate"`
	Distance                 QtyUnit        `json:"distance"`
	StepCount                QtyUnit        `json:"stepCount"`
	Temperature              QtyUnit        `json:"temperature"`
	Elevation                Elevation      `json:"elevation"`
	Route                    []GPSLog       `json:"route"`
	HeartRateData            []HeartRateLog `json:"heartRateData"`
	HeartRateRecovery        []HeartRateLog `json:"heartRateRecovery"`
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
	Lat       float64    `json:"lat"`
	Lon       float64    `json:"lon"`
	Altitude  float64    `json:"altitude"`
	Timestamp *Timestamp `json:"timestamp"`
}

type Elevation struct {
	Ascent  float64 `json:"ascent"`
	Descent float64 `json:"descent"`
	Units   string  `json:"units"`
}

type HeartRateLog struct {
	Units string     `json:"units"`
	Date  *Timestamp `json:"date"`
	Qty   float64    `json:"qty"`
}
