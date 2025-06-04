package request

// ECG represents an electrocardiogram entry.
type ECG struct {
	Classification              string       `json:"classification"`
	VoltageMeasurements         []ECGVoltage `json:"voltageMeasurements"`
	Source                      string       `json:"source"`
	AverageHeartRate            float64      `json:"averageHeartRate"`
	Start                       *Timestamp   `json:"start"`
	NumberOfVoltageMeasurements int          `json:"numberOfVoltageMeasurements"`
	SamplingFrequency           int          `json:"samplingFrequency"`
	End                         *Timestamp   `json:"end"`
}

// ECGVoltage holds a single voltage measurement for an ECG recording.
type ECGVoltage struct {
	Date    *UnixTimestamp `json:"date"`
	Voltage float64        `json:"voltage"`
	Units   string         `json:"units"`
}
