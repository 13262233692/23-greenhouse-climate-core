package entity

import "time"

const (
	VPDMinThreshold = 0.8
	VPDMaxThreshold = 1.2
)

type VPDStatus string

const (
	VPDStatusNormal VPDStatus = "normal"
	VPDStatusLow    VPDStatus = "low"
	VPDStatusHigh   VPDStatus = "high"
)

type VPDReading struct {
	ID              uint64
	SensorID        uint16
	GreenhouseID    string
	AirTemp         float64
	AirHumidity     float64
	LeafTemp        float64
	SaturationVPD   float64
	ActualVPD       float64
	Status          VPDStatus
	Timestamp       time.Time
	ContinuousDeviation time.Duration
}

func (v *VPDReading) Calculate() {
	es := 0.61078 * 1000 * (7.5 * v.AirTemp) / (237.3 + v.AirTemp)
	ea := es * (v.AirHumidity / 100.0)
	v.SaturationVPD = (es - ea) / 1000.0

	esLeaf := 0.61078 * 1000 * (7.5 * v.LeafTemp) / (237.3 + v.LeafTemp)
	v.ActualVPD = (esLeaf - ea) / 1000.0

	v.updateStatus()
}

func (v *VPDReading) updateStatus() {
	if v.ActualVPD < VPDMinThreshold {
		v.Status = VPDStatusLow
	} else if v.ActualVPD > VPDMaxThreshold {
		v.Status = VPDStatusHigh
	} else {
		v.Status = VPDStatusNormal
	}
}

func (v *VPDReading) IsOutOfRange() bool {
	return v.Status != VPDStatusNormal
}

func (v *VPDReading) NeedsCooling() bool {
	return v.Status == VPDStatusHigh
}

func (v *VPDReading) NeedsHumidification() bool {
	return v.Status == VPDStatusLow
}
