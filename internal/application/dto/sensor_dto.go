package dto

import (
	"greenhouse-climate-core/internal/domain/entity"
	"time"
)

type SensorDTO struct {
	ID          uint16 `json:"id"`
	SlaveID     uint8  `json:"slave_id"`
	Type        string `json:"type"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Address     string `json:"address"`
	Port        int    `json:"port"`
	Register    uint16 `json:"register"`
	IsConnected bool   `json:"is_connected"`
}

type SensorReadingDTO struct {
	SensorID    uint16    `json:"sensor_id"`
	Type        string    `json:"type"`
	LeafTemp    float64   `json:"leaf_temp,omitempty"`
	LeafHumidity float64  `json:"leaf_humidity,omitempty"`
	AirTemp     float64   `json:"air_temp,omitempty"`
	AirHumidity float64   `json:"air_humidity,omitempty"`
	PAR         float64   `json:"par,omitempty"`
	CO2         float64   `json:"co2,omitempty"`
	Timestamp   time.Time `json:"timestamp"`
}

type VPDDTO struct {
	ID                  uint64        `json:"id"`
	SensorID            uint16        `json:"sensor_id"`
	GreenhouseID        string        `json:"greenhouse_id"`
	AirTemp             float64       `json:"air_temp"`
	AirHumidity         float64       `json:"air_humidity"`
	LeafTemp            float64       `json:"leaf_temp"`
	SaturationVPD       float64       `json:"saturation_vpd"`
	ActualVPD           float64       `json:"actual_vpd"`
	Status              string        `json:"status"`
	Timestamp           time.Time     `json:"timestamp"`
	ContinuousDeviation time.Duration `json:"continuous_deviation"`
}

type PLCCommandDTO struct {
	ID            uint64    `json:"id"`
	GreenhouseID  string    `json:"greenhouse_id"`
	VPDID         uint64    `json:"vpd_id,omitempty"`
	Type          string    `json:"type"`
	TargetDevice  uint8     `json:"target_device"`
	OpeningDegree uint8     `json:"opening_degree"`
	PulseDuration int64     `json:"pulse_duration_ms"`
	Status        string    `json:"status"`
	CreatedAt     time.Time `json:"created_at"`
	Reason        string    `json:"reason"`
}

type ControlAction struct {
	Action      string `json:"action"`
	TargetDevice uint8 `json:"target_device"`
	DurationMs  int64  `json:"duration_ms,omitempty"`
	Reason      string `json:"reason"`
}

func ToSensorDTO(s *entity.Sensor, isConnected bool) *SensorDTO {
	return &SensorDTO{
		ID:          s.ID,
		SlaveID:     s.SlaveID,
		Type:        string(s.Type),
		Name:        s.Name,
		Description: s.Description,
		Address:     s.Address,
		Port:        s.Port,
		Register:    s.Register,
		IsConnected: isConnected,
	}
}

func ToSensorReadingDTO(r *entity.SensorReading) *SensorReadingDTO {
	return &SensorReadingDTO{
		SensorID:     r.SensorID,
		Type:         string(r.Type),
		LeafTemp:     r.LeafTemp,
		LeafHumidity: r.LeafHumidity,
		AirTemp:      r.AirTemp,
		AirHumidity:  r.AirHumidity,
		PAR:          r.PAR,
		CO2:          r.CO2,
		Timestamp:    r.Timestamp,
	}
}

func ToVPDDTO(v *entity.VPDReading) *VPDDTO {
	return &VPDDTO{
		ID:                  v.ID,
		SensorID:            v.SensorID,
		GreenhouseID:        v.GreenhouseID,
		AirTemp:             v.AirTemp,
		AirHumidity:         v.AirHumidity,
		LeafTemp:            v.LeafTemp,
		SaturationVPD:       v.SaturationVPD,
		ActualVPD:           v.ActualVPD,
		Status:              string(v.Status),
		Timestamp:           v.Timestamp,
		ContinuousDeviation: v.ContinuousDeviation,
	}
}

func ToPLCCommandDTO(c *entity.PLCCommand) *PLCCommandDTO {
	return &PLCCommandDTO{
		ID:            c.ID,
		GreenhouseID:  c.GreenhouseID,
		VPDID:         c.VPDID,
		Type:          string(c.Type),
		TargetDevice:  c.TargetDevice,
		OpeningDegree: c.OpeningDegree,
		PulseDuration: c.PulseDuration.Milliseconds(),
		Status:        string(c.Status),
		CreatedAt:     c.CreatedAt,
		Reason:        c.Reason,
	}
}
