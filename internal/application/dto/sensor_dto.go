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

type DLIDTO struct {
	ID              uint64    `json:"id"`
	GreenhouseID    string    `json:"greenhouse_id"`
	SensorID        uint16    `json:"sensor_id"`
	Date            time.Time `json:"date"`
	AccumulatedDLI  float64   `json:"accumulated_dli"`
	TargetDLI       float64   `json:"target_dli"`
	CurrentPAR      float64   `json:"current_par"`
	SunriseTime     time.Time `json:"sunrise_time"`
	SunsetTime      time.Time `json:"sunset_time"`
	LastUpdated     time.Time `json:"last_updated"`
	Deficit         float64   `json:"deficit"`
	ProjectedDLI    float64   `json:"projected_dli"`
	IsSupplementing bool      `json:"is_supplementing"`
	RemainingMinutes int      `json:"remaining_minutes"`
}

type LightPlanDTO struct {
	ID             uint64      `json:"id"`
	GreenhouseID   string      `json:"greenhouse_id"`
	TargetDLI      float64     `json:"target_dli"`
	CurrentDLI     float64     `json:"current_dli"`
	Deficit        float64     `json:"deficit"`
	RemainingHours float64     `json:"remaining_hours"`
	RequiredPower  float64     `json:"required_power"`
	PowerSteps     []float64   `json:"power_steps"`
	StepDurationMs int64       `json:"step_duration_ms"`
	Devices        []LEDDeviceDTO `json:"devices"`
	StartTime      time.Time   `json:"start_time"`
	EndTime        time.Time   `json:"end_time"`
	IsActive       bool        `json:"is_active"`
	CreatedAt      time.Time   `json:"created_at"`
	CurrentStep    int         `json:"current_step"`
	CurrentPower   float64     `json:"current_power"`
}

type LEDDeviceDTO struct {
	ID           uint8   `json:"id"`
	Name         string  `json:"name"`
	MaxPower     float64 `json:"max_power"`
	CurrentPower float64 `json:"current_power"`
	IsActive     bool    `json:"is_active"`
	Zone         string  `json:"zone"`
	PowerPercent float64 `json:"power_percent"`
}

type SunTimesDTO struct {
	Sunrise          time.Time `json:"sunrise"`
	Sunset           time.Time `json:"sunset"`
	DaylightDuration string    `json:"daylight_duration"`
	DaylightHours    float64   `json:"daylight_hours"`
	IsDaylight       bool      `json:"is_daylight"`
	TimeUntilSunset  string    `json:"time_until_sunset"`
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

func ToDLIDTO(d *entity.DLIReading) *DLIDTO {
	if d == nil {
		return nil
	}
	return &DLIDTO{
		ID:               d.ID,
		GreenhouseID:     d.GreenhouseID,
		SensorID:         d.SensorID,
		Date:             d.Date,
		AccumulatedDLI:   d.AccumulatedDLI,
		TargetDLI:        d.TargetDLI,
		CurrentPAR:       d.CurrentPAR,
		SunriseTime:      d.SunriseTime,
		SunsetTime:       d.SunsetTime,
		LastUpdated:      d.LastUpdated,
		Deficit:          d.Deficit,
		ProjectedDLI:     d.ProjectedDLI,
		IsSupplementing:  d.IsSupplementing,
		RemainingMinutes: d.GetRemainingMinutesUntilSunset(),
	}
}

func ToLightPlanDTO(p *entity.LightSupplementPlan) *LightPlanDTO {
	if p == nil {
		return nil
	}
	devices := make([]LEDDeviceDTO, 0, len(p.Devices))
	for _, dev := range p.Devices {
		devices = append(devices, ToLEDDeviceDTO(dev))
	}
	return &LightPlanDTO{
		ID:             p.ID,
		GreenhouseID:   p.GreenhouseID,
		TargetDLI:      p.TargetDLI,
		CurrentDLI:     p.CurrentDLI,
		Deficit:        p.Deficit,
		RemainingHours: p.RemainingHours,
		RequiredPower:  p.RequiredPower,
		PowerSteps:     p.PowerSteps,
		StepDurationMs: p.StepDuration.Milliseconds(),
		Devices:        devices,
		StartTime:      p.StartTime,
		EndTime:        p.EndTime,
		IsActive:       p.IsActive,
		CreatedAt:      p.CreatedAt,
		CurrentStep:    p.GetCurrentStep(),
		CurrentPower:   p.GetCurrentPower(),
	}
}

func ToLEDDeviceDTO(d *entity.LEDLightDevice) LEDDeviceDTO {
	powerPercent := 0.0
	if d.MaxPower > 0 {
		powerPercent = (d.CurrentPower / d.MaxPower) * 100
	}
	return LEDDeviceDTO{
		ID:           d.ID,
		Name:         d.Name,
		MaxPower:     d.MaxPower,
		CurrentPower: d.CurrentPower,
		IsActive:     d.IsActive,
		Zone:         d.Zone,
		PowerPercent: powerPercent,
	}
}

func ToSunTimesDTO(sunrise, sunset time.Time, now time.Time) *SunTimesDTO {
	duration := sunset.Sub(sunrise)
	until := time.Until(sunset)
	if until < 0 {
		until = 0
	}
	return &SunTimesDTO{
		Sunrise:          sunrise,
		Sunset:           sunset,
		DaylightDuration: duration.String(),
		DaylightHours:    duration.Hours(),
		IsDaylight:       !now.Before(sunrise) && !now.After(sunset),
		TimeUntilSunset:  until.String(),
	}
}
