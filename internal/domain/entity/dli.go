package entity

import (
	"time"
)

const (
	TomatoDLITarget      = 15.0
	DefaultSunsetCutoff  = 2 * time.Hour
	DefaultMinuteWindow  = 1 * time.Minute
)

type MinutePARBucket struct {
	StartMinute time.Time
	SumPAR      float64
	SampleCount int
	AvgPAR      float64
}

type DLIReading struct {
	ID              uint64
	GreenhouseID    string
	SensorID        uint16
	Date            time.Time
	AccumulatedDLI  float64
	TargetDLI       float64
	CurrentPAR      float64
	MinuteBuckets   []*MinutePARBucket
	SunriseTime     time.Time
	SunsetTime      time.Time
	LastUpdated     time.Time
	Deficit         float64
	ProjectedDLI    float64
	IsSupplementing bool
}

type LEDLightDevice struct {
	ID          uint8
	Name        string
	MaxPower    float64
	CurrentPower float64
	IsActive    bool
	Zone        string
}

type LightSupplementPlan struct {
	ID              uint64
	GreenhouseID    string
	DLIID           uint64
	TargetDLI       float64
	CurrentDLI      float64
	Deficit         float64
	RemainingHours  float64
	RequiredPower   float64
	PowerSteps      []float64
	StepDuration    time.Duration
	Devices         []*LEDLightDevice
	StartTime       time.Time
	EndTime         time.Time
	IsActive        bool
	CreatedAt       time.Time
	CompletedAt     time.Time
}

func NewMinutePARBucket(startTime time.Time) *MinutePARBucket {
	return &MinutePARBucket{
		StartMinute: startTime.Truncate(time.Minute),
		SumPAR:      0,
		SampleCount: 0,
		AvgPAR:      0,
	}
}

func (b *MinutePARBucket) AddSample(par float64) {
	b.SumPAR += par
	b.SampleCount++
	b.AvgPAR = b.SumPAR / float64(b.SampleCount)
}

func (b *MinutePARBucket) Integrate() float64 {
	seconds := 60.0
	return b.AvgPAR * seconds / 1e6
}

func NewDLIReading(greenhouseID string, sensorID uint16, targetDLI float64) *DLIReading {
	now := time.Now()
	return &DLIReading{
		GreenhouseID:   greenhouseID,
		SensorID:       sensorID,
		Date:           now.Truncate(24 * time.Hour),
		AccumulatedDLI: 0,
		TargetDLI:      targetDLI,
		MinuteBuckets:  make([]*MinutePARBucket, 0),
		LastUpdated:    now,
	}
}

func (d *DLIReading) AddPARSample(par float64, timestamp time.Time) {
	minuteStart := timestamp.Truncate(DefaultMinuteWindow)

	var currentBucket *MinutePARBucket
	if len(d.MinuteBuckets) > 0 {
		lastBucket := d.MinuteBuckets[len(d.MinuteBuckets)-1]
		if lastBucket.StartMinute.Equal(minuteStart) {
			currentBucket = lastBucket
		}
	}

	if currentBucket == nil {
		currentBucket = NewMinutePARBucket(minuteStart)
		d.MinuteBuckets = append(d.MinuteBuckets, currentBucket)
	}

	currentBucket.AddSample(par)
	d.CurrentPAR = par
	d.recalculate()
	d.LastUpdated = timestamp
}

func (d *DLIReading) recalculate() {
	totalDLI := 0.0
	for _, bucket := range d.MinuteBuckets {
		totalDLI += bucket.Integrate()
	}
	d.AccumulatedDLI = totalDLI
	d.Deficit = d.TargetDLI - d.AccumulatedDLI
	if d.Deficit < 0 {
		d.Deficit = 0
	}
}

func (d *DLIReading) CalculateProjectedDLI() {
	if d.SunsetTime.IsZero() || d.SunriseTime.IsZero() {
		d.ProjectedDLI = d.AccumulatedDLI
		return
	}

	now := time.Now()
	elapsed := now.Sub(d.SunriseTime)
	totalDaylight := d.SunsetTime.Sub(d.SunriseTime)

	if elapsed <= 0 || totalDaylight <= 0 {
		d.ProjectedDLI = d.AccumulatedDLI
		return
	}

	rate := d.AccumulatedDLI / elapsed.Hours()
	d.ProjectedDLI = rate * totalDaylight.Hours()
}

func (d *DLIReading) IsNearSunset(cutoff time.Duration) bool {
	if d.SunsetTime.IsZero() {
		return false
	}
	return time.Until(d.SunsetTime) <= cutoff
}

func (d *DLIReading) NeedsSupplement() bool {
	return d.Deficit > 0
}

func (d *DLIReading) GetRemainingMinutesUntilSunset() int {
	if d.SunsetTime.IsZero() {
		return 0
	}
	remaining := time.Until(d.SunsetTime)
	if remaining < 0 {
		return 0
	}
	return int(remaining.Minutes())
}

func NewLightSupplementPlan(
	greenhouseID string,
	dli *DLIReading,
	devices []*LEDLightDevice,
) *LightSupplementPlan {
	remainingMinutes := float64(dli.GetRemainingMinutesUntilSunset())
	if remainingMinutes <= 0 {
		remainingMinutes = 120
	}

	remainingHours := remainingMinutes / 60.0

	requiredDLI := dli.Deficit
	requiredPAR := (requiredDLI * 1e6) / (remainingHours * 3600.0)

	totalMaxPower := 0.0
	for _, dev := range devices {
		totalMaxPower += dev.MaxPower
	}

	requiredPower := 0.0
	if totalMaxPower > 0 {
		requiredPower = requiredPAR
	}

	plan := &LightSupplementPlan{
		GreenhouseID:   greenhouseID,
		DLIID:          dli.ID,
		TargetDLI:      dli.TargetDLI,
		CurrentDLI:     dli.AccumulatedDLI,
		Deficit:        dli.Deficit,
		RemainingHours: remainingHours,
		RequiredPower:  requiredPower,
		StepDuration:   10 * time.Minute,
		Devices:        devices,
		StartTime:      time.Now(),
		EndTime:        dli.SunsetTime,
		IsActive:       true,
		CreatedAt:      time.Now(),
	}

	plan.calculatePowerSteps()
	return plan
}

func (p *LightSupplementPlan) calculatePowerSteps() {
	steps := 5
	p.PowerSteps = make([]float64, steps)

	for i := 0; i < steps; i++ {
		ratio := float64(i+1) / float64(steps)
		p.PowerSteps[i] = p.RequiredPower * ratio
	}
}

func (p *LightSupplementPlan) GetCurrentStep() int {
	elapsed := time.Since(p.StartTime)
	stepIndex := int(elapsed / p.StepDuration)
	if stepIndex >= len(p.PowerSteps) {
		return len(p.PowerSteps) - 1
	}
	if stepIndex < 0 {
		return 0
	}
	return stepIndex
}

func (p *LightSupplementPlan) GetCurrentPower() float64 {
	if !p.IsActive {
		return 0
	}
	step := p.GetCurrentStep()
	if step >= len(p.PowerSteps) {
		return 0
	}
	return p.PowerSteps[step]
}

func (p *LightSupplementPlan) Complete() {
	p.IsActive = false
	p.CompletedAt = time.Now()
}
