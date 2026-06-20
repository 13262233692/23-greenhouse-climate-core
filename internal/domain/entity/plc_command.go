package entity

import "time"

type PLCCommandType string

const (
	PLCCommandTypeMistCooling PLCCommandType = "mist_cooling"
	PLCCommandTypeCO2Control  PLCCommandType = "co2_control"
	PLCCommandTypeStop        PLCCommandType = "stop"
)

type PLCCommandStatus string

const (
	PLCCommandStatusPending   PLCCommandStatus = "pending"
	PLCCommandStatusSent      PLCCommandStatus = "sent"
	PLCCommandStatusCompleted PLCCommandStatus = "completed"
	PLCCommandStatusFailed    PLCCommandStatus = "failed"
)

type PLCCommand struct {
	ID              uint64
	GreenhouseID    string
	VPDID           uint64
	Type            PLCCommandType
	TargetDevice    uint8
	OpeningDegree   uint8
	PulseDuration   time.Duration
	Status          PLCCommandStatus
	CreatedAt       time.Time
	SentAt          time.Time
	CompletedAt     time.Time
	Reason          string
}

func NewMistCoolingCommand(greenhouseID string, vpdID uint64, pulseDuration time.Duration) *PLCCommand {
	return &PLCCommand{
		GreenhouseID:  greenhouseID,
		VPDID:         vpdID,
		Type:          PLCCommandTypeMistCooling,
		TargetDevice:  1,
		OpeningDegree: 100,
		PulseDuration: pulseDuration,
		Status:        PLCCommandStatusPending,
		CreatedAt:     time.Now(),
		Reason:        "VPD exceeds high threshold",
	}
}

func NewCO2ControlCommand(greenhouseID string, vpdID uint64, openingDegree uint8) *PLCCommand {
	return &PLCCommand{
		GreenhouseID:  greenhouseID,
		VPDID:         vpdID,
		Type:          PLCCommandTypeCO2Control,
		TargetDevice:  2,
		OpeningDegree: openingDegree,
		PulseDuration: 5 * time.Second,
		Status:        PLCCommandStatusPending,
		CreatedAt:     time.Now(),
		Reason:        "VPD below low threshold",
	}
}

func NewStopCommand(greenhouseID string, targetDevice uint8) *PLCCommand {
	return &PLCCommand{
		GreenhouseID:  greenhouseID,
		Type:          PLCCommandTypeStop,
		TargetDevice:  targetDevice,
		OpeningDegree: 0,
		PulseDuration: 1 * time.Second,
		Status:        PLCCommandStatusPending,
		CreatedAt:     time.Now(),
		Reason:        "Manual stop command",
	}
}

func (c *PLCCommand) MarkSent() {
	c.Status = PLCCommandStatusSent
	c.SentAt = time.Now()
}

func (c *PLCCommand) MarkCompleted() {
	c.Status = PLCCommandStatusCompleted
	c.CompletedAt = time.Now()
}

func (c *PLCCommand) MarkFailed(reason string) {
	c.Status = PLCCommandStatusFailed
	c.Reason = reason
}
