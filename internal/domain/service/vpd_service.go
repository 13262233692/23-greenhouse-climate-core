package service

import (
	"greenhouse-climate-core/internal/domain/entity"
	"time"
)

type VPDCalculator interface {
	Calculate(airTemp, airHumidity, leafTemp float64) *entity.VPDReading
}

type VPDRuleEngine interface {
	ShouldTriggerAction(vpd *entity.VPDReading, deviationHistory []*entity.VPDReading) (bool, time.Duration)
	GetContinuousDeviation(history []*entity.VPDReading) time.Duration
}

type PLCCommandGenerator interface {
	GenerateCommand(vpd *entity.VPDReading) *entity.PLCCommand
}
