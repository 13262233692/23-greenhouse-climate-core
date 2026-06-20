package service

import (
	"greenhouse-climate-core/internal/domain/entity"
	"greenhouse-climate-core/internal/domain/service"
	"time"
)

type PLCCommandGeneratorService struct{}

func NewPLCCommandGeneratorService() service.PLCCommandGenerator {
	return &PLCCommandGeneratorService{}
}

func (g *PLCCommandGeneratorService) GenerateCommand(vpd *entity.VPDReading) *entity.PLCCommand {
	if vpd == nil || !vpd.IsOutOfRange() {
		return nil
	}

	switch vpd.Status {
	case entity.VPDStatusHigh:
		deviation := vpd.ActualVPD - entity.VPDMaxThreshold
		pulseDuration := g.calculateMistDuration(deviation)
		return entity.NewMistCoolingCommand(vpd.GreenhouseID, vpd.ID, pulseDuration)

	case entity.VPDStatusLow:
		deviation := entity.VPDMinThreshold - vpd.ActualVPD
		openingDegree := g.calculateCO2Opening(deviation)
		return entity.NewCO2ControlCommand(vpd.GreenhouseID, vpd.ID, openingDegree)

	default:
		return nil
	}
}

func (g *PLCCommandGeneratorService) calculateMistDuration(deviation float64) time.Duration {
	baseDuration := 10 * time.Second
	maxDuration := 60 * time.Second

	multiplier := deviation * 2
	duration := time.Duration(float64(baseDuration) * multiplier)

	if duration > maxDuration {
		duration = maxDuration
	}
	if duration < baseDuration {
		duration = baseDuration
	}

	return duration
}

func (g *PLCCommandGeneratorService) calculateCO2Opening(deviation float64) uint8 {
	baseOpening := uint8(30)
	maxOpening := uint8(100)

	additional := uint8(deviation * 50)
	opening := baseOpening + additional

	if opening > maxOpening {
		opening = maxOpening
	}

	return opening
}
