package service

import (
	"greenhouse-climate-core/internal/domain/entity"
	"greenhouse-climate-core/internal/domain/repository"
	"greenhouse-climate-core/internal/domain/service"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	DefaultContinuousThreshold = 3 * time.Minute
	DefaultHistoryWindow       = 5 * time.Minute
)

type VPDRuleEngineService struct {
	vpdRepo             repository.VPDRepository
	continuousThreshold time.Duration
	historyWindow       time.Duration
	logger              *logrus.Logger
	lastTriggered       map[string]time.Time
	cooldownPeriod      time.Duration
	mu                  sync.RWMutex
}

func NewVPDRuleEngineService(
	vpdRepo repository.VPDRepository,
	logger *logrus.Logger,
) service.VPDRuleEngine {
	return &VPDRuleEngineService{
		vpdRepo:             vpdRepo,
		continuousThreshold: DefaultContinuousThreshold,
		historyWindow:       DefaultHistoryWindow,
		logger:              logger,
		lastTriggered:       make(map[string]time.Time),
		cooldownPeriod:      1 * time.Minute,
	}
}

func (e *VPDRuleEngineService) SetContinuousThreshold(duration time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.continuousThreshold = duration
}

func (e *VPDRuleEngineService) GetContinuousDeviation(history []*entity.VPDReading) time.Duration {
	if len(history) < 2 {
		return 0
	}

	var currentStatus entity.VPDStatus
	var startTime time.Time
	var maxDuration time.Duration
	var currentDuration time.Duration

	for i, vpd := range history {
		if vpd.Status == entity.VPDStatusNormal {
			if currentDuration > maxDuration {
				maxDuration = currentDuration
			}
			currentDuration = 0
			startTime = time.Time{}
			continue
		}

		if startTime.IsZero() {
			currentStatus = vpd.Status
			startTime = vpd.Timestamp
			continue
		}

		if vpd.Status == currentStatus {
			currentDuration = vpd.Timestamp.Sub(startTime)
		} else {
			if currentDuration > maxDuration {
				maxDuration = currentDuration
			}
			currentStatus = vpd.Status
			startTime = vpd.Timestamp
			currentDuration = 0
		}

		if i == len(history)-1 && currentDuration > maxDuration {
			maxDuration = currentDuration
		}
	}

	return maxDuration
}

func (e *VPDRuleEngineService) ShouldTriggerAction(
	vpd *entity.VPDReading,
	deviationHistory []*entity.VPDReading,
) (bool, time.Duration) {
	if !vpd.IsOutOfRange() {
		return false, 0
	}

	continuousDeviation := e.GetContinuousDeviation(deviationHistory)
	vpd.ContinuousDeviation = continuousDeviation

	e.mu.RLock()
	lastTrigger, hasTriggered := e.lastTriggered[vpd.GreenhouseID]
	e.mu.RUnlock()

	if hasTriggered && time.Since(lastTrigger) < e.cooldownPeriod {
		e.logger.Debugf("Cooldown active for greenhouse %s, skipping trigger", vpd.GreenhouseID)
		return false, continuousDeviation
	}

	e.mu.RLock()
	threshold := e.continuousThreshold
	e.mu.RUnlock()

	shouldTrigger := continuousDeviation >= threshold

	if shouldTrigger {
		e.logger.Infof("VPD deviation threshold reached for greenhouse %s: %.2f >= %.2f",
			vpd.GreenhouseID, continuousDeviation.Seconds(), threshold.Seconds())

		e.mu.Lock()
		e.lastTriggered[vpd.GreenhouseID] = time.Now()
		e.mu.Unlock()
	} else {
		e.logger.Debugf("VPD deviation for greenhouse %s: %.2f < %.2f seconds",
			vpd.GreenhouseID, continuousDeviation.Seconds(), threshold.Seconds())
	}

	return shouldTrigger, continuousDeviation
}

func (e *VPDRuleEngineService) EvaluateGreenhouse(greenhouseID string) (bool, *entity.VPDReading, time.Duration, error) {
	vpd, err := e.vpdRepo.FindLatestByGreenhouseID(greenhouseID)
	if err != nil {
		return false, nil, 0, err
	}
	if vpd == nil {
		return false, nil, 0, nil
	}

	e.mu.RLock()
	window := e.historyWindow
	e.mu.RUnlock()

	history, err := e.vpdRepo.FindDeviationHistory(greenhouseID, window)
	if err != nil {
		return false, vpd, 0, err
	}

	shouldTrigger, deviation := e.ShouldTriggerAction(vpd, history)
	return shouldTrigger, vpd, deviation, nil
}

func (e *VPDRuleEngineService) ResetCooldown(greenhouseID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.lastTriggered, greenhouseID)
	e.logger.Infof("Cooldown reset for greenhouse %s", greenhouseID)
}
