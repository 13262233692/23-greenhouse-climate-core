package service

import (
	"context"
	"greenhouse-climate-core/internal/domain/entity"
	"greenhouse-climate-core/internal/domain/repository"
	"greenhouse-climate-core/internal/infrastructure/plc"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	DefaultDLIEvalInterval    = 30 * time.Second
	DefaultPARCollectInterval = 1 * time.Second
	DefaultSmoothSteps        = 5
	DefaultStepDuration       = 10 * time.Minute
	DefaultSupplementCutoff   = 2 * time.Hour
)

type DLICoordinator struct {
	dliRepo       repository.DLIRepository
	planRepo      repository.LightSupplementPlanRepository
	ledRepo       repository.LEDDeviceRepository
	ledController *plc.LEDController
	sunCalc       *SunCalculator
	logger        *logrus.Logger
	greenhouseID  string
	targetDLI     float64
	pSensorIDs    []uint16

	readingChan   <-chan *entity.SensorReading
	latestPAR     map[uint16]float64
	currentDLI    *entity.DLIReading
	activePlan    *entity.LightSupplementPlan

	stopChan      chan struct{}
	wg            sync.WaitGroup
	mu            sync.RWMutex
	ctx           context.Context
	cancel        context.CancelFunc

	dliSamples    uint64
	supplementOps uint64
	isRunning     bool
}

func NewDLICoordinator(
	dliRepo repository.DLIRepository,
	planRepo repository.LightSupplementPlanRepository,
	ledRepo repository.LEDDeviceRepository,
	ledController *plc.LEDController,
	sunCalc *SunCalculator,
	logger *logrus.Logger,
	greenhouseID string,
	parSensorIDs []uint16,
) *DLICoordinator {
	ctx, cancel := context.WithCancel(context.Background())

	return &DLICoordinator{
		dliRepo:       dliRepo,
		planRepo:      planRepo,
		ledRepo:       ledRepo,
		ledController: ledController,
		sunCalc:       sunCalc,
		logger:        logger,
		greenhouseID:  greenhouseID,
		targetDLI:     entity.TomatoDLITarget,
		pSensorIDs:    parSensorIDs,
		latestPAR:     make(map[uint16]float64),
		stopChan:      make(chan struct{}),
		ctx:           ctx,
		cancel:        cancel,
	}
}

func (d *DLICoordinator) SetReadingChannel(ch <-chan *entity.SensorReading) {
	d.readingChan = ch
}

func (d *DLICoordinator) SetTargetDLI(target float64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.targetDLI = target
	if d.currentDLI != nil {
		d.currentDLI.TargetDLI = target
	}
}

func (d *DLICoordinator) Start() error {
	d.mu.Lock()
	if d.isRunning {
		d.mu.Unlock()
		return nil
	}
	d.isRunning = true
	d.mu.Unlock()

	d.initializeDailyDLI()

	d.wg.Add(1)
	go d.collectPARData()

	d.wg.Add(1)
	go d.evaluateDLIRules()

	d.logger.Infof("DLI coordinator started for greenhouse %s, target=%.1f mol/m²/d, PAR sensors=%d",
		d.greenhouseID, d.targetDLI, len(d.pSensorIDs))
	return nil
}

func (d *DLICoordinator) initializeDailyDLI() {
	now := time.Now()
	today := now.Truncate(24 * time.Hour)

	existing, err := d.dliRepo.FindByGreenhouseIDAndDate(d.greenhouseID, today)
	if err != nil {
		d.logger.Warnf("Failed to load existing DLI: %v", err)
	}

	if existing != nil {
		d.currentDLI = existing
		d.logger.Infof("Resumed DLI from storage: %.2f mol/m²/d", existing.AccumulatedDLI)
	} else {
		parSensorID := uint16(0)
		if len(d.pSensorIDs) > 0 {
			parSensorID = d.pSensorIDs[0]
		}
		d.currentDLI = entity.NewDLIReading(d.greenhouseID, parSensorID, d.targetDLI)
	}

	sunrise, sunset := d.sunCalc.CalculateSunTimes(now)
	d.currentDLI.SunriseTime = sunrise
	d.currentDLI.SunsetTime = sunset
	d.currentDLI.CalculateProjectedDLI()

	active, err := d.planRepo.FindActive(d.greenhouseID)
	if err == nil && active != nil && active.IsActive {
		d.activePlan = active
		d.logger.Infof("Resumed active supplement plan: deficit=%.2f mol/m²", active.Deficit)
	}
}

func (d *DLICoordinator) collectPARData() {
	defer d.wg.Done()

	if d.readingChan == nil {
		d.logger.Warn("DLI coordinator: reading channel not set, PAR collection disabled")
		return
	}

	for {
		select {
		case <-d.stopChan:
			d.logger.Info("PAR data collector stopped")
			return
		case <-d.ctx.Done():
			d.logger.Info("PAR data collector context cancelled")
			return
		case reading, ok := <-d.readingChan:
			if !ok {
				d.logger.Info("PAR reading channel closed")
				return
			}
			if reading != nil && reading.HasPARData() {
				d.processPARSample(reading)
			}
		}
	}
}

func (d *DLICoordinator) processPARSample(reading *entity.SensorReading) {
	d.mu.Lock()
	d.latestPAR[reading.SensorID] = reading.PAR
	d.mu.Unlock()

	avgPAR := d.calculateAveragePAR()
	if avgPAR <= 0 {
		return
	}

	d.mu.Lock()
	if d.currentDLI == nil {
		d.initializeDailyDLI()
	}
	d.currentDLI.AddPARSample(avgPAR, reading.Timestamp)
	d.mu.Unlock()

	atomic.AddUint64(&d.dliSamples, 1)
}

func (d *DLICoordinator) calculateAveragePAR() float64 {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if len(d.latestPAR) == 0 {
		return 0
	}

	total := 0.0
	count := 0
	validCutoff := time.Now().Add(-30 * time.Second)

	for sensorID, par := range d.latestPAR {
		if par > 0 {
			total += par
			count++
		}
		_ = sensorID
		_ = validCutoff
	}

	if count == 0 {
		return 0
	}
	return total / float64(count)
}

func (d *DLICoordinator) evaluateDLIRules() {
	defer d.wg.Done()

	ticker := time.NewTicker(DefaultDLIEvalInterval)
	defer ticker.Stop()

	dayTicker := time.NewTicker(1 * time.Hour)
	defer dayTicker.Stop()

	for {
		select {
		case <-d.stopChan:
			d.logger.Info("DLI rule evaluator stopped")
			return
		case <-d.ctx.Done():
			d.logger.Info("DLI rule evaluator context cancelled")
			return
		case <-ticker.C:
			d.checkDLISupplement()
		case <-dayTicker.C:
			d.checkDayRollover()
		}
	}
}

func (d *DLICoordinator) checkDLISupplement() {
	d.mu.RLock()
	if d.currentDLI == nil {
		d.mu.RUnlock()
		return
	}

	d.currentDLI.CalculateProjectedDLI()

	currentDLI := d.currentDLI.AccumulatedDLI
	projectedDLI := d.currentDLI.ProjectedDLI
	deficit := d.currentDLI.Deficit
	isNearSunset := d.currentDLI.IsNearSunset(DefaultSupplementCutoff)
	target := d.currentDLI.TargetDLI
	sunset := d.currentDLI.SunsetTime
	d.mu.RUnlock()

	remaining := time.Until(sunset)

	d.logger.Debugf("DLI check: current=%.3f, projected=%.3f, deficit=%.3f, target=%.1f, near_sunset=%v, remaining=%v",
		currentDLI, projectedDLI, deficit, target, isNearSunset, remaining)

	d.mu.Lock()
	hasActivePlan := d.activePlan != nil && d.activePlan.IsActive
	d.mu.Unlock()

	if isNearSunset && deficit > 0 && !hasActivePlan {
		d.logger.Infof("DLI threshold check: current=%.3f < target=%.1f, deficit=%.3f mol/m², sunset in %v — starting supplement",
			currentDLI, target, deficit, remaining)
		d.startLightSupplement()
	}

	if hasActivePlan && deficit <= 0 {
		d.logger.Infof("DLI target reached (%.3f >= %.1f), stopping supplement", currentDLI, target)
		d.stopLightSupplement()
	}

	if err := d.dliRepo.Save(d.currentDLI); err != nil {
		d.logger.Warnf("Failed to persist DLI reading: %v", err)
	}
}

func (d *DLICoordinator) startLightSupplement() {
	d.mu.Lock()
	if d.activePlan != nil && d.activePlan.IsActive {
		d.mu.Unlock()
		return
	}
	d.mu.Unlock()

	devices, err := d.ledRepo.FindAll()
	if err != nil || len(devices) == 0 {
		d.logger.Errorf("No LED devices available for supplement: %v", err)
		return
	}

	d.mu.RLock()
	plan := entity.NewLightSupplementPlan(d.greenhouseID, d.currentDLI, devices)
	d.mu.RUnlock()

	if err := d.planRepo.Save(plan); err != nil {
		d.logger.Errorf("Failed to save supplement plan: %v", err)
		return
	}

	d.mu.Lock()
	d.activePlan = plan
	if d.currentDLI != nil {
		d.currentDLI.IsSupplementing = true
	}
	d.mu.Unlock()

	targetPowerPercent := d.calculateRequiredPowerPercent(plan)
	go d.executeSmoothSupplement(plan, targetPowerPercent)

	d.logger.Infof("Light supplement plan started: deficit=%.3f mol/m², target_power=%d%%, devices=%d",
		plan.Deficit, targetPowerPercent, len(devices))
}

func (d *DLICoordinator) calculateRequiredPowerPercent(plan *entity.LightSupplementPlan) uint8 {
	devices := plan.Devices
	totalMaxPower := 0.0
	for _, dev := range devices {
		totalMaxPower += dev.MaxPower
	}

	if totalMaxPower <= 0 {
		return 100
	}

	requiredPAR := plan.RequiredPower
	estimatedPARPerWatt := 2.0
	requiredWatts := requiredPAR / estimatedPARPerWatt

	percent := (requiredWatts / totalMaxPower) * 100
	if percent < 10 {
		percent = 10
	}
	if percent > 100 {
		percent = 100
	}
	return uint8(percent)
}

func (d *DLICoordinator) executeSmoothSupplement(plan *entity.LightSupplementPlan, targetPower uint8) {
	ctx, cancel := context.WithCancel(d.ctx)
	defer cancel()

	atomic.AddUint64(&d.supplementOps, 1)

	err := d.ledController.ApplySmoothStep(ctx, plan.Devices, targetPower, DefaultSmoothSteps, DefaultStepDuration)
	if err != nil {
		if err != context.Canceled {
			d.logger.Errorf("Smooth supplement execution failed: %v", err)
		}
	}
}

func (d *DLICoordinator) stopLightSupplement() {
	d.mu.Lock()
	if d.activePlan != nil {
		d.activePlan.Complete()
		if err := d.planRepo.Update(d.activePlan); err != nil {
			d.logger.Warnf("Failed to update completed plan: %v", err)
		}
		d.activePlan = nil
	}
	if d.currentDLI != nil {
		d.currentDLI.IsSupplementing = false
	}
	d.mu.Unlock()

	go func() {
		ctx, cancel := context.WithTimeout(d.ctx, 30*time.Second)
		defer cancel()
		if err := d.ledController.StopAll(ctx); err != nil {
			d.logger.Warnf("Failed to stop all LEDs: %v", err)
		}
	}()

	d.logger.Info("Light supplement stopped")
}

func (d *DLICoordinator) checkDayRollover() {
	d.mu.Lock()
	if d.currentDLI == nil {
		d.mu.Unlock()
		return
	}

	now := time.Now()
	today := now.Truncate(24 * time.Hour)
	recordDay := d.currentDLI.Date.Truncate(24 * time.Hour)

	if !today.Equal(recordDay) {
		oldDLI := d.currentDLI

		sunrise, sunset := d.sunCalc.CalculateSunTimes(now)
		parSensorID := uint16(0)
		if len(d.pSensorIDs) > 0 {
			parSensorID = d.pSensorIDs[0]
		}
		newDLI := entity.NewDLIReading(d.greenhouseID, parSensorID, d.targetDLI)
		newDLI.SunriseTime = sunrise
		newDLI.SunsetTime = sunset

		d.currentDLI = newDLI

		if d.activePlan != nil && d.activePlan.IsActive {
			d.activePlan.Complete()
			_ = d.planRepo.Update(d.activePlan)
			d.activePlan = nil
		}

		d.logger.Infof("Day rollover: previous DLI=%.3f mol/m², new day initialized", oldDLI.AccumulatedDLI)
	}
	d.mu.Unlock()
}

func (d *DLICoordinator) Stop() {
	d.logger.Info("Stopping DLI coordinator")

	d.mu.Lock()
	if !d.isRunning {
		d.mu.Unlock()
		return
	}
	d.isRunning = false
	d.mu.Unlock()

	d.cancel()
	close(d.stopChan)

	d.ledController.Stop()

	d.wg.Wait()

	d.logger.Infof("DLI coordinator stopped. Samples=%d, Supplement ops=%d",
		atomic.LoadUint64(&d.dliSamples),
		atomic.LoadUint64(&d.supplementOps))
}

func (d *DLICoordinator) GetCurrentDLI() *entity.DLIReading {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.currentDLI == nil {
		return nil
	}
	result := *d.currentDLI
	return &result
}

func (d *DLICoordinator) GetActivePlan() *entity.LightSupplementPlan {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.activePlan == nil {
		return nil
	}
	result := *d.activePlan
	return &result
}

func (d *DLICoordinator) GetStats() map[string]interface{} {
	d.mu.RLock()
	defer d.mu.RUnlock()

	stats := map[string]interface{}{
		"samples":        atomic.LoadUint64(&d.dliSamples),
		"supplement_ops": atomic.LoadUint64(&d.supplementOps),
		"greenhouse_id":  d.greenhouseID,
		"target_dli":     d.targetDLI,
		"par_sensors":    len(d.pSensorIDs),
		"is_running":     d.isRunning,
	}

	if d.currentDLI != nil {
		stats["current_dli"] = d.currentDLI.AccumulatedDLI
		stats["projected_dli"] = d.currentDLI.ProjectedDLI
		stats["deficit"] = d.currentDLI.Deficit
		stats["is_supplementing"] = d.currentDLI.IsSupplementing
		stats["sunrise"] = d.currentDLI.SunriseTime
		stats["sunset"] = d.currentDLI.SunsetTime
	}

	if d.activePlan != nil {
		stats["active_plan_id"] = d.activePlan.ID
		stats["plan_deficit"] = d.activePlan.Deficit
		stats["plan_required_power"] = d.activePlan.RequiredPower
	}

	return stats
}

func (d *DLICoordinator) ManualStartSupplement(targetPower uint8) error {
	d.mu.Lock()
	if d.activePlan != nil && d.activePlan.IsActive {
		d.mu.Unlock()
		return nil
	}
	d.mu.Unlock()

	d.startLightSupplement()
	return nil
}

func (d *DLICoordinator) ManualStopSupplement() error {
	d.stopLightSupplement()
	return nil
}

func (d *DLICoordinator) GetSunCalculator() *SunCalculator {
	return d.sunCalc
}
