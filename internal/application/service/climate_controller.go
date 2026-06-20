package service

import (
	"context"
	"greenhouse-climate-core/internal/domain/entity"
	"greenhouse-climate-core/internal/domain/repository"
	"greenhouse-climate-core/internal/infrastructure/plc"
	"greenhouse-climate-core/internal/infrastructure/sensor"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	DefaultConsumerWorkers   = 8
	DefaultVPDWorkers       = 4
	DefaultChannelBuffer     = 10000
	DefaultProcessingTimeout = 5 * time.Second
)

type ClimateController struct {
	sensorClient     *sensor.TCPSensorClient
	sensorRepo       repository.SensorRepository
	readingRepo      repository.SensorReadingRepository
	vpdRepo          repository.VPDRepository
	plcRepo          repository.PLCCommandRepository
	tsRepo           repository.TimeSeriesRepository
	vpdCalculator    *VPDCalculatorService
	ruleEngine       *VPDRuleEngineService
	commandGenerator *PLCCommandGeneratorService
	plcClient        *plc.PLCClient
	dliCoordinator   *DLICoordinator
	logger           *logrus.Logger
	greenhouseID     string
	readingChan      <-chan *entity.SensorReading
	dliReadingChan   chan *entity.SensorReading
	latestReadings   map[uint16]*entity.SensorReading
	stopChan         chan struct{}
	wg               sync.WaitGroup
	mu               sync.RWMutex
	ctx              context.Context
	cancel           context.CancelFunc
	consumerPool     chan struct{}
	vpdPool          chan struct{}
	processedCount   uint64
	vpdCount         uint64
	droppedCount     uint64
	commandCount     uint64
}

func NewClimateController(
	sensorClient *sensor.TCPSensorClient,
	sensorRepo repository.SensorRepository,
	readingRepo repository.SensorReadingRepository,
	vpdRepo repository.VPDRepository,
	plcRepo repository.PLCCommandRepository,
	tsRepo repository.TimeSeriesRepository,
	vpdCalculator *VPDCalculatorService,
	ruleEngine *VPDRuleEngineService,
	commandGenerator *PLCCommandGeneratorService,
	plcClient *plc.PLCClient,
	dliCoordinator *DLICoordinator,
	logger *logrus.Logger,
	greenhouseID string,
) *ClimateController {
	ctx, cancel := context.WithCancel(context.Background())

	return &ClimateController{
		sensorClient:     sensorClient,
		sensorRepo:       sensorRepo,
		readingRepo:      readingRepo,
		vpdRepo:          vpdRepo,
		plcRepo:          plcRepo,
		tsRepo:           tsRepo,
		vpdCalculator:    vpdCalculator,
		ruleEngine:       ruleEngine,
		commandGenerator: commandGenerator,
		plcClient:        plcClient,
		dliCoordinator:   dliCoordinator,
		logger:           logger,
		greenhouseID:     greenhouseID,
		latestReadings:   make(map[uint16]*entity.SensorReading),
		dliReadingChan:   make(chan *entity.SensorReading, DefaultChannelBuffer),
		stopChan:         make(chan struct{}),
		ctx:              ctx,
		cancel:           cancel,
		consumerPool:     make(chan struct{}, DefaultConsumerWorkers),
		vpdPool:          make(chan struct{}, DefaultVPDWorkers),
	}
}

func (c *ClimateController) Start() error {
	sensors, err := c.sensorRepo.FindAll()
	if err != nil {
		return err
	}

	if err := c.sensorClient.InitializeSensors(sensors); err != nil {
		return err
	}

	if err := c.sensorClient.StartPolling(1 * time.Second); err != nil {
		return err
	}

	c.readingChan = c.sensorClient.GetReadingChannel()

	if c.dliCoordinator != nil {
		c.dliCoordinator.SetReadingChannel(c.dliReadingChan)
		if err := c.dliCoordinator.Start(); err != nil {
			c.logger.Warnf("Failed to start DLI coordinator: %v", err)
		}
	}

	go c.monitorStats()

	c.wg.Add(1)
	go c.processSensorReadings()

	c.wg.Add(1)
	go c.startRuleEvaluation()

	c.plcClient.Start()

	c.logger.Infof("Climate controller started for greenhouse %s with %d consumer workers, %d VPD workers",
		c.greenhouseID, DefaultConsumerWorkers, DefaultVPDWorkers)
	return nil
}

func (c *ClimateController) monitorStats() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopChan:
			return
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			sensorStats := c.sensorClient.GetStats()
			processed := atomic.LoadUint64(&c.processedCount)
			vpdCount := atomic.LoadUint64(&c.vpdCount)
			dropped := atomic.LoadUint64(&c.droppedCount)
			commands := atomic.LoadUint64(&c.commandCount)

			c.logger.Infof("Controller stats: processed=%d, vpd=%d, dropped=%d, commands=%d, channel_len=%d, backpressure=%.2f",
				processed, vpdCount, dropped, commands,
				sensorStats["channel_len"],
				sensorStats["backpressure"])
		}
	}
}

func (c *ClimateController) processSensorReadings() {
	defer c.wg.Done()

	for {
		select {
		case <-c.stopChan:
			c.logger.Info("Sensor reading processor stopped")
			return
		case <-c.ctx.Done():
			c.logger.Info("Sensor reading processor context cancelled")
			return
		case reading, ok := <-c.readingChan:
			if !ok {
				c.logger.Info("Reading channel closed")
				return
			}

			select {
			case c.consumerPool <- struct{}{}:
				c.wg.Add(1)
				go func(r *entity.SensorReading) {
					defer c.wg.Done()
					defer func() { <-c.consumerPool }()
					c.processReading(r)
				}(reading)
			default:
				atomic.AddUint64(&c.droppedCount, 1)
				c.logger.Warnf("Consumer pool full, dropping reading from sensor %d (total dropped: %d)",
					reading.SensorID, atomic.LoadUint64(&c.droppedCount))
			}
		}
	}
}

func (c *ClimateController) processReading(reading *entity.SensorReading) {
	if err := c.ctx.Err(); err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(c.ctx, DefaultProcessingTimeout)
	defer cancel()

	c.mu.Lock()
	c.latestReadings[reading.SensorID] = reading
	c.mu.Unlock()

	if err := c.readingRepo.Save(reading); err != nil {
		c.logger.Warnf("Failed to save sensor reading: %v", err)
	}

	select {
	case <-ctx.Done():
		c.logger.Debug("Timeseries write cancelled")
		return
	default:
	}

	if err := c.tsRepo.WriteSensorReading(reading); err != nil {
		c.logger.Warnf("Failed to write sensor reading to timeseries: %v", err)
	}

	atomic.AddUint64(&c.processedCount, 1)

	if c.dliCoordinator != nil && reading.HasPARData() {
		select {
		case c.dliReadingChan <- reading:
		default:
			c.logger.Tracef("DLI channel full, dropping PAR sample from sensor %d", reading.SensorID)
		}
	}

	if reading.HasLeafData() {
		select {
		case c.vpdPool <- struct{}{}:
			c.wg.Add(1)
			go func(r *entity.SensorReading) {
				defer c.wg.Done()
				defer func() { <-c.vpdPool }()
				c.processVPDCalculation(ctx, r)
			}(reading)
		default:
			c.logger.Tracef("VPD pool full, skipping VPD calculation for sensor %d", reading.SensorID)
		}
	}
}

func (c *ClimateController) processVPDCalculation(ctx context.Context, leafReading *entity.SensorReading) {
	if err := ctx.Err(); err != nil {
		return
	}

	c.mu.RLock()
	airReading, hasAirData := c.findLatestAirReading()
	c.mu.RUnlock()

	if !hasAirData {
		return
	}

	vpd, err := c.vpdCalculator.CalculateFromReadings(
		c.greenhouseID,
		leafReading.SensorID,
		leafReading,
		airReading,
	)
	if err != nil {
		c.logger.Errorf("VPD calculation failed: %v", err)
		return
	}
	if vpd == nil {
		return
	}

	if err := c.vpdRepo.Save(vpd); err != nil {
		c.logger.Warnf("Failed to save VPD reading: %v", err)
	}

	select {
	case <-ctx.Done():
		return
	default:
	}

	if err := c.tsRepo.WriteVPDReading(vpd); err != nil {
		c.logger.Warnf("Failed to write VPD to timeseries: %v", err)
	}

	atomic.AddUint64(&c.vpdCount, 1)

	c.logger.Debugf("VPD calculated: sensor=%d, actual=%.4f kPa, status=%s",
		leafReading.SensorID, vpd.ActualVPD, vpd.Status)
}

func (c *ClimateController) findLatestAirReading() (*entity.SensorReading, bool) {
	for _, r := range c.latestReadings {
		if r.HasAirData() && time.Since(r.Timestamp) < 5*time.Second {
			return r, true
		}
	}
	return nil, false
}

func (c *ClimateController) startRuleEvaluation() {
	defer c.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopChan:
			c.logger.Info("Rule evaluator stopped")
			return
		case <-c.ctx.Done():
			c.logger.Info("Rule evaluator context cancelled")
			return
		case <-ticker.C:
			c.evaluateRules()
		}
	}
}

func (c *ClimateController) evaluateRules() {
	evalCtx, evalCancel := context.WithTimeout(c.ctx, 10*time.Second)
	defer evalCancel()

	shouldTrigger, vpd, deviation, err := c.ruleEngine.EvaluateGreenhouse(c.greenhouseID)
	if err != nil {
		c.logger.Errorf("Rule evaluation failed: %v", err)
		return
	}
	if vpd == nil {
		return
	}

	c.logger.Debugf("Rule evaluation: vpd=%.4f, deviation=%v, shouldTrigger=%v",
		vpd.ActualVPD, deviation, shouldTrigger)

	select {
	case <-evalCtx.Done():
		return
	default:
	}

	if shouldTrigger {
		command := c.commandGenerator.GenerateCommand(vpd)
		if command != nil {
			c.executeCommand(evalCtx, command)
		}
	}
}

func (c *ClimateController) executeCommand(ctx context.Context, command *entity.PLCCommand) {
	if err := ctx.Err(); err != nil {
		return
	}

	if err := c.plcRepo.Save(command); err != nil {
		c.logger.Errorf("Failed to save PLC command: %v", err)
		return
	}

	if err := c.tsRepo.WritePLCCommand(command); err != nil {
		c.logger.Warnf("Failed to write PLC command to timeseries: %v", err)
	}

	c.plcClient.QueueCommand(command)

	atomic.AddUint64(&c.commandCount, 1)

	c.logger.Infof("PLC command executed: type=%s, target=%d, opening=%d%%, duration=%v",
		command.Type, command.TargetDevice, command.OpeningDegree, command.PulseDuration)
}

func (c *ClimateController) Stop() {
	c.logger.Info("Stopping climate controller")

	c.cancel()

	close(c.stopChan)

	c.sensorClient.Stop()

	c.plcClient.Stop()

	if c.dliCoordinator != nil {
		c.dliCoordinator.Stop()
	}

	c.wg.Wait()

	stats := c.GetStats()
	c.logger.Infof("Climate controller stopped. Stats: %+v", stats)
}

func (c *ClimateController) GetLatestReading(sensorID uint16) *entity.SensorReading {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.latestReadings[sensorID]
}

func (c *ClimateController) GetAllLatestReadings() map[uint16]*entity.SensorReading {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make(map[uint16]*entity.SensorReading)
	for k, v := range c.latestReadings {
		result[k] = v
	}
	return result
}

func (c *ClimateController) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"processed":        atomic.LoadUint64(&c.processedCount),
		"vpd_calculated":   atomic.LoadUint64(&c.vpdCount),
		"dropped":          atomic.LoadUint64(&c.droppedCount),
		"commands_issued":  atomic.LoadUint64(&c.commandCount),
		"consumer_workers": DefaultConsumerWorkers,
		"vpd_workers":      DefaultVPDWorkers,
		"greenhouse_id":    c.greenhouseID,
	}
}

func (c *ClimateController) ManualMistCooling(duration time.Duration) *entity.PLCCommand {
	command := entity.NewMistCoolingCommand(c.greenhouseID, 0, duration)
	c.executeCommand(c.ctx, command)
	return command
}

func (c *ClimateController) ManualCO2Control(openingDegree uint8) *entity.PLCCommand {
	command := entity.NewCO2ControlCommand(c.greenhouseID, 0, openingDegree)
	c.executeCommand(c.ctx, command)
	return command
}

func (c *ClimateController) StopDevice(deviceID uint8) *entity.PLCCommand {
	command := entity.NewStopCommand(c.greenhouseID, deviceID)
	c.executeCommand(c.ctx, command)
	return command
}

func (c *ClimateController) GetGreenhouseID() string {
	return c.greenhouseID
}

func (c *ClimateController) GetVPDCalculator() *VPDCalculatorService {
	return c.vpdCalculator
}

func (c *ClimateController) GetRuleEngine() *VPDRuleEngineService {
	return c.ruleEngine
}

func (c *ClimateController) GetCommandGenerator() *PLCCommandGeneratorService {
	return c.commandGenerator
}

func (c *ClimateController) GetDLICoordinator() *DLICoordinator {
	return c.dliCoordinator
}
