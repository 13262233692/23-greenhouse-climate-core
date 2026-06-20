package service

import (
	"greenhouse-climate-core/internal/domain/entity"
	"greenhouse-climate-core/internal/domain/repository"
	"greenhouse-climate-core/internal/infrastructure/plc"
	"greenhouse-climate-core/internal/infrastructure/sensor"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
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
	logger           *logrus.Logger
	greenhouseID     string
	readingChan      <-chan *entity.SensorReading
	latestReadings   map[uint16]*entity.SensorReading
	stopChan         chan struct{}
	wg               sync.WaitGroup
	mu               sync.RWMutex
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
	logger *logrus.Logger,
	greenhouseID string,
) *ClimateController {
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
		logger:           logger,
		greenhouseID:     greenhouseID,
		latestReadings:   make(map[uint16]*entity.SensorReading),
		stopChan:         make(chan struct{}),
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

	c.wg.Add(2)
	go c.processSensorReadings()
	go c.startRuleEvaluation()

	c.plcClient.Start()

	c.logger.Infof("Climate controller started for greenhouse %s", c.greenhouseID)
	return nil
}

func (c *ClimateController) processSensorReadings() {
	defer c.wg.Done()

	for reading := range c.readingChan {
		c.mu.Lock()
		c.latestReadings[reading.SensorID] = reading
		c.mu.Unlock()

		if err := c.readingRepo.Save(reading); err != nil {
			c.logger.Warnf("Failed to save sensor reading: %v", err)
		}

		if err := c.tsRepo.WriteSensorReading(reading); err != nil {
			c.logger.Warnf("Failed to write sensor reading to timeseries: %v", err)
		}

		if reading.HasLeafData() {
			go c.processVPDCalculation(reading)
		}
	}
	c.logger.Info("Sensor reading processor stopped")
}

func (c *ClimateController) processVPDCalculation(leafReading *entity.SensorReading) {
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
		case <-ticker.C:
			c.evaluateRules()
		}
	}
}

func (c *ClimateController) evaluateRules() {
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

	if shouldTrigger {
		command := c.commandGenerator.GenerateCommand(vpd)
		if command != nil {
			c.executeCommand(command)
		}
	}
}

func (c *ClimateController) executeCommand(command *entity.PLCCommand) {
	if err := c.plcRepo.Save(command); err != nil {
		c.logger.Errorf("Failed to save PLC command: %v", err)
		return
	}

	if err := c.tsRepo.WritePLCCommand(command); err != nil {
		c.logger.Warnf("Failed to write PLC command to timeseries: %v", err)
	}

	c.plcClient.QueueCommand(command)

	c.logger.Infof("PLC command executed: type=%s, target=%d, opening=%d%%, duration=%v",
		command.Type, command.TargetDevice, command.OpeningDegree, command.PulseDuration)
}

func (c *ClimateController) Stop() {
	c.logger.Info("Stopping climate controller")
	close(c.stopChan)
	c.sensorClient.Stop()
	c.plcClient.Stop()
	c.wg.Wait()
	c.logger.Info("Climate controller stopped")
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

func (c *ClimateController) ManualMistCooling(duration time.Duration) *entity.PLCCommand {
	command := entity.NewMistCoolingCommand(c.greenhouseID, 0, duration)
	c.executeCommand(command)
	return command
}

func (c *ClimateController) ManualCO2Control(openingDegree uint8) *entity.PLCCommand {
	command := entity.NewCO2ControlCommand(c.greenhouseID, 0, openingDegree)
	c.executeCommand(command)
	return command
}

func (c *ClimateController) StopDevice(deviceID uint8) *entity.PLCCommand {
	command := entity.NewStopCommand(c.greenhouseID, deviceID)
	c.executeCommand(command)
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
