package service

import (
	"greenhouse-climate-core/internal/domain/entity"
	"greenhouse-climate-core/internal/domain/repository"
	"greenhouse-climate-core/internal/domain/service"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type VPDCalculatorService struct {
	vpdRepo     repository.VPDRepository
	readingRepo repository.SensorReadingRepository
	tsRepo      repository.TimeSeriesRepository
	logger      *logrus.Logger
	latestVPD   map[string]*entity.VPDReading
	mu          sync.RWMutex
}

func NewVPDCalculatorService(
	vpdRepo repository.VPDRepository,
	readingRepo repository.SensorReadingRepository,
	tsRepo repository.TimeSeriesRepository,
	logger *logrus.Logger,
) service.VPDCalculator {
	return &VPDCalculatorService{
		vpdRepo:     vpdRepo,
		readingRepo: readingRepo,
		tsRepo:      tsRepo,
		logger:      logger,
		latestVPD:   make(map[string]*entity.VPDReading),
	}
}

func (s *VPDCalculatorService) Calculate(airTemp, airHumidity, leafTemp float64) *entity.VPDReading {
	vpd := &entity.VPDReading{
		AirTemp:     airTemp,
		AirHumidity: airHumidity,
		LeafTemp:    leafTemp,
		Timestamp:   time.Now(),
	}
	vpd.Calculate()
	return vpd
}

func (s *VPDCalculatorService) CalculateFromReadings(
	greenhouseID string,
	sensorID uint16,
	leafReading *entity.SensorReading,
	airReading *entity.SensorReading,
) (*entity.VPDReading, error) {
	airTemp := airReading.AirTemp
	airHumidity := airReading.AirHumidity
	leafTemp := leafReading.LeafTemp

	if airTemp == 0 || airHumidity == 0 || leafTemp == 0 {
		return nil, nil
	}

	vpd := s.Calculate(airTemp, airHumidity, leafTemp)
	vpd.GreenhouseID = greenhouseID
	vpd.SensorID = sensorID

	if err := s.vpdRepo.Save(vpd); err != nil {
		return nil, err
	}

	if err := s.tsRepo.WriteVPDReading(vpd); err != nil {
		s.logger.Warnf("Failed to write VPD to timeseries: %v", err)
	}

	s.mu.Lock()
	s.latestVPD[greenhouseID] = vpd
	s.mu.Unlock()

	s.logger.Debugf("VPD calculated: greenhouse=%s, actual=%.4f kPa, status=%s",
		greenhouseID, vpd.ActualVPD, vpd.Status)

	return vpd, nil
}

func (s *VPDCalculatorService) GetLatestVPD(greenhouseID string) *entity.VPDReading {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if vpd, exists := s.latestVPD[greenhouseID]; exists {
		return vpd
	}

	vpd, _ := s.vpdRepo.FindLatestByGreenhouseID(greenhouseID)
	return vpd
}

func (s *VPDCalculatorService) GetVPDHistory(greenhouseID string, duration time.Duration) ([]*entity.VPDReading, error) {
	return s.vpdRepo.FindDeviationHistory(greenhouseID, duration)
}
