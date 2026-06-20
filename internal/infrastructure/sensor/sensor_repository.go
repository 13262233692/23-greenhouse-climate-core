package sensor

import (
	"errors"
	"greenhouse-climate-core/internal/domain/entity"
	"greenhouse-climate-core/internal/domain/repository"
	"sync"
	"time"
)

type InMemorySensorRepository struct {
	sensors map[uint16]*entity.Sensor
	mu      sync.RWMutex
}

type InMemorySensorReadingRepository struct {
	readings map[uint16][]*entity.SensorReading
	mu       sync.RWMutex
	maxSize  int
}

func NewInMemorySensorRepository() repository.SensorRepository {
	return &InMemorySensorRepository{
		sensors: make(map[uint16]*entity.Sensor),
	}
}

func (r *InMemorySensorRepository) FindAll() ([]*entity.Sensor, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*entity.Sensor, 0, len(r.sensors))
	for _, s := range r.sensors {
		result = append(result, s)
	}
	return result, nil
}

func (r *InMemorySensorRepository) FindByID(id uint16) (*entity.Sensor, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	s, exists := r.sensors[id]
	if !exists {
		return nil, errors.New("sensor not found")
	}
	return s, nil
}

func (r *InMemorySensorRepository) FindByType(sensorType entity.SensorType) ([]*entity.Sensor, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []*entity.Sensor
	for _, s := range r.sensors {
		if s.Type == sensorType {
			result = append(result, s)
		}
	}
	return result, nil
}

func (r *InMemorySensorRepository) Save(sensor *entity.Sensor) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.sensors[sensor.ID] = sensor
	return nil
}

func NewInMemorySensorReadingRepository(maxSize int) repository.SensorReadingRepository {
	return &InMemorySensorReadingRepository{
		readings: make(map[uint16][]*entity.SensorReading),
		maxSize:  maxSize,
	}
}

func (r *InMemorySensorReadingRepository) Save(reading *entity.SensorReading) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.readings[reading.SensorID] = append(r.readings[reading.SensorID], reading)
	if len(r.readings[reading.SensorID]) > r.maxSize {
		r.readings[reading.SensorID] = r.readings[reading.SensorID][1:]
	}
	return nil
}

func (r *InMemorySensorReadingRepository) FindBySensorID(sensorID uint16, from, to time.Time) ([]*entity.SensorReading, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	readings, exists := r.readings[sensorID]
	if !exists {
		return nil, nil
	}

	var result []*entity.SensorReading
	for _, reading := range readings {
		if (reading.Timestamp.Equal(from) || reading.Timestamp.After(from)) &&
			(reading.Timestamp.Equal(to) || reading.Timestamp.Before(to)) {
			result = append(result, reading)
		}
	}
	return result, nil
}

func (r *InMemorySensorReadingRepository) FindLatestBySensorID(sensorID uint16) (*entity.SensorReading, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	readings, exists := r.readings[sensorID]
	if !exists || len(readings) == 0 {
		return nil, nil
	}
	return readings[len(readings)-1], nil
}
