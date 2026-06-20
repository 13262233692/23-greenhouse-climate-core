package database

import (
	"greenhouse-climate-core/internal/domain/entity"
	"greenhouse-climate-core/internal/domain/repository"
	"sync"
	"time"
)

type InMemoryVPDRepository struct {
	vpdReadings map[string][]*entity.VPDReading
	mu          sync.RWMutex
	maxSize     int
	idCounter   uint64
}

func NewInMemoryVPDRepository(maxSize int) repository.VPDRepository {
	return &InMemoryVPDRepository{
		vpdReadings: make(map[string][]*entity.VPDReading),
		maxSize:     maxSize,
		idCounter:   1,
	}
}

func (r *InMemoryVPDRepository) Save(vpd *entity.VPDReading) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if vpd.ID == 0 {
		vpd.ID = r.idCounter
		r.idCounter++
	}

	ghID := vpd.GreenhouseID
	r.vpdReadings[ghID] = append(r.vpdReadings[ghID], vpd)
	if len(r.vpdReadings[ghID]) > r.maxSize {
		r.vpdReadings[ghID] = r.vpdReadings[ghID][1:]
	}
	return nil
}

func (r *InMemoryVPDRepository) FindByGreenhouseID(greenhouseID string, from, to time.Time) ([]*entity.VPDReading, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	readings, exists := r.vpdReadings[greenhouseID]
	if !exists {
		return nil, nil
	}

	var result []*entity.VPDReading
	for _, vpd := range readings {
		if (vpd.Timestamp.Equal(from) || vpd.Timestamp.After(from)) &&
			(vpd.Timestamp.Equal(to) || vpd.Timestamp.Before(to)) {
			result = append(result, vpd)
		}
	}
	return result, nil
}

func (r *InMemoryVPDRepository) FindLatestByGreenhouseID(greenhouseID string) (*entity.VPDReading, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	readings, exists := r.vpdReadings[greenhouseID]
	if !exists || len(readings) == 0 {
		return nil, nil
	}
	return readings[len(readings)-1], nil
}

func (r *InMemoryVPDRepository) FindDeviationHistory(greenhouseID string, duration time.Duration) ([]*entity.VPDReading, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	readings, exists := r.vpdReadings[greenhouseID]
	if !exists {
		return nil, nil
	}

	cutoff := time.Now().Add(-duration)
	var result []*entity.VPDReading
	for i := len(readings) - 1; i >= 0; i-- {
		if readings[i].Timestamp.After(cutoff) {
			result = append([]*entity.VPDReading{readings[i]}, result...)
		} else {
			break
		}
	}
	return result, nil
}
