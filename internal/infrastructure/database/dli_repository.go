package database

import (
	"greenhouse-climate-core/internal/domain/entity"
	"greenhouse-climate-core/internal/domain/repository"
	"sync"
	"time"
)

type InMemoryDLIRepository struct {
	readings  map[string]map[time.Time]*entity.DLIReading
	idCounter uint64
	mu        sync.RWMutex
}

type InMemoryLightPlanRepository struct {
	plans     map[string]*entity.LightSupplementPlan
	history   map[string][]*entity.LightSupplementPlan
	idCounter uint64
	mu        sync.RWMutex
}

type InMemoryLEDDeviceRepository struct {
	devices   map[uint8]*entity.LEDLightDevice
	mu        sync.RWMutex
}

func NewInMemoryDLIRepository() repository.DLIRepository {
	return &InMemoryDLIRepository{
		readings: make(map[string]map[time.Time]*entity.DLIReading),
	}
}

func (r *InMemoryDLIRepository) Save(dli *entity.DLIReading) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if dli.ID == 0 {
		r.idCounter++
		dli.ID = r.idCounter
	}

	dateKey := dli.Date.Truncate(24 * time.Hour)
	if _, exists := r.readings[dli.GreenhouseID]; !exists {
		r.readings[dli.GreenhouseID] = make(map[time.Time]*entity.DLIReading)
	}
	r.readings[dli.GreenhouseID][dateKey] = dli
	return nil
}

func (r *InMemoryDLIRepository) FindByGreenhouseIDAndDate(greenhouseID string, date time.Time) (*entity.DLIReading, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	dateKey := date.Truncate(24 * time.Hour)
	ghReadings, exists := r.readings[greenhouseID]
	if !exists {
		return nil, nil
	}
	return ghReadings[dateKey], nil
}

func (r *InMemoryDLIRepository) FindLatest(greenhouseID string) (*entity.DLIReading, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ghReadings, exists := r.readings[greenhouseID]
	if !exists || len(ghReadings) == 0 {
		return nil, nil
	}

	var latest *entity.DLIReading
	var latestDate time.Time
	for date, reading := range ghReadings {
		if date.After(latestDate) {
			latestDate = date
			latest = reading
		}
	}
	return latest, nil
}

func (r *InMemoryDLIRepository) FindHistory(greenhouseID string, from, to time.Time) ([]*entity.DLIReading, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ghReadings, exists := r.readings[greenhouseID]
	if !exists {
		return nil, nil
	}

	var result []*entity.DLIReading
	for date, reading := range ghReadings {
		if (date.Equal(from) || date.After(from)) && (date.Equal(to) || date.Before(to)) {
			result = append(result, reading)
		}
	}
	return result, nil
}

func NewInMemoryLightPlanRepository() repository.LightSupplementPlanRepository {
	return &InMemoryLightPlanRepository{
		plans:   make(map[string]*entity.LightSupplementPlan),
		history: make(map[string][]*entity.LightSupplementPlan),
	}
}

func (r *InMemoryLightPlanRepository) Save(plan *entity.LightSupplementPlan) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if plan.ID == 0 {
		r.idCounter++
		plan.ID = r.idCounter
	}

	if plan.IsActive {
		r.plans[plan.GreenhouseID] = plan
	}

	r.history[plan.GreenhouseID] = append(r.history[plan.GreenhouseID], plan)
	return nil
}

func (r *InMemoryLightPlanRepository) FindActive(greenhouseID string) (*entity.LightSupplementPlan, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	plan, exists := r.plans[greenhouseID]
	if !exists {
		return nil, nil
	}
	return plan, nil
}

func (r *InMemoryLightPlanRepository) FindHistory(greenhouseID string, from, to time.Time) ([]*entity.LightSupplementPlan, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	history, exists := r.history[greenhouseID]
	if !exists {
		return nil, nil
	}

	var result []*entity.LightSupplementPlan
	for _, plan := range history {
		if (plan.CreatedAt.Equal(from) || plan.CreatedAt.After(from)) &&
			(plan.CreatedAt.Equal(to) || plan.CreatedAt.Before(to)) {
			result = append(result, plan)
		}
	}
	return result, nil
}

func (r *InMemoryLightPlanRepository) Update(plan *entity.LightSupplementPlan) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !plan.IsActive {
		delete(r.plans, plan.GreenhouseID)
	} else {
		r.plans[plan.GreenhouseID] = plan
	}
	return nil
}

func NewInMemoryLEDDeviceRepository() repository.LEDDeviceRepository {
	repo := &InMemoryLEDDeviceRepository{
		devices: make(map[uint8]*entity.LEDLightDevice),
	}
	repo.initDefaultDevices()
	return repo
}

func (r *InMemoryLEDDeviceRepository) initDefaultDevices() {
	devices := []*entity.LEDLightDevice{
		{ID: 1, Name: "LED_Top_Z1", MaxPower: 1000, CurrentPower: 0, IsActive: false, Zone: "Z1"},
		{ID: 2, Name: "LED_Top_Z2", MaxPower: 1000, CurrentPower: 0, IsActive: false, Zone: "Z2"},
		{ID: 3, Name: "LED_Top_Z3", MaxPower: 1000, CurrentPower: 0, IsActive: false, Zone: "Z3"},
		{ID: 4, Name: "LED_Top_Z4", MaxPower: 1000, CurrentPower: 0, IsActive: false, Zone: "Z4"},
		{ID: 5, Name: "LED_Top_Z5", MaxPower: 1000, CurrentPower: 0, IsActive: false, Zone: "Z5"},
	}
	for _, dev := range devices {
		r.devices[dev.ID] = dev
	}
}

func (r *InMemoryLEDDeviceRepository) FindAll() ([]*entity.LEDLightDevice, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*entity.LEDLightDevice, 0, len(r.devices))
	for _, dev := range r.devices {
		result = append(result, dev)
	}
	return result, nil
}

func (r *InMemoryLEDDeviceRepository) FindByZone(zone string) ([]*entity.LEDLightDevice, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []*entity.LEDLightDevice
	for _, dev := range r.devices {
		if dev.Zone == zone {
			result = append(result, dev)
		}
	}
	return result, nil
}

func (r *InMemoryLEDDeviceRepository) Save(device *entity.LEDLightDevice) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.devices[device.ID] = device
	return nil
}

func (r *InMemoryLEDDeviceRepository) Update(device *entity.LEDLightDevice) error {
	return r.Save(device)
}
