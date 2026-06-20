package repository

import (
	"greenhouse-climate-core/internal/domain/entity"
	"time"
)

type DLIRepository interface {
	Save(dli *entity.DLIReading) error
	FindByGreenhouseIDAndDate(greenhouseID string, date time.Time) (*entity.DLIReading, error)
	FindLatest(greenhouseID string) (*entity.DLIReading, error)
	FindHistory(greenhouseID string, from, to time.Time) ([]*entity.DLIReading, error)
}

type LightSupplementPlanRepository interface {
	Save(plan *entity.LightSupplementPlan) error
	FindActive(greenhouseID string) (*entity.LightSupplementPlan, error)
	FindHistory(greenhouseID string, from, to time.Time) ([]*entity.LightSupplementPlan, error)
	Update(plan *entity.LightSupplementPlan) error
}

type LEDDeviceRepository interface {
	FindAll() ([]*entity.LEDLightDevice, error)
	FindByZone(zone string) ([]*entity.LEDLightDevice, error)
	Save(device *entity.LEDLightDevice) error
	Update(device *entity.LEDLightDevice) error
}
