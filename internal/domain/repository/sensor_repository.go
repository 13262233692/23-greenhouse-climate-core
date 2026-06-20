package repository

import (
	"greenhouse-climate-core/internal/domain/entity"
	"time"
)

type SensorRepository interface {
	FindAll() ([]*entity.Sensor, error)
	FindByID(id uint16) (*entity.Sensor, error)
	FindByType(sensorType entity.SensorType) ([]*entity.Sensor, error)
	Save(sensor *entity.Sensor) error
}

type SensorReadingRepository interface {
	Save(reading *entity.SensorReading) error
	FindBySensorID(sensorID uint16, from, to time.Time) ([]*entity.SensorReading, error)
	FindLatestBySensorID(sensorID uint16) (*entity.SensorReading, error)
}

type VPDRepository interface {
	Save(vpd *entity.VPDReading) error
	FindByGreenhouseID(greenhouseID string, from, to time.Time) ([]*entity.VPDReading, error)
	FindLatestByGreenhouseID(greenhouseID string) (*entity.VPDReading, error)
	FindDeviationHistory(greenhouseID string, duration time.Duration) ([]*entity.VPDReading, error)
}

type PLCCommandRepository interface {
	Save(command *entity.PLCCommand) error
	FindByID(id uint64) (*entity.PLCCommand, error)
	FindPending() ([]*entity.PLCCommand, error)
	FindByGreenhouseID(greenhouseID string, from, to time.Time) ([]*entity.PLCCommand, error)
	UpdateStatus(command *entity.PLCCommand) error
}

type TimeSeriesRepository interface {
	WriteSensorReading(reading *entity.SensorReading) error
	WriteVPDReading(vpd *entity.VPDReading) error
	WritePLCCommand(command *entity.PLCCommand) error
}
