package entity

import "time"

type SensorType string

const (
	SensorTypeLeafTempHumidity  SensorType = "leaf_temp_humidity"
	SensorTypePAR               SensorType = "par"
	SensorTypeCO2               SensorType = "co2"
	SensorTypeAirTempHumidity   SensorType = "air_temp_humidity"
)

type Sensor struct {
	ID          uint16
	SlaveID     uint8
	Type        SensorType
	Name        string
	Description string
	Address     string
	Port        int
	Register    uint16
	DataLength  uint16
}

type SensorReading struct {
	SensorID    uint16
	SlaveID     uint8
	Type        SensorType
	LeafTemp    float64
	LeafHumidity float64
	AirTemp     float64
	AirHumidity float64
	PAR         float64
	CO2         float64
	Timestamp   time.Time
	RawData     []byte
}

func NewSensor(id uint16, slaveID uint8, sensorType SensorType, name, address string, port int, register uint16, dataLength uint16) *Sensor {
	return &Sensor{
		ID:         id,
		SlaveID:    slaveID,
		Type:       sensorType,
		Name:       name,
		Address:    address,
		Port:       port,
		Register:   register,
		DataLength: dataLength,
	}
}

func (s *SensorReading) HasLeafData() bool {
	return s.LeafTemp != 0 || s.LeafHumidity != 0
}

func (s *SensorReading) HasAirData() bool {
	return s.AirTemp != 0 || s.AirHumidity != 0
}

func (s *SensorReading) HasPARData() bool {
	return s.PAR > 0
}

func (s *SensorReading) HasCO2Data() bool {
	return s.CO2 > 0
}
