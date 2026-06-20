package sensor

import (
	"fmt"
	"greenhouse-climate-core/internal/domain/entity"
)

type SensorFactory struct{}

func NewSensorFactory() *SensorFactory {
	return &SensorFactory{}
}

func (f *SensorFactory) CreateGreenhouseSensors(greenhouseID string, basePort int) []*entity.Sensor {
	var sensors []*entity.Sensor
	var id uint16 = 1

	sensorConfigs := []struct {
		count      int
		sensorType entity.SensorType
		reg        uint16
		dataLen    uint16
		namePrefix string
	}{
		{count: 20, sensorType: entity.SensorTypeLeafTempHumidity, reg: 0x0000, dataLen: 4, namePrefix: "Leaf_Temp_Hum"},
		{count: 15, sensorType: entity.SensorTypeAirTempHumidity, reg: 0x0000, dataLen: 2, namePrefix: "Air_Temp_Hum"},
		{count: 10, sensorType: entity.SensorTypePAR, reg: 0x0002, dataLen: 1, namePrefix: "PAR"},
		{count: 5, sensorType: entity.SensorTypeCO2, reg: 0x0003, dataLen: 1, namePrefix: "CO2"},
	}

	for _, cfg := range sensorConfigs {
		for i := 0; i < cfg.count; i++ {
			sensor := entity.NewSensor(
				id,
				uint8(id%247 + 1),
				cfg.sensorType,
				fmt.Sprintf("%s_%s_%02d", greenhouseID, cfg.namePrefix, i+1),
				"127.0.0.1",
				basePort + int(id),
				cfg.reg,
				cfg.dataLen,
			)
			sensors = append(sensors, sensor)
			id++
		}
	}

	return sensors
}

func (f *SensorFactory) Create50Sensors() []*entity.Sensor {
	return f.CreateGreenhouseSensors("GH-001", 5000)
}
