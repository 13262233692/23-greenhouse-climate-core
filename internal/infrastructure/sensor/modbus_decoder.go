package sensor

import (
	"encoding/binary"
	"errors"
	"greenhouse-climate-core/internal/domain/entity"
	"math"
	"time"
)

type ModbusFunctionCode uint8

const (
	ModbusFunctionReadHoldingRegisters ModbusFunctionCode = 0x03
	ModbusFunctionReadInputRegisters   ModbusFunctionCode = 0x04
)

type ModbusRTUFrame struct {
	SlaveID    uint8
	Function   ModbusFunctionCode
	StartAddr  uint16
	Data       []byte
	CRC        uint16
	Timestamp  time.Time
}

type ModbusTCPFrame struct {
	TransactionID uint16
	ProtocolID  uint16
	Length      uint16
	SlaveID     uint8
	Function    ModbusFunctionCode
	Data        []byte
	DataLen     uint8
	Payload     []byte
}

type ModbusDecoder struct{}

func NewModbusDecoder() *ModbusDecoder {
	return &ModbusDecoder{}
}

func (d *ModbusDecoder) CalculateCRC(data []byte) uint16 {
	var crc uint16 = 0xFFFF
	for _, b := range data {
		crc ^= uint16(b)
		for i := 0; i < 8; i++ {
			if crc&0x0001 != 0 {
				crc = (crc >> 1) ^ 0xA001
			} else {
				crc >>= 1
			}
		}
	}
	return crc
}

func (d *ModbusDecoder) VerifyCRC(frame []byte) bool {
	if len(frame) < 5 {
		return false
	}
	crc := binary.LittleEndian.Uint16(frame[len(frame)-2:])
	calculatedCRC := d.CalculateCRC(frame[:len(frame)-2])
	return crc == calculatedCRC
}

func (d *ModbusDecoder) BuildReadRequest(slaveID uint8, startAddr, quantity uint16) []byte {
	frame := make([]byte, 6)
	frame[0] = slaveID
	frame[1] = byte(ModbusFunctionReadHoldingRegisters)
	binary.BigEndian.PutUint16(frame[2:4], startAddr)
	binary.BigEndian.PutUint16(frame[4:6], quantity)
	crc := d.CalculateCRC(frame)
	request := make([]byte, 8)
	copy(request[:6], frame)
	binary.LittleEndian.PutUint16(request[6:8], crc)
	return request
}

func (d *ModbusDecoder) ParseRTUResponse(data []byte) (*ModbusRTUFrame, error) {
	if len(data) < 5 {
		return nil, errors.New("invalid response too short")
	}
	if !d.VerifyCRC(data) {
		return nil, errors.New("CRC verification failed")
	}
	frame := &ModbusRTUFrame{
		SlaveID:   data[0],
		Function:  ModbusFunctionCode(data[1]),
		Timestamp: time.Now(),
	}
	if frame.Function&0x80 != 0 {
		return nil, errors.New("modbus exception")
	}
	dataLen := int(data[2])
	frame.Data = data[3 : 3+dataLen]
	frame.CRC = binary.LittleEndian.Uint16(data[3+dataLen:])
	return frame, nil
}

func (d *ModbusDecoder) DecodeSensorReading(sensor *entity.Sensor, frame *ModbusRTUFrame) (*entity.SensorReading, error) {
	if len(frame.Data) < 4 {
		return nil, errors.New("insufficient data")
	}

	reading := &entity.SensorReading{
		SensorID:  sensor.ID,
		SlaveID:   sensor.SlaveID,
		Type:      sensor.Type,
		Timestamp: frame.Timestamp,
		RawData:   frame.Data,
	}

	switch sensor.Type {
	case entity.SensorTypeLeafTempHumidity:
		leafTempRaw := binary.BigEndian.Uint16(frame.Data[0:2])
		leafHumidityRaw := binary.BigEndian.Uint16(frame.Data[2:4])
		reading.LeafTemp = float64(leafTempRaw) / 10.0
		reading.LeafHumidity = float64(leafHumidityRaw) / 10.0
		if len(frame.Data) >= 8 {
			airTempRaw := binary.BigEndian.Uint16(frame.Data[4:6])
			airHumidityRaw := binary.BigEndian.Uint16(frame.Data[6:8])
			reading.AirTemp = float64(airTempRaw) / 10.0
			reading.AirHumidity = float64(airHumidityRaw) / 10.0
		}

	case entity.SensorTypeAirTempHumidity:
		airTempRaw := binary.BigEndian.Uint16(frame.Data[0:2])
		airHumidityRaw := binary.BigEndian.Uint16(frame.Data[2:4])
		reading.AirTemp = float64(airTempRaw) / 10.0
		reading.AirHumidity = float64(airHumidityRaw) / 10.0

	case entity.SensorTypePAR:
		parRaw := binary.BigEndian.Uint16(frame.Data[0:2])
		reading.PAR = float64(parRaw)

	case entity.SensorTypeCO2:
		co2Raw := binary.BigEndian.Uint16(frame.Data[0:2])
		reading.CO2 = float64(co2Raw)
	}

	return reading, nil
}

func (d *ModbusDecoder) parseFloat32(data []byte) float32 {
	bits := binary.BigEndian.Uint32(data)
	return math.Float32frombits(bits)
}
