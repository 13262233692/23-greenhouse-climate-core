package sensor

import (
	"encoding/binary"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type MockSensorServer struct {
	address      string
	port         int
	listener     net.Listener
	logger       *logrus.Logger
	stopChan     chan struct{}
	wg           sync.WaitGroup
	decoder      *ModbusDecoder
	mu           sync.Mutex
	airTemp      float64
	airHumidity  float64
	leafTemp     float64
	leafHumidity float64
	par          float64
	co2          float64
}

func NewMockSensorServer(address string, port int, logger *logrus.Logger) *MockSensorServer {
	return &MockSensorServer{
		address:      address,
		port:         port,
		logger:       logger,
		stopChan:     make(chan struct{}),
		decoder:      NewModbusDecoder(),
		airTemp:      25.0,
		airHumidity:  65.0,
		leafTemp:     26.5,
		leafHumidity:  70.0,
		par:          800.0,
		co2:          600.0,
	}
}

func (s *MockSensorServer) Start() error {
	addr := net.JoinHostPort(s.address, strconv.Itoa(s.port))
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.listener = listener

	s.wg.Add(1)
	go s.acceptLoop()

	s.wg.Add(1)
	go s.simulateEnvironmentalChanges()

	s.logger.Infof("Mock sensor server started on %s", addr)
	return nil
}

func (s *MockSensorServer) acceptLoop() {
	defer s.wg.Done()

	for {
		select {
		case <-s.stopChan:
			return
		default:
			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case <-s.stopChan:
					return
				default:
					s.logger.Warnf("Accept error: %v", err)
					continue
				}
			}
			s.wg.Add(1)
			go s.handleConnection(conn)
		}
	}
}

func (s *MockSensorServer) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	buf := make([]byte, 256)

	for {
		select {
		case <-s.stopChan:
			return
		default:
			conn.SetReadDeadline(time.Now().Add(30 * time.Second))
			n, err := conn.Read(buf)
			if err != nil {
				return
			}

			response := s.handleRequest(buf[:n])
			if response != nil {
				conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
				_, err = conn.Write(response)
				if err != nil {
					return
				}
			}
		}
	}
}

func (s *MockSensorServer) handleRequest(request []byte) []byte {
	if len(request) < 8 {
		return nil
	}

	slaveID := request[0]
	function := request[1]
	startAddr := binary.BigEndian.Uint16(request[2:4])
	quantity := binary.BigEndian.Uint16(request[4:6])

	if function != byte(ModbusFunctionReadHoldingRegisters) {
		return s.buildExceptionResponse(slaveID, function, 0x01)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	registers := make([]uint16, quantity)

	for i := uint16(0); i < quantity; i++ {
		reg := startAddr + i
		switch reg {
		case 0x0000:
			registers[i] = uint16(s.leafTemp * 10)
		case 0x0001:
			registers[i] = uint16(s.leafHumidity * 10)
		case 0x0002:
			registers[i] = uint16(s.airTemp * 10)
		case 0x0003:
			registers[i] = uint16(s.airHumidity * 10)
		case 0x0004:
			registers[i] = uint16(s.par)
		case 0x0005:
			registers[i] = uint16(s.co2)
		default:
			registers[i] = 0
		}
	}

	return s.buildReadResponse(slaveID, function, registers)
}

func (s *MockSensorServer) buildReadResponse(slaveID, function byte, registers []uint16) []byte {
	byteCount := len(registers) * 2
	response := make([]byte, 3+byteCount+2)

	response[0] = slaveID
	response[1] = function
	response[2] = byte(byteCount)

	for i, reg := range registers {
		binary.BigEndian.PutUint16(response[3+i*2:5+i*2], reg)
	}

	crc := s.decoder.CalculateCRC(response[:3+byteCount])
	binary.LittleEndian.PutUint16(response[3+byteCount:], crc)

	return response
}

func (s *MockSensorServer) buildExceptionResponse(slaveID, function, exceptionCode byte) []byte {
	response := make([]byte, 5)
	response[0] = slaveID
	response[1] = function | 0x80
	response[2] = exceptionCode
	crc := s.decoder.CalculateCRC(response[:3])
	binary.LittleEndian.PutUint16(response[3:], crc)
	return response
}

func (s *MockSensorServer) simulateEnvironmentalChanges() {
	defer s.wg.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopChan:
			return
		case <-ticker.C:
			s.mu.Lock()
			s.airTemp += (rand.Float64() - 0.5) * 0.5
			s.airHumidity += (rand.Float64() - 0.5) * 2
			s.leafTemp += (rand.Float64() - 0.5) * 0.3
			s.leafHumidity += (rand.Float64() - 0.5) * 1
			s.par += (rand.Float64() - 0.5) * 50
			s.co2 += (rand.Float64() - 0.5) * 50

			if s.airTemp < 15 {
				s.airTemp = 15
			}
			if s.airTemp > 40 {
				s.airTemp = 40
			}
			if s.airHumidity < 30 {
				s.airHumidity = 30
			}
			if s.airHumidity > 95 {
				s.airHumidity = 95
			}
			if s.leafTemp < 15 {
				s.leafTemp = 15
			}
			if s.leafTemp > 40 {
				s.leafTemp = 40
			}
			if s.leafHumidity < 30 {
				s.leafHumidity = 30
			}
			if s.leafHumidity > 95 {
				s.leafHumidity = 95
			}
			if s.par < 0 {
				s.par = 0
			}
			if s.par > 2000 {
				s.par = 2000
			}
			if s.co2 < 300 {
				s.co2 = 300
			}
			if s.co2 > 1500 {
				s.co2 = 1500
			}
			s.mu.Unlock()
		}
	}
}

func (s *MockSensorServer) SetVPDHigh() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.airTemp = 35.0
	s.airHumidity = 40.0
	s.leafTemp = 38.0
	s.logger.Info("Mock environment set to VPD HIGH condition")
}

func (s *MockSensorServer) SetVPDLow() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.airTemp = 18.0
	s.airHumidity = 90.0
	s.leafTemp = 19.0
	s.logger.Info("Mock environment set to VPD LOW condition")
}

func (s *MockSensorServer) SetVPNormal() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.airTemp = 25.0
	s.airHumidity = 70.0
	s.leafTemp = 26.0
	s.logger.Info("Mock environment set to VPD NORMAL condition")
}

func (s *MockSensorServer) GetAirTemp() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.airTemp
}

func (s *MockSensorServer) GetAirHumidity() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.airHumidity
}

func (s *MockSensorServer) GetLeafTemp() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.leafTemp
}

func (s *MockSensorServer) GetLeafHumidity() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.leafHumidity
}

func (s *MockSensorServer) GetPAR() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.par
}

func (s *MockSensorServer) GetCO2() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.co2
}

func (s *MockSensorServer) Stop() {
	close(s.stopChan)
	if s.listener != nil {
		s.listener.Close()
	}
	s.wg.Wait()
	s.logger.Info("Mock sensor server stopped")
}
