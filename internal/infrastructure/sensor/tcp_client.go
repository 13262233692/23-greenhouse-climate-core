package sensor

import (
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"greenhouse-climate-core/internal/domain/entity"
)

type SensorConnection struct {
	sensor      *entity.Sensor
	conn        net.Conn
	decoder     *ModbusDecoder
	lastActive  time.Time
	mu          sync.Mutex
	isConnected bool
	retryCount  int
}

type TCPSensorClient struct {
	connections map[uint16]*SensorConnection
	decoder     *ModbusDecoder
	timeout     time.Duration
	logger      *logrus.Logger
	mu          sync.RWMutex
	readingChan chan *entity.SensorReading
	wg          sync.WaitGroup
	stopChan    chan struct{}
}

func NewTCPSensorClient(logger *logrus.Logger) *TCPSensorClient {
	return &TCPSensorClient{
		connections: make(map[uint16]*SensorConnection),
		decoder:     NewModbusDecoder(),
		timeout:     5 * time.Second,
		logger:      logger,
		readingChan: make(chan *entity.SensorReading, 1000),
		stopChan:    make(chan struct{}),
	}
}

func (c *TCPSensorClient) GetReadingChannel() <-chan *entity.SensorReading {
	return c.readingChan
}

func (c *TCPSensorClient) InitializeSensors(sensors []*entity.Sensor) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, sensor := range sensors {
		sc := &SensorConnection{
			sensor:  sensor,
			decoder: c.decoder,
		}
		c.connections[sensor.ID] = sc
		c.logger.Infof("Sensor %d initialized: %s (%s)", sensor.ID, sensor.Name, sensor.Type)
	}
	c.logger.Infof("Total %d sensors initialized", len(sensors))
	return nil
}

func (c *TCPSensorClient) connectSensor(sc *SensorConnection) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.isConnected && sc.conn != nil {
		return nil
	}

	addr := sc.sensor.Address
	if addr == "" {
		addr = "127.0.0.1"
	}
	port := sc.sensor.Port
	if port == 0 {
		port = 502
	}

	conn, err := net.DialTimeout("tcp", net.JoinHostPort(addr, strconv.Itoa(port)), c.timeout)
	if err != nil {
		sc.retryCount++
		sc.isConnected = false
		c.logger.Warnf("Sensor %d connect failed: %v, retry: %d", sc.sensor.ID, err, sc.retryCount)
		return err
	}

	sc.conn = conn
	sc.isConnected = true
	sc.lastActive = time.Now()
	sc.retryCount = 0
	c.logger.Infof("Sensor %d connected successfully", sc.sensor.ID)
	return nil
}

func (c *TCPSensorClient) readSensor(sc *SensorConnection) (*entity.SensorReading, error) {
	if err := c.connectSensor(sc); err != nil {
		return nil, err
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.conn.SetDeadline(time.Now().Add(c.timeout))

	request := c.decoder.BuildReadRequest(
		sc.sensor.SlaveID,
		sc.sensor.Register,
		sc.sensor.DataLength,
	)

	_, err := sc.conn.Write(request)
	if err != nil {
		sc.isConnected = false
		return nil, err
	}

	response := make([]byte, 256)
	n, err := sc.conn.Read(response)
	if err != nil {
		sc.isConnected = false
		return nil, err
	}

	frame, err := c.decoder.ParseRTUResponse(response[:n])
	if err != nil {
		return nil, err
	}

	sc.lastActive = time.Now()
	return c.decoder.DecodeSensorReading(sc.sensor, frame)
}

func (c *TCPSensorClient) startSensorPolling(sc *SensorConnection, interval time.Duration) {
	defer c.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopChan:
			c.logger.Infof("Stopping polling for sensor %d", sc.sensor.ID)
			return
		case <-ticker.C:
			reading, err := c.readSensor(sc)
			if err != nil {
				c.logger.Warnf("Sensor %d read error: %v", sc.sensor.ID, err)
				continue
			}
			select {
			case c.readingChan <- reading:
			default:
				c.logger.Warnf("Reading channel full, dropping data from sensor %d", sc.sensor.ID)
			}
		}
	}
}

func (c *TCPSensorClient) StartPolling(interval time.Duration) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	c.logger.Infof("Starting polling for %d sensors with interval %v", len(c.connections), interval)

	for _, sc := range c.connections {
		c.wg.Add(1)
		go c.startSensorPolling(sc, interval)
	}

	return nil
}

func (c *TCPSensorClient) Stop() {
	c.logger.Info("Stopping all sensor polling")
	close(c.stopChan)
	c.wg.Wait()
	close(c.readingChan)

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, sc := range c.connections {
		sc.mu.Lock()
		if sc.conn != nil {
			sc.conn.Close()
			sc.isConnected = false
		}
		sc.mu.Unlock()
	}
	c.logger.Info("All sensor connections closed")
}

func (c *TCPSensorClient) GetSensorStatus(sensorID uint16) (bool, time.Time, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	sc, exists := c.connections[sensorID]
	if !exists {
		return false, time.Time{}, nil
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	return sc.isConnected, sc.lastActive, nil
}

func (c *TCPSensorClient) GetAllSensorStatus() map[uint16]bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	status := make(map[uint16]bool)
	for id, sc := range c.connections {
		sc.mu.Lock()
		status[id] = sc.isConnected
		sc.mu.Unlock()
	}
	return status
}

func (c *TCPSensorClient) ReconnectSensor(sensorID uint16) error {
	c.mu.RLock()
	sc, exists := c.connections[sensorID]
	c.mu.RUnlock()

	if !exists {
		return nil
	}

	sc.mu.Lock()
	if sc.conn != nil {
		sc.conn.Close()
	}
	sc.isConnected = false
	sc.mu.Unlock()

	return c.connectSensor(sc)
}
