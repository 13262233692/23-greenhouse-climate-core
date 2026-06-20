package sensor

import (
	"context"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
	"greenhouse-climate-core/internal/domain/entity"
)

const (
	DefaultWorkerCount      = 10
	DefaultChannelBuffer    = 5000
	DefaultRateLimit        = 100
	DefaultMaxRetries       = 3
	DefaultReconnectDelay   = 5 * time.Second
	DefaultWriteTimeout     = 500 * time.Millisecond
	DefaultReadTimeout      = 2 * time.Second
)

type backpressureSignal struct {
	level     int32
	threshold int32
}

func (b *backpressureSignal) load() int32 {
	return atomic.LoadInt32(&b.level)
}

func (b *backpressureSignal) store(val int32) {
	atomic.StoreInt32(&b.level, val)
}

func (b *backpressureSignal) isHigh() bool {
	return b.load() > b.threshold
}

type SensorConnection struct {
	sensor       *entity.Sensor
	conn         net.Conn
	decoder      *ModbusDecoder
	lastActive   time.Time
	mu           sync.Mutex
	isConnected  bool
	retryCount   int
	lastError    time.Time
	limiter      *rate.Limiter
	ctx          context.Context
	cancel       context.CancelFunc
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
	workerPool  chan struct{}
	backpressure backpressureSignal
	rateLimiter *rate.Limiter
	maxRetries  int
	workerCount int
	ctx         context.Context
	cancel      context.CancelFunc
	droppedCount uint64
	processedCount uint64
}

func NewTCPSensorClient(logger *logrus.Logger) *TCPSensorClient {
	ctx, cancel := context.WithCancel(context.Background())

	return &TCPSensorClient{
		connections: make(map[uint16]*SensorConnection),
		decoder:     NewModbusDecoder(),
		timeout:     5 * time.Second,
		logger:      logger,
		readingChan: make(chan *entity.SensorReading, DefaultChannelBuffer),
		stopChan:    make(chan struct{}),
		workerPool:  make(chan struct{}, DefaultWorkerCount),
		backpressure: backpressureSignal{
			threshold: int32(DefaultChannelBuffer * 0.8),
		},
		rateLimiter: rate.NewLimiter(rate.Limit(DefaultRateLimit), DefaultRateLimit/2),
		maxRetries:  DefaultMaxRetries,
		workerCount: DefaultWorkerCount,
		ctx:         ctx,
		cancel:      cancel,
	}
}

func (c *TCPSensorClient) GetReadingChannel() <-chan *entity.SensorReading {
	return c.readingChan
}

func (c *TCPSensorClient) GetBackpressureLevel() float64 {
	level := c.backpressure.load()
	return float64(level) / float64(DefaultChannelBuffer)
}

func (c *TCPSensorClient) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"dropped":      atomic.LoadUint64(&c.droppedCount),
		"processed":    atomic.LoadUint64(&c.processedCount),
		"channel_len":  len(c.readingChan),
		"channel_cap":  cap(c.readingChan),
		"backpressure": c.GetBackpressureLevel(),
		"worker_count": c.workerCount,
	}
}

func (c *TCPSensorClient) InitializeSensors(sensors []*entity.Sensor) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, sensor := range sensors {
		ctx, cancel := context.WithCancel(c.ctx)
		sc := &SensorConnection{
			sensor:  sensor,
			decoder: c.decoder,
			limiter: rate.NewLimiter(rate.Limit(2), 1),
			ctx:     ctx,
			cancel:  cancel,
		}
		c.connections[sensor.ID] = sc
		c.logger.Infof("Sensor %d initialized: %s (%s)", sensor.ID, sensor.Name, sensor.Type)
	}
	c.logger.Infof("Total %d sensors initialized with %d workers", len(sensors), c.workerCount)
	return nil
}

func (c *TCPSensorClient) connectSensor(ctx context.Context, sc *SensorConnection) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.isConnected && sc.conn != nil {
		return nil
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if !sc.limiter.Allow() {
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

	dialer := &net.Dialer{
		Timeout:   c.timeout,
		KeepAlive: 30 * time.Second,
	}

	conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(addr, strconv.Itoa(port)))
	if err != nil {
		sc.retryCount++
		sc.isConnected = false
		sc.lastError = time.Now()
		c.logger.Warnf("Sensor %d connect failed: %v, retry: %d", sc.sensor.ID, err, sc.retryCount)
		return err
	}

	if sc.conn != nil {
		sc.conn.Close()
	}

	sc.conn = conn
	sc.isConnected = true
	sc.lastActive = time.Now()
	sc.retryCount = 0
	c.logger.Debugf("Sensor %d connected successfully", sc.sensor.ID)
	return nil
}

func (c *TCPSensorClient) readSensor(ctx context.Context, sc *SensorConnection) (*entity.SensorReading, error) {
	if err := c.connectSensor(ctx, sc); err != nil {
		return nil, err
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if !sc.isConnected || sc.conn == nil {
		return nil, nil
	}

	writeDeadline := time.Now().Add(DefaultWriteTimeout)
	readDeadline := time.Now().Add(DefaultReadTimeout)

	if err := sc.conn.SetWriteDeadline(writeDeadline); err != nil {
		sc.isConnected = false
		return nil, err
	}

	request := c.decoder.BuildReadRequest(
		sc.sensor.SlaveID,
		sc.sensor.Register,
		sc.sensor.DataLength,
	)

	if _, err := sc.conn.Write(request); err != nil {
		sc.isConnected = false
		return nil, err
	}

	if err := sc.conn.SetReadDeadline(readDeadline); err != nil {
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

func (c *TCPSensorClient) startSensorPolling(ctx context.Context, sc *SensorConnection, interval time.Duration) {
	defer c.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	retryDelay := time.Duration(0)

	for {
		if retryDelay > 0 {
			select {
			case <-ctx.Done():
				return
			case <-c.stopChan:
				return
			case <-time.After(retryDelay):
				retryDelay = 0
			}
		}

		select {
		case <-ctx.Done():
			c.logger.Debugf("Context cancelled, stopping polling for sensor %d", sc.sensor.ID)
			return
		case <-c.stopChan:
			c.logger.Infof("Stopping polling for sensor %d", sc.sensor.ID)
			return
		case <-ticker.C:
			if c.backpressure.isHigh() {
				atomic.AddUint64(&c.droppedCount, 1)
				c.logger.Tracef("Backpressure high, skipping sensor %d", sc.sensor.ID)
				continue
			}

			if !c.rateLimiter.Allow() {
				continue
			}

			select {
			case c.workerPool <- struct{}{}:
				c.wg.Add(1)
				go func() {
					defer c.wg.Done()
					defer func() { <-c.workerPool }()

					readCtx, readCancel := context.WithTimeout(ctx, c.timeout)
					defer readCancel()

					reading, err := c.readSensor(readCtx, sc)
					if err != nil {
						if ctx.Err() == nil {
							c.logger.Warnf("Sensor %d read error: %v", sc.sensor.ID, err)
							retryDelay = DefaultReconnectDelay
						}
						return
					}
					if reading == nil {
						return
					}

					select {
					case c.readingChan <- reading:
						atomic.AddUint64(&c.processedCount, 1)
						c.backpressure.store(int32(len(c.readingChan)))
					case <-ctx.Done():
						return
					case <-c.stopChan:
						return
					default:
						atomic.AddUint64(&c.droppedCount, 1)
						c.logger.Warnf("Reading channel full, dropping data from sensor %d (dropped: %d)",
							sc.sensor.ID, atomic.LoadUint64(&c.droppedCount))
					}
				}()
			default:
				atomic.AddUint64(&c.droppedCount, 1)
				c.logger.Tracef("Worker pool full, skipping sensor %d", sc.sensor.ID)
			}
		}
	}
}

func (c *TCPSensorClient) StartPolling(interval time.Duration) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	c.logger.Infof("Starting polling for %d sensors with interval %v, workers=%d",
		len(c.connections), interval, c.workerCount)

	go c.monitorBackpressure()

	for _, sc := range c.connections {
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		default:
			sensorCtx, sensorCancel := context.WithCancel(c.ctx)
			sc.ctx = sensorCtx
			sc.cancel = sensorCancel

			c.wg.Add(1)
			go c.startSensorPolling(sensorCtx, sc, interval)
		}
	}

	return nil
}

func (c *TCPSensorClient) monitorBackpressure() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopChan:
			return
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			level := c.backpressure.load()
			processed := atomic.LoadUint64(&c.processedCount)
			dropped := atomic.LoadUint64(&c.droppedCount)

			if level > c.backpressure.threshold {
				c.logger.Warnf("Backpressure high: channel=%d/%d (%.0f%%), processed=%d, dropped=%d",
					level, DefaultChannelBuffer, float64(level)/DefaultChannelBuffer*100, processed, dropped)
			} else {
				c.logger.Debugf("Backpressure: channel=%d/%d, processed=%d, dropped=%d",
					level, DefaultChannelBuffer, processed, dropped)
			}
		}
	}
}

func (c *TCPSensorClient) Stop() {
	c.logger.Info("Stopping all sensor polling")

	c.cancel()

	close(c.stopChan)

	c.wg.Wait()

	c.mu.Lock()
	defer c.mu.Unlock()

	for id, sc := range c.connections {
		sc.cancel()
		sc.mu.Lock()
		if sc.conn != nil {
			sc.conn.Close()
			sc.isConnected = false
		}
		sc.mu.Unlock()
		c.logger.Debugf("Closed connection for sensor %d", id)
	}

	close(c.readingChan)

	stats := c.GetStats()
	c.logger.Infof("Sensor client stopped. Stats: %+v", stats)
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

	sc.cancel()

	sc.mu.Lock()
	if sc.conn != nil {
		sc.conn.Close()
	}
	sc.isConnected = false
	sc.mu.Unlock()

	ctx, cancel := context.WithTimeout(c.ctx, c.timeout)
	defer cancel()

	return c.connectSensor(ctx, sc)
}

func (c *TCPSensorClient) SetWorkerCount(count int) {
	if count <= 0 {
		count = 1
	}
	if count > 100 {
		count = 100
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.workerCount = count
	c.workerPool = make(chan struct{}, count)
	c.logger.Infof("Worker count adjusted to %d", count)
}
