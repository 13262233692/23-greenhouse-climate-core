package database

import (
	"context"
	"fmt"
	"greenhouse-climate-core/internal/domain/entity"
	"greenhouse-climate-core/internal/domain/repository"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	DefaultBufferSize     = 5000
	DefaultFlushInterval  = 2 * time.Second
	DefaultWriteTimeout   = 3 * time.Second
	DefaultMaxBatchSize   = 1000
	DefaultMaxConcurrency = 3
	CircuitBreakerThreshold = 5
	CircuitBreakerResetTime = 30 * time.Second
)

type CircuitBreakerState int32

const (
	CircuitClosed CircuitBreakerState = iota
	CircuitOpen
	CircuitHalfOpen
)

type InfluxDBPoint struct {
	Measurement string
	Tags        map[string]string
	Fields      map[string]interface{}
	Timestamp   time.Time
}

type pointBuffer struct {
	points   []InfluxDBPoint
	mu       sync.Mutex
}

type InfluxDBClient struct {
	logger           *logrus.Logger
	buffers          []*pointBuffer
	bufferCount      int
	bufferSize       int
	flushInterval    time.Duration
	writeTimeout     time.Duration
	maxBatchSize     int
	maxConcurrency   int
	stopChan         chan struct{}
	wg               sync.WaitGroup
	enabled          bool
	ctx              context.Context
	cancel           context.CancelFunc
	circuitState     int32
	failureCount     int32
	lastSuccessTime  int64
	lastFailureTime  int64
	droppedCount     uint64
	writtenCount     uint64
	flushWorkerPool  chan struct{}
}

func NewInfluxDBClient(logger *logrus.Logger, enabled bool) repository.TimeSeriesRepository {
	ctx, cancel := context.WithCancel(context.Background())

	bufferCount := 4
	buffers := make([]*pointBuffer, bufferCount)
	for i := 0; i < bufferCount; i++ {
		buffers[i] = &pointBuffer{
			points: make([]InfluxDBPoint, 0, DefaultBufferSize/bufferCount),
		}
	}

	client := &InfluxDBClient{
		logger:         logger,
		buffers:        buffers,
		bufferCount:    bufferCount,
		bufferSize:     DefaultBufferSize,
		flushInterval:  DefaultFlushInterval,
		writeTimeout:   DefaultWriteTimeout,
		maxBatchSize:   DefaultMaxBatchSize,
		maxConcurrency: DefaultMaxConcurrency,
		stopChan:       make(chan struct{}),
		enabled:        enabled,
		ctx:            ctx,
		cancel:         cancel,
		circuitState:   int32(CircuitClosed),
		flushWorkerPool: make(chan struct{}, DefaultMaxConcurrency),
	}

	if enabled {
		client.wg.Add(1)
		go client.flushLoop()
	}

	return client
}

func (c *InfluxDBClient) getCircuitState() CircuitBreakerState {
	return CircuitBreakerState(atomic.LoadInt32(&c.circuitState))
}

func (c *InfluxDBClient) setCircuitState(state CircuitBreakerState) {
	atomic.StoreInt32(&c.circuitState, int32(state))
}

func (c *InfluxDBClient) recordSuccess() {
	atomic.StoreInt32(&c.failureCount, 0)
	atomic.StoreInt64(&c.lastSuccessTime, time.Now().UnixNano())
	c.setCircuitState(CircuitClosed)
}

func (c *InfluxDBClient) recordFailure() {
	atomic.AddInt32(&c.failureCount, 1)
	atomic.StoreInt64(&c.lastFailureTime, time.Now().UnixNano())

	if atomic.LoadInt32(&c.failureCount) >= CircuitBreakerThreshold {
		c.setCircuitState(CircuitOpen)
		c.logger.Warn("Circuit breaker opened for InfluxDB writes")
	}
}

func (c *InfluxDBClient) canWrite() bool {
	state := c.getCircuitState()

	switch state {
	case CircuitClosed:
		return true
	case CircuitOpen:
		lastFailure := atomic.LoadInt64(&c.lastFailureTime)
		if time.Since(time.Unix(0, lastFailure)) > CircuitBreakerResetTime {
			c.setCircuitState(CircuitHalfOpen)
			c.logger.Info("Circuit breaker half-open, allowing test write")
			return true
		}
		return false
	case CircuitHalfOpen:
		return true
	default:
		return true
	}
}

func (c *InfluxDBClient) getBuffer(index int) *pointBuffer {
	return c.buffers[index%c.bufferCount]
}

func (c *InfluxDBClient) flushLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopChan:
			c.flushAll()
			return
		case <-c.ctx.Done():
			c.flushAll()
			return
		case <-ticker.C:
			c.flushAll()
		}
	}
}

func (c *InfluxDBClient) flushAll() {
	for i := 0; i < c.bufferCount; i++ {
		buf := c.getBuffer(i)

		buf.mu.Lock()
		if len(buf.points) == 0 {
			buf.mu.Unlock()
			continue
		}

		points := buf.points
		buf.points = make([]InfluxDBPoint, 0, c.bufferSize/c.bufferCount)
		buf.mu.Unlock()

		select {
		case c.flushWorkerPool <- struct{}{}:
			c.wg.Add(1)
			go func(pts []InfluxDBPoint) {
				defer c.wg.Done()
				defer func() { <-c.flushWorkerPool }()
				c.flushBatch(pts)
			}(points)
		default:
			c.logger.Warnf("Flush worker pool full, dropping %d points", len(points))
			atomic.AddUint64(&c.droppedCount, uint64(len(points)))
		}
	}
}

func (c *InfluxDBClient) flushBatch(points []InfluxDBPoint) {
	if !c.canWrite() {
		atomic.AddUint64(&c.droppedCount, uint64(len(points)))
		c.logger.Warnf("Circuit breaker open, dropping %d points", len(points))
		return
	}

	ctx, cancel := context.WithTimeout(c.ctx, c.writeTimeout)
	defer cancel()

	for i := 0; i < len(points); i += c.maxBatchSize {
		end := i + c.maxBatchSize
		if end > len(points) {
			end = len(points)
		}
		batch := points[i:end]

		if err := c.writeBatchWithContext(ctx, batch); err != nil {
			c.recordFailure()
			atomic.AddUint64(&c.droppedCount, uint64(len(batch)))
			c.logger.Errorf("Failed to write batch of %d points: %v", len(batch), err)
			return
		}

		atomic.AddUint64(&c.writtenCount, uint64(len(batch)))
	}

	c.recordSuccess()

	if c.getCircuitState() == CircuitHalfOpen {
		c.logger.Info("Circuit breaker closed after successful write")
	}
}

func (c *InfluxDBClient) writeBatchWithContext(ctx context.Context, points []InfluxDBPoint) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	var lines []string
	for _, point := range points {
		line := c.formatLineProtocol(point)
		lines = append(lines, line)
		c.logger.Tracef("InfluxDB Write: %s", line)
	}

	c.logger.Debugf("Flushed %d points to InfluxDB", len(points))
	return nil
}

func (c *InfluxDBClient) formatLineProtocol(point InfluxDBPoint) string {
	var tags []string
	for k, v := range point.Tags {
		tags = append(tags, fmt.Sprintf("%s=%s", k, v))
	}
	tagStr := ""
	if len(tags) > 0 {
		tagStr = "," + strings.Join(tags, ",")
	}

	var fields []string
	for k, v := range point.Fields {
		switch val := v.(type) {
		case float64:
			fields = append(fields, fmt.Sprintf("%s=%f", k, val))
		case int:
			fields = append(fields, fmt.Sprintf("%s=%di", k, val))
		case uint16:
			fields = append(fields, fmt.Sprintf("%s=%di", k, val))
		case bool:
			fields = append(fields, fmt.Sprintf("%s=%t", k, val))
		case string:
			fields = append(fields, fmt.Sprintf("%s=\"%s\"", k, val))
		default:
			fields = append(fields, fmt.Sprintf("%s=%v", k, val))
		}
	}
	fieldStr := strings.Join(fields, ",")

	return fmt.Sprintf("%s%s %s %d",
		point.Measurement,
		tagStr,
		fieldStr,
		point.Timestamp.UnixNano(),
	)
}

func (c *InfluxDBClient) writePoint(point InfluxDBPoint) error {
	if !c.enabled {
		c.logger.Tracef("InfluxDB disabled: %s", c.formatLineProtocol(point))
		return nil
	}

	if !c.canWrite() {
		atomic.AddUint64(&c.droppedCount, 1)
		return nil
	}

	hash := int(point.Timestamp.UnixNano() % int64(c.bufferCount))
	buf := c.getBuffer(hash)

	buf.mu.Lock()
	defer buf.mu.Unlock()

	if len(buf.points) >= c.bufferSize/c.bufferCount {
		atomic.AddUint64(&c.droppedCount, 1)
		c.logger.Trace("Buffer full, dropping point")
		return nil
	}

	buf.points = append(buf.points, point)

	return nil
}

func (c *InfluxDBClient) WriteSensorReading(reading *entity.SensorReading) error {
	point := InfluxDBPoint{
		Measurement: "sensor_readings",
		Tags: map[string]string{
			"sensor_id":   fmt.Sprintf("%d", reading.SensorID),
			"slave_id":    fmt.Sprintf("%d", reading.SlaveID),
			"sensor_type": string(reading.Type),
		},
		Fields:    make(map[string]interface{}),
		Timestamp: reading.Timestamp,
	}

	if reading.LeafTemp != 0 {
		point.Fields["leaf_temp"] = reading.LeafTemp
	}
	if reading.LeafHumidity != 0 {
		point.Fields["leaf_humidity"] = reading.LeafHumidity
	}
	if reading.AirTemp != 0 {
		point.Fields["air_temp"] = reading.AirTemp
	}
	if reading.AirHumidity != 0 {
		point.Fields["air_humidity"] = reading.AirHumidity
	}
	if reading.PAR != 0 {
		point.Fields["par"] = reading.PAR
	}
	if reading.CO2 != 0 {
		point.Fields["co2"] = reading.CO2
	}

	return c.writePoint(point)
}

func (c *InfluxDBClient) WriteVPDReading(vpd *entity.VPDReading) error {
	point := InfluxDBPoint{
		Measurement: "vpd_readings",
		Tags: map[string]string{
			"greenhouse_id": vpd.GreenhouseID,
			"sensor_id":     fmt.Sprintf("%d", vpd.SensorID),
			"status":        string(vpd.Status),
		},
		Fields: map[string]interface{}{
			"air_temp":               vpd.AirTemp,
			"air_humidity":           vpd.AirHumidity,
			"leaf_temp":              vpd.LeafTemp,
			"saturation_vpd":         vpd.SaturationVPD,
			"actual_vpd":             vpd.ActualVPD,
			"continuous_deviation_ms": vpd.ContinuousDeviation.Milliseconds(),
		},
		Timestamp: vpd.Timestamp,
	}

	return c.writePoint(point)
}

func (c *InfluxDBClient) WritePLCCommand(command *entity.PLCCommand) error {
	point := InfluxDBPoint{
		Measurement: "plc_commands",
		Tags: map[string]string{
			"greenhouse_id": command.GreenhouseID,
			"command_type":  string(command.Type),
			"status":        string(command.Status),
			"target_device": fmt.Sprintf("%d", command.TargetDevice),
		},
		Fields: map[string]interface{}{
			"vpd_id":            fmt.Sprintf("%d", command.VPDID),
			"opening_degree":    command.OpeningDegree,
			"pulse_duration_ms": command.PulseDuration.Milliseconds(),
			"reason":            command.Reason,
		},
		Timestamp: command.CreatedAt,
	}

	return c.writePoint(point)
}

func (c *InfluxDBClient) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"written":        atomic.LoadUint64(&c.writtenCount),
		"dropped":        atomic.LoadUint64(&c.droppedCount),
		"circuit_state":  c.getCircuitState(),
		"failure_count":  atomic.LoadInt32(&c.failureCount),
		"enabled":        c.enabled,
	}
}

func (c *InfluxDBClient) Close() {
	if c.enabled {
		c.cancel()
		close(c.stopChan)
		c.wg.Wait()
	}

	stats := c.GetStats()
	c.logger.Infof("InfluxDB client closed. Stats: %+v", stats)
}
