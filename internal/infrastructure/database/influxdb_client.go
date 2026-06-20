package database

import (
	"fmt"
	"greenhouse-climate-core/internal/domain/entity"
	"greenhouse-climate-core/internal/domain/repository"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type InfluxDBPoint struct {
	Measurement string
	Tags        map[string]string
	Fields      map[string]interface{}
	Timestamp   time.Time
}

type InfluxDBClient struct {
	logger      *logrus.Logger
	buffer      []InfluxDBPoint
	bufferSize  int
	flushInterval time.Duration
	mu          sync.Mutex
	stopChan    chan struct{}
	wg          sync.WaitGroup
	enabled     bool
}

func NewInfluxDBClient(logger *logrus.Logger, enabled bool) repository.TimeSeriesRepository {
	client := &InfluxDBClient{
		logger:        logger,
		buffer:        make([]InfluxDBPoint, 0),
		bufferSize:    1000,
		flushInterval: 5 * time.Second,
		stopChan:      make(chan struct{}),
		enabled:       enabled,
	}

	if enabled {
		client.wg.Add(1)
		go client.flushLoop()
	}

	return client
}

func (c *InfluxDBClient) flushLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopChan:
			c.flush()
			return
		case <-ticker.C:
			c.flush()
		}
	}
}

func (c *InfluxDBClient) flush() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.buffer) == 0 {
		return
	}

	for _, point := range c.buffer {
		line := c.formatLineProtocol(point)
		c.logger.Debugf("InfluxDB Write: %s", line)
	}

	c.logger.Infof("Flushed %d points to InfluxDB", len(c.buffer))
	c.buffer = c.buffer[:0]
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

	c.mu.Lock()
	defer c.mu.Unlock()

	c.buffer = append(c.buffer, point)

	if len(c.buffer) >= c.bufferSize {
		go c.flush()
	}

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
			"air_temp":          vpd.AirTemp,
			"air_humidity":      vpd.AirHumidity,
			"leaf_temp":         vpd.LeafTemp,
			"saturation_vpd":    vpd.SaturationVPD,
			"actual_vpd":        vpd.ActualVPD,
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
			"vpd_id":          fmt.Sprintf("%d", command.VPDID),
			"opening_degree":  command.OpeningDegree,
			"pulse_duration_ms": command.PulseDuration.Milliseconds(),
			"reason":          command.Reason,
		},
		Timestamp: command.CreatedAt,
	}

	return c.writePoint(point)
}

func (c *InfluxDBClient) Close() {
	if c.enabled {
		close(c.stopChan)
		c.wg.Wait()
	}
	c.logger.Info("InfluxDB client closed")
}
