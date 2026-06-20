package plc

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"greenhouse-climate-core/internal/domain/entity"
	"greenhouse-climate-core/internal/domain/repository"
)

type LEDController struct {
	address     string
	port        int
	logger      *logrus.Logger
	deviceRepo  repository.LEDDeviceRepository
	conn        net.Conn
	mu          sync.Mutex
	isConnected bool
	timeout     time.Duration
	ctx         context.Context
	cancel      context.CancelFunc
}

func NewLEDController(
	address string,
	port int,
	logger *logrus.Logger,
	deviceRepo repository.LEDDeviceRepository,
) *LEDController {
	ctx, cancel := context.WithCancel(context.Background())
	return &LEDController{
		address:    address,
		port:       port,
		logger:     logger,
		deviceRepo: deviceRepo,
		timeout:    3 * time.Second,
		ctx:        ctx,
		cancel:     cancel,
	}
}

func (c *LEDController) connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.isConnected && c.conn != nil {
		return nil
	}

	conn, err := net.DialTimeout("tcp", net.JoinHostPort(c.address, strconv.Itoa(c.port)), c.timeout)
	if err != nil {
		c.logger.Warnf("LED controller connect failed: %v", err)
		return err
	}

	c.conn = conn
	c.isConnected = true
	c.logger.Info("LED controller connected successfully")
	return nil
}

func (c *LEDController) disconnect() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	c.isConnected = false
}

func (c *LEDController) buildLEDCommand(deviceID uint8, powerPercent uint8) []byte {
	frame := make([]byte, 12)

	frame[0] = 0x02
	frame[1] = 0x06

	binary.BigEndian.PutUint16(frame[2:4], uint16(deviceID))
	binary.BigEndian.PutUint16(frame[4:6], uint16(powerPercent))
	frame[6] = 0x01

	crc := c.calculateCRC(frame[:8])
	binary.LittleEndian.PutUint16(frame[8:10], crc)

	return frame[:10]
}

func (c *LEDController) calculateCRC(data []byte) uint16 {
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

func (c *LEDController) SetDevicePower(ctx context.Context, device *entity.LEDLightDevice, powerPercent uint8) error {
	if powerPercent > 100 {
		powerPercent = 100
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if err := c.connect(); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.conn.SetDeadline(time.Now().Add(c.timeout))

	request := c.buildLEDCommand(device.ID, powerPercent)
	if _, err := c.conn.Write(request); err != nil {
		c.isConnected = false
		return err
	}

	response := make([]byte, 64)
	n, err := c.conn.Read(response)
	if err != nil {
		c.isConnected = false
		return err
	}

	if n < 5 {
		return errors.New("invalid LED controller response")
	}

	if response[1]&0x80 != 0 {
		return errors.New("LED controller command exception")
	}

	device.CurrentPower = float64(powerPercent) / 100.0 * device.MaxPower
	device.IsActive = powerPercent > 0

	if err := c.deviceRepo.Update(device); err != nil {
		c.logger.Warnf("Failed to update LED device state: %v", err)
	}

	c.logger.Infof("LED %s power set to %d%% (%.1fW)", device.Name, powerPercent, device.CurrentPower)
	return nil
}

func (c *LEDController) SetAllPower(ctx context.Context, powerPercent uint8) error {
	devices, err := c.deviceRepo.FindAll()
	if err != nil {
		return err
	}

	var lastErr error
	for _, dev := range devices {
		if err := c.SetDevicePower(ctx, dev, powerPercent); err != nil {
			lastErr = err
			c.logger.Warnf("Failed to set power for LED %s: %v", dev.Name, err)
		}
	}
	return lastErr
}

func (c *LEDController) ApplySmoothStep(ctx context.Context, devices []*entity.LEDLightDevice, targetPower uint8, steps int, stepDuration time.Duration) error {
	if steps <= 0 {
		steps = 5
	}
	if stepDuration <= 0 {
		stepDuration = 10 * time.Second
	}

	for step := 1; step <= steps; step++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		currentPower := uint8(float64(targetPower) * float64(step) / float64(steps))

		for _, dev := range devices {
			if err := c.SetDevicePower(ctx, dev, currentPower); err != nil {
				c.logger.Warnf("Smooth step failed for %s at step %d: %v", dev.Name, step, err)
			}
		}

		c.logger.Infof("LED smooth step %d/%d: power=%d%%", step, steps, currentPower)

		if step < steps {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(stepDuration):
			}
		}
	}

	return nil
}

func (c *LEDController) StopAll(ctx context.Context) error {
	return c.SetAllPower(ctx, 0)
}

func (c *LEDController) Stop() {
	c.cancel()
	c.disconnect()
	c.logger.Info("LED controller stopped")
}

func (c *LEDController) IsConnected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.isConnected
}
