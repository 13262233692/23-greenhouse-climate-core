package plc

import (
	"encoding/binary"
	"errors"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"greenhouse-climate-core/internal/domain/entity"
)

type PLCClient struct {
	address    string
	port       int
	conn       net.Conn
	timeout    time.Duration
	logger     *logrus.Logger
	mu         sync.Mutex
	isConnected bool
	commandChan chan *entity.PLCCommand
	stopChan    chan struct{}
	wg          sync.WaitGroup
}

func NewPLCClient(address string, port int, logger *logrus.Logger) *PLCClient {
	return &PLCClient{
		address:     address,
		port:        port,
		timeout:     5 * time.Second,
		logger:      logger,
		commandChan: make(chan *entity.PLCCommand, 100),
		stopChan:    make(chan struct{}),
	}
}

func (c *PLCClient) connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.isConnected && c.conn != nil {
		return nil
	}

	conn, err := net.DialTimeout("tcp", net.JoinHostPort(c.address, strconv.Itoa(c.port)), c.timeout)
	if err != nil {
		c.logger.Errorf("PLC connect failed: %v", err)
		return err
	}

	c.conn = conn
	c.isConnected = true
	c.logger.Info("PLC connected successfully")
	return nil
}

func (c *PLCClient) disconnect() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	c.isConnected = false
}

func (c *PLCClient) buildPulseCommand(command *entity.PLCCommand) []byte {
	frame := make([]byte, 12)

	frame[0] = 0x01
	frame[1] = 0x0F

	binary.BigEndian.PutUint16(frame[2:4], uint16(command.TargetDevice)*100)
	binary.BigEndian.PutUint16(frame[4:6], 8)
	frame[6] = 0x01

	if command.OpeningDegree > 0 {
		frame[7] = 0xFF
	} else {
		frame[7] = 0x00
	}

	crc := c.calculateCRC(frame[:8])
	binary.LittleEndian.PutUint16(frame[8:10], crc)

	return frame[:10]
}

func (c *PLCClient) calculateCRC(data []byte) uint16 {
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

func (c *PLCClient) executeCommand(command *entity.PLCCommand) error {
	if err := c.connect(); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.conn.SetDeadline(time.Now().Add(c.timeout))

	request := c.buildPulseCommand(command)

	_, err := c.conn.Write(request)
	if err != nil {
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
		return errors.New("invalid PLC response")
	}

	if response[1]&0x80 != 0 {
		return errors.New("PLC command exception")
	}

	c.logger.Infof("PLC command executed: type=%s, device=%d, opening=%d%%, duration=%v",
		command.Type, command.TargetDevice, command.OpeningDegree, command.PulseDuration)

	return nil
}

func (c *PLCClient) SendPulseCommand(command *entity.PLCCommand) error {
	command.MarkSent()

	err := c.executeCommand(command)
	if err != nil {
		command.MarkFailed(err.Error())
		c.logger.Errorf("PLC command failed: %v", err)
		return err
	}

	if command.PulseDuration > 0 {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			time.Sleep(command.PulseDuration)
			c.ensureStop(command)
		}()
	}

	command.MarkCompleted()
	return nil
}

func (c *PLCClient) ensureStop(command *entity.PLCCommand) {
	stopCommand := entity.NewStopCommand(command.GreenhouseID, command.TargetDevice)
	stopCommand.VPDID = command.VPDID

	if err := c.connect(); err != nil {
		c.logger.Errorf("PLC stop connect failed: %v", err)
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.conn.SetDeadline(time.Now().Add(c.timeout))

	request := c.buildPulseCommand(stopCommand)
	_, err := c.conn.Write(request)
	if err != nil {
		c.isConnected = false
		c.logger.Errorf("PLC stop command failed: %v", err)
		return
	}

	response := make([]byte, 64)
	_, err = c.conn.Read(response)
	if err != nil {
		c.isConnected = false
		c.logger.Errorf("PLC stop response read failed: %v", err)
		return
	}

	c.logger.Infof("PLC auto-stop completed: device=%d", command.TargetDevice)
}

func (c *PLCClient) Start() {
	c.logger.Info("Starting PLC command processor")
	c.wg.Add(1)
	go c.processLoop()
}

func (c *PLCClient) processLoop() {
	defer c.wg.Done()

	for {
		select {
		case <-c.stopChan:
			c.logger.Info("Stopping PLC command processor")
			return
		case command := <-c.commandChan:
			if err := c.SendPulseCommand(command); err != nil {
				c.logger.Errorf("Failed to process PLC command: %v", err)
			}
		}
	}
}

func (c *PLCClient) QueueCommand(command *entity.PLCCommand) {
	select {
	case c.commandChan <- command:
		c.logger.Infof("PLC command queued: type=%s, id=%d", command.Type, command.ID)
	default:
		c.logger.Warn("PLC command queue full, dropping command")
	}
}

func (c *PLCClient) Stop() {
	close(c.stopChan)
	c.wg.Wait()
	c.disconnect()
	close(c.commandChan)
	c.logger.Info("PLC client stopped")
}

func (c *PLCClient) IsConnected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.isConnected
}
