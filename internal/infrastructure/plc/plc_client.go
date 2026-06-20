package plc

import (
	"context"
	"encoding/binary"
	"errors"
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
	DefaultMaxConnections  = 5
	DefaultMaxRetries       = 3
	DefaultRetryDelay        = 500 * time.Millisecond
	DefaultCommandTimeout  = 10 * time.Second
	DefaultIdleTimeout       = 60 * time.Second
)

type plcConnection struct {
	conn       net.Conn
	lastUsed   time.Time
	inUse      bool
}

type PLCClient struct {
	address        string
	port           int
	timeout        time.Duration
	logger         *logrus.Logger
	mu             sync.Mutex
	commandChan    chan *entity.PLCCommand
	stopChan       chan struct{}
	wg             sync.WaitGroup
	connPool       []*plcConnection
	maxConnections  int
	limiter        *rate.Limiter
	ctx            context.Context
	cancel         context.CancelFunc
	maxRetries     int
	retryDelay      time.Duration
	activeCommands  int64
	completedCount uint64
	failedCount    uint64
}

func NewPLCClient(address string, port int, logger *logrus.Logger) *PLCClient {
	ctx, cancel := context.WithCancel(context.Background())

	return &PLCClient{
		address:        address,
		port:           port,
		timeout:        DefaultCommandTimeout,
		logger:         logger,
		commandChan:    make(chan *entity.PLCCommand, 1000),
		stopChan:       make(chan struct{}),
		connPool:       make([]*plcConnection, 0, DefaultMaxConnections),
		maxConnections: DefaultMaxConnections,
		limiter:        rate.NewLimiter(rate.Limit(10), 5),
		ctx:            ctx,
		cancel:         cancel,
		maxRetries:     DefaultMaxRetries,
		retryDelay:     DefaultRetryDelay,
	}
}

func (c *PLCClient) acquireConnection(ctx context.Context) (*plcConnection, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, pc := range c.connPool {
		if !pc.inUse {
			if time.Since(pc.lastUsed) > DefaultIdleTimeout {
				if pc.conn != nil {
					pc.conn.Close()
					pc.conn = nil
				}
			}
			if pc.conn == nil {
				conn, err := c.dialWithContext(ctx)
				if err != nil {
					return nil, err
				}
				pc.conn = conn
			}
			pc.inUse = true
			pc.lastUsed = time.Now()
			return pc, nil
		}
	}

	if len(c.connPool) < c.maxConnections {
		conn, err := c.dialWithContext(ctx)
		if err != nil {
			return nil, err
		}
		pc := &plcConnection{
			conn:     conn,
			inUse:    true,
			lastUsed: time.Now(),
		}
		c.connPool = append(c.connPool, pc)
		return pc, nil
	}

	return nil, errors.New("connection pool exhausted")
}

func (c *PLCClient) releaseConnection(pc *plcConnection) {
	c.mu.Lock()
	defer c.mu.Unlock()

	pc.inUse = false
	pc.lastUsed = time.Now()
}

func (c *PLCClient) dialWithContext(ctx context.Context) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if !c.limiter.Allow() {
		return nil, errors.New("rate limited")
	}

	dialer := &net.Dialer{
		Timeout:   c.timeout,
		KeepAlive: 30 * time.Second,
	}

	conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(c.address, strconv.Itoa(c.port)))
	if err != nil {
		c.logger.Errorf("PLC connect failed: %v", err)
		return nil, err
	}

	c.logger.Debug("PLC connected successfully")
	return conn, nil
}

func (c *PLCClient) closeAllConnections() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, pc := range c.connPool {
		if pc.conn != nil {
			pc.conn.Close()
			pc.conn = nil
		}
	}
	c.connPool = c.connPool[:0]
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

func (c *PLCClient) executeCommandWithContext(ctx context.Context, command *entity.PLCCommand) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	pc, err := c.acquireConnection(ctx)
	if err != nil {
		return err
	}
	defer c.releaseConnection(pc)

	writeDeadline := time.Now().Add(2 * time.Second)
	readDeadline := time.Now().Add(3 * time.Second)

	if err := pc.conn.SetWriteDeadline(writeDeadline); err != nil {
		pc.conn.Close()
		pc.conn = nil
		return err
	}

	request := c.buildPulseCommand(command)

	if _, err := pc.conn.Write(request); err != nil {
		pc.conn.Close()
		pc.conn = nil
		return err
	}

	if err := pc.conn.SetReadDeadline(readDeadline); err != nil {
		pc.conn.Close()
		pc.conn = nil
		return err
	}

	response := make([]byte, 64)
	n, err := pc.conn.Read(response)
	if err != nil {
		pc.conn.Close()
		pc.conn = nil
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

func (c *PLCClient) executeWithRetry(ctx context.Context, command *entity.PLCCommand) error {
	var lastErr error

	for attempt := 0; attempt < c.maxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		attemptCtx, attemptCancel := context.WithTimeout(ctx, c.timeout)
		err := c.executeCommandWithContext(attemptCtx, command)
		attemptCancel()

		if err == nil {
			return nil
		}

		lastErr = err
		c.logger.Warnf("PLC command attempt %d failed: %v", attempt+1, err)

		if attempt < c.maxRetries-1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.retryDelay * time.Duration(attempt+1)):
			}
		}
	}

	return lastErr
}

func (c *PLCClient) SendPulseCommandWithContext(ctx context.Context, command *entity.PLCCommand) error {
	command.MarkSent()

	execCtx, execCancel := context.WithTimeout(ctx, c.timeout*time.Duration(c.maxRetries))
	defer execCancel()

	err := c.executeWithRetry(execCtx, command)
	if err != nil {
		command.MarkFailed(err.Error())
		c.logger.Errorf("PLC command failed after %d retries: %v", c.maxRetries, err)
		return err
	}

	if command.PulseDuration > 0 {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()

			stopCtx, stopCancel := context.WithTimeout(c.ctx, command.PulseDuration+c.timeout)
			defer stopCancel()

			select {
			case <-stopCtx.Done():
				if stopCtx.Err() == context.Canceled {
					c.logger.Debugf("Auto-stop cancelled for device %d", command.TargetDevice)
					return
				}
			case <-time.After(command.PulseDuration):
			}

			c.ensureStopWithContext(stopCtx, command)
		}()
	}

	command.MarkCompleted()
	return nil
}

func (c *PLCClient) ensureStopWithContext(ctx context.Context, command *entity.PLCCommand) {
	stopCommand := entity.NewStopCommand(command.GreenhouseID, command.TargetDevice)
	stopCommand.VPDID = command.VPDID

	stopCtx, stopCancel := context.WithTimeout(ctx, c.timeout*time.Duration(c.maxRetries))
	defer stopCancel()

	if err := ctx.Err(); err != nil {
		c.logger.Warnf("PLC stop context cancelled: %v", err)
		return
	}

	if err := c.executeWithRetry(stopCtx, stopCommand); err != nil {
		c.logger.Errorf("PLC stop command failed: %v", err)
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
		case <-c.ctx.Done():
			c.logger.Info("PLC context cancelled, stopping processor")
			return
		case command := <-c.commandChan:
			cmdCtx, cmdCancel := context.WithTimeout(c.ctx, c.timeout*time.Duration(c.maxRetries))

			c.wg.Add(1)
			go func() {
				defer c.wg.Done()
				defer cmdCancel()

				if err := c.SendPulseCommandWithContext(cmdCtx, command); err != nil {
					c.logger.Errorf("Failed to process PLC command: %v", err)
				}
			}()
		}
	}
}

func (c *PLCClient) QueueCommand(command *entity.PLCCommand) {
	select {
	case c.commandChan <- command:
		c.logger.Infof("PLC command queued: type=%s, id=%d", command.Type, command.ID)
	case <-c.ctx.Done():
		c.logger.Warn("PLC client stopped, cannot queue command")
	default:
		c.logger.Warn("PLC command queue full, dropping command")
	}
}

func (c *PLCClient) Stop() {
	c.logger.Info("Stopping PLC client")

	c.cancel()

	close(c.stopChan)

	c.wg.Wait()

	c.closeAllConnections()

	close(c.commandChan)

	c.logger.Info("PLC client stopped. Completed: %d, Failed: %d",
		atomic.LoadUint64(&c.completedCount),
		atomic.LoadUint64(&c.failedCount))
}

func (c *PLCClient) IsConnected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, pc := range c.connPool {
		if pc.conn != nil && pc.inUse {
			return true
		}
	}
	return false
}

func (c *PLCClient) GetStats() map[string]interface{} {
	c.mu.Lock()
	defer c.mu.Unlock()

	inUse := 0
	for _, pc := range c.connPool {
		if pc.inUse {
			inUse++
		}
	}

	return map[string]interface{}{
		"pool_size":    len(c.connPool),
		"pool_max":   c.maxConnections,
		"in_use":     inUse,
		"completed": atomic.LoadUint64(&c.completedCount),
		"failed":    atomic.LoadUint64(&c.failedCount),
		"queue_len": len(c.commandChan),
		"active":    atomic.LoadInt64(&c.activeCommands),
	}
}

func (c *PLCClient) SetMaxConnections(max int) {
	if max <= 0 {
		max = 1
	}
	if max > 20 {
		max = 20
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.maxConnections = max
	c.logger.Infof("PLC max connections set to %d", max)
}
