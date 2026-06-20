package api

import (
	"greenhouse-climate-core/internal/application/dto"
	"greenhouse-climate-core/internal/application/service"
	"greenhouse-climate-core/internal/domain/repository"
	"greenhouse-climate-core/internal/infrastructure/sensor"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

type SensorHandler struct {
	sensorRepo   repository.SensorRepository
	readingRepo  repository.SensorReadingRepository
	sensorClient *sensor.TCPSensorClient
	controller   *service.ClimateController
	logger       *logrus.Logger
}

func NewSensorHandler(
	sensorRepo repository.SensorRepository,
	readingRepo repository.SensorReadingRepository,
	sensorClient *sensor.TCPSensorClient,
	controller *service.ClimateController,
	logger *logrus.Logger,
) *SensorHandler {
	return &SensorHandler{
		sensorRepo:   sensorRepo,
		readingRepo:  readingRepo,
		sensorClient: sensorClient,
		controller:   controller,
		logger:       logger,
	}
}

func (h *SensorHandler) GetSensors(c *gin.Context) {
	sensors, err := h.sensorRepo.FindAll()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	statusMap := h.sensorClient.GetAllSensorStatus()
	result := make([]*dto.SensorDTO, 0, len(sensors))
	for _, s := range sensors {
		result = append(result, dto.ToSensorDTO(s, statusMap[s.ID]))
	}

	c.JSON(http.StatusOK, result)
}

func (h *SensorHandler) GetSensorByID(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 16)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid sensor id"})
		return
	}

	sensor, err := h.sensorRepo.FindByID(uint16(id))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "sensor not found"})
		return
	}

	isConnected, _, _ := h.sensorClient.GetSensorStatus(uint16(id))
	c.JSON(http.StatusOK, dto.ToSensorDTO(sensor, isConnected))
}

func (h *SensorHandler) GetLatestReading(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 16)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid sensor id"})
		return
	}

	reading := h.controller.GetLatestReading(uint16(id))
	if reading == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "no reading available"})
		return
	}

	c.JSON(http.StatusOK, dto.ToSensorReadingDTO(reading))
}

func (h *SensorHandler) GetAllLatestReadings(c *gin.Context) {
	readings := h.controller.GetAllLatestReadings()
	result := make([]*dto.SensorReadingDTO, 0, len(readings))
	for _, r := range readings {
		result = append(result, dto.ToSensorReadingDTO(r))
	}

	c.JSON(http.StatusOK, result)
}

func (h *SensorHandler) ReconnectSensor(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 16)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid sensor id"})
		return
	}

	if err := h.sensorClient.ReconnectSensor(uint16(id)); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "reconnecting sensor"})
}

func (h *SensorHandler) GetSensorStatus(c *gin.Context) {
	statusMap := h.sensorClient.GetAllSensorStatus()
	total := len(statusMap)
	connectedCount := 0
	for _, isConnected := range statusMap {
		if isConnected {
			connectedCount++
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"total":     total,
		"connected": connectedCount,
		"status":    statusMap,
	})
}
