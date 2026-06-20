package api

import (
	"greenhouse-climate-core/internal/application/dto"
	"greenhouse-climate-core/internal/application/service"
	"greenhouse-climate-core/internal/domain/repository"
	"greenhouse-climate-core/internal/infrastructure/plc"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

type PLCHandler struct {
	plcRepo    repository.PLCCommandRepository
	plcClient  *plc.PLCClient
	controller *service.ClimateController
	logger     *logrus.Logger
}

func NewPLCHandler(
	plcRepo repository.PLCCommandRepository,
	plcClient *plc.PLCClient,
	controller *service.ClimateController,
	logger *logrus.Logger,
) *PLCHandler {
	return &PLCHandler{
		plcRepo:    plcRepo,
		plcClient:  plcClient,
		controller: controller,
		logger:     logger,
	}
}

func (h *PLCHandler) TriggerMistCooling(c *gin.Context) {
	var req struct {
		DurationSeconds int `json:"duration_seconds"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		req.DurationSeconds = 30
	}

	if req.DurationSeconds <= 0 {
		req.DurationSeconds = 30
	}
	if req.DurationSeconds > 300 {
		req.DurationSeconds = 300
	}

	command := h.controller.ManualMistCooling(time.Duration(req.DurationSeconds) * time.Second)

	c.JSON(http.StatusOK, gin.H{
		"message": "mist cooling triggered",
		"command": dto.ToPLCCommandDTO(command),
	})
}

func (h *PLCHandler) TriggerCO2Control(c *gin.Context) {
	var req struct {
		OpeningDegree uint8 `json:"opening_degree" binding:"required,min=0,max=100"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	command := h.controller.ManualCO2Control(req.OpeningDegree)

	c.JSON(http.StatusOK, gin.H{
		"message": "CO2 control triggered",
		"command": dto.ToPLCCommandDTO(command),
	})
}

func (h *PLCHandler) StopDevice(c *gin.Context) {
	deviceIDStr := c.Param("device_id")
	deviceID, err := strconv.ParseUint(deviceIDStr, 10, 8)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid device id"})
		return
	}

	command := h.controller.StopDevice(uint8(deviceID))

	c.JSON(http.StatusOK, gin.H{
		"message": "device stop command sent",
		"command": dto.ToPLCCommandDTO(command),
	})
}

func (h *PLCHandler) GetCommandHistory(c *gin.Context) {
	greenhouseID := c.DefaultQuery("greenhouse_id", h.controller.GetGreenhouseID())
	durationStr := c.DefaultQuery("duration", "24h")

	duration, err := time.ParseDuration(durationStr)
	if err != nil {
		duration = 24 * time.Hour
	}

	commands, err := h.plcRepo.FindByGreenhouseID(
		greenhouseID,
		time.Now().Add(-duration),
		time.Now(),
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	result := make([]*dto.PLCCommandDTO, 0, len(commands))
	for _, cmd := range commands {
		result = append(result, dto.ToPLCCommandDTO(cmd))
	}

	c.JSON(http.StatusOK, result)
}

func (h *PLCHandler) GetPendingCommands(c *gin.Context) {
	commands, err := h.plcRepo.FindPending()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	result := make([]*dto.PLCCommandDTO, 0, len(commands))
	for _, cmd := range commands {
		result = append(result, dto.ToPLCCommandDTO(cmd))
	}

	c.JSON(http.StatusOK, result)
}

func (h *PLCHandler) GetCommandByID(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid command id"})
		return
	}

	command, err := h.plcRepo.FindByID(id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if command == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "command not found"})
		return
	}

	c.JSON(http.StatusOK, dto.ToPLCCommandDTO(command))
}

func (h *PLCHandler) GetPLCStatus(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"connected": h.plcClient.IsConnected(),
		"queue_size": 0,
	})
}
