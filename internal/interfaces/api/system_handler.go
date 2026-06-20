package api

import (
	"greenhouse-climate-core/internal/application/service"
	"greenhouse-climate-core/internal/infrastructure/sensor"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

type SystemHandler struct {
	controller      *service.ClimateController
	mockServer      *sensor.MockSensorServer
	logger          *logrus.Logger
	startTime       time.Time
	useMockSensors  bool
}

func NewSystemHandler(
	controller *service.ClimateController,
	mockServer *sensor.MockSensorServer,
	logger *logrus.Logger,
	useMockSensors bool,
) *SystemHandler {
	return &SystemHandler{
		controller:     controller,
		mockServer:     mockServer,
		logger:         logger,
		startTime:      time.Now(),
		useMockSensors: useMockSensors,
	}
}

func (h *SystemHandler) GetHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":    "healthy",
		"timestamp": time.Now(),
		"uptime":    time.Since(h.startTime).String(),
		"version":   "1.0.0",
	})
}

func (h *SystemHandler) GetStatus(c *gin.Context) {
	greenhouseID := h.controller.GetGreenhouseID()
	vpd := h.controller.GetVPDCalculator().GetLatestVPD(greenhouseID)

	var vpdStatus string
	var actualVPD float64
	if vpd != nil {
		vpdStatus = string(vpd.Status)
		actualVPD = vpd.ActualVPD
	}

	c.JSON(http.StatusOK, gin.H{
		"greenhouse_id": greenhouseID,
		"uptime":        time.Since(h.startTime).String(),
		"use_mock":      h.useMockSensors,
		"vpd_status":    vpdStatus,
		"actual_vpd":    actualVPD,
	})
}

func (h *SystemHandler) SetMockEnvironment(c *gin.Context) {
	if !h.useMockSensors || h.mockServer == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "mock sensors not enabled"})
		return
	}

	var req struct {
		Mode string `json:"mode" binding:"required,oneof=normal high low"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	switch req.Mode {
	case "normal":
		h.mockServer.SetVPNormal()
	case "high":
		h.mockServer.SetVPDHigh()
	case "low":
		h.mockServer.SetVPDLow()
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "mock environment updated",
		"mode":    req.Mode,
	})
}

func (h *SystemHandler) GetMockEnvironment(c *gin.Context) {
	if !h.useMockSensors || h.mockServer == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "mock sensors not enabled"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"air_temp":      h.mockServer.GetAirTemp(),
		"air_humidity":  h.mockServer.GetAirHumidity(),
		"leaf_temp":     h.mockServer.GetLeafTemp(),
		"leaf_humidity": h.mockServer.GetLeafHumidity(),
		"par":           h.mockServer.GetPAR(),
		"co2":           h.mockServer.GetCO2(),
	})
}
