package api

import (
	"greenhouse-climate-core/internal/application/dto"
	"greenhouse-climate-core/internal/application/service"
	"greenhouse-climate-core/internal/domain/entity"
	"greenhouse-climate-core/internal/domain/repository"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

type VPDHandler struct {
	vpdRepo       repository.VPDRepository
	vpdCalculator *service.VPDCalculatorService
	ruleEngine    *service.VPDRuleEngineService
	controller    *service.ClimateController
	logger        *logrus.Logger
}

func NewVPDHandler(
	vpdRepo repository.VPDRepository,
	vpdCalculator *service.VPDCalculatorService,
	ruleEngine *service.VPDRuleEngineService,
	controller *service.ClimateController,
	logger *logrus.Logger,
) *VPDHandler {
	return &VPDHandler{
		vpdRepo:       vpdRepo,
		vpdCalculator: vpdCalculator,
		ruleEngine:    ruleEngine,
		controller:    controller,
		logger:        logger,
	}
}

func (h *VPDHandler) GetLatestVPD(c *gin.Context) {
	greenhouseID := c.DefaultQuery("greenhouse_id", h.controller.GetGreenhouseID())

	vpd := h.vpdCalculator.GetLatestVPD(greenhouseID)
	if vpd == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "no VPD data available"})
		return
	}

	c.JSON(http.StatusOK, dto.ToVPDDTO(vpd))
}

func (h *VPDHandler) GetVPDHistory(c *gin.Context) {
	greenhouseID := c.DefaultQuery("greenhouse_id", h.controller.GetGreenhouseID())
	durationStr := c.DefaultQuery("duration", "1h")

	duration, err := time.ParseDuration(durationStr)
	if err != nil {
		duration = 1 * time.Hour
	}

	history, err := h.vpdRepo.FindByGreenhouseID(
		greenhouseID,
		time.Now().Add(-duration),
		time.Now(),
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	result := make([]*dto.VPDDTO, 0, len(history))
	for _, v := range history {
		result = append(result, dto.ToVPDDTO(v))
	}

	c.JSON(http.StatusOK, result)
}

func (h *VPDHandler) GetDeviationStatus(c *gin.Context) {
	greenhouseID := c.DefaultQuery("greenhouse_id", h.controller.GetGreenhouseID())

	shouldTrigger, vpd, deviation, err := h.ruleEngine.EvaluateGreenhouse(greenhouseID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if vpd == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "no VPD data available"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"vpd":                 dto.ToVPDDTO(vpd),
		"continuous_deviation": deviation.String(),
		"deviation_seconds":    deviation.Seconds(),
		"threshold_seconds":    180,
		"should_trigger":      shouldTrigger,
		"threshold_range": gin.H{
			"min": entity.VPDMinThreshold,
			"max": entity.VPDMaxThreshold,
		},
	})
}

func (h *VPDHandler) CalculateVPD(c *gin.Context) {
	var req struct {
		AirTemp     float64 `json:"air_temp" binding:"required"`
		AirHumidity float64 `json:"air_humidity" binding:"required"`
		LeafTemp    float64 `json:"leaf_temp" binding:"required"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	vpd := h.vpdCalculator.Calculate(req.AirTemp, req.AirHumidity, req.LeafTemp)
	vpd.GreenhouseID = h.controller.GetGreenhouseID()

	h.vpdRepo.Save(vpd)

	c.JSON(http.StatusOK, dto.ToVPDDTO(vpd))
}

func (h *VPDHandler) SetThreshold(c *gin.Context) {
	secondsStr := c.Query("seconds")
	if secondsStr == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "seconds parameter required"})
		return
	}

	seconds, err := strconv.Atoi(secondsStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid seconds value"})
		return
	}

	h.ruleEngine.SetContinuousThreshold(time.Duration(seconds) * time.Second)

	c.JSON(http.StatusOK, gin.H{
		"message":            "threshold updated",
		"threshold_seconds":  seconds,
		"threshold_duration": time.Duration(seconds) * time.Second,
	})
}

func (h *VPDHandler) ResetCooldown(c *gin.Context) {
	greenhouseID := c.DefaultQuery("greenhouse_id", h.controller.GetGreenhouseID())
	h.ruleEngine.ResetCooldown(greenhouseID)
	c.JSON(http.StatusOK, gin.H{"message": "cooldown reset"})
}

func (h *VPDHandler) GetThresholdConfig(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"vpd_range": gin.H{
			"min":     entity.VPDMinThreshold,
			"max":     entity.VPDMaxThreshold,
			"unit":    "kPa",
		},
		"continuous_threshold_seconds": 180,
		"cooldown_period_seconds":      60,
		"history_window_seconds":       300,
	})
}
