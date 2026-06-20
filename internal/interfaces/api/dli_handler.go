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

type DLIHandler struct {
	dliRepo     repository.DLIRepository
	planRepo    repository.LightSupplementPlanRepository
	ledRepo     repository.LEDDeviceRepository
	coordinator *service.DLICoordinator
	logger      *logrus.Logger
}

func NewDLIHandler(
	dliRepo repository.DLIRepository,
	planRepo repository.LightSupplementPlanRepository,
	ledRepo repository.LEDDeviceRepository,
	coordinator *service.DLICoordinator,
	logger *logrus.Logger,
) *DLIHandler {
	return &DLIHandler{
		dliRepo:     dliRepo,
		planRepo:    planRepo,
		ledRepo:     ledRepo,
		coordinator: coordinator,
		logger:      logger,
	}
}

func (h *DLIHandler) GetCurrentDLI(c *gin.Context) {
	greenhouseID := c.DefaultQuery("greenhouse_id", "GH001")

	current := h.coordinator.GetCurrentDLI()
	if current == nil {
		existing, err := h.dliRepo.FindLatest(greenhouseID)
		if err != nil || existing == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "no DLI data available"})
			return
		}
		current = existing
	}

	c.JSON(http.StatusOK, dto.ToDLIDTO(current))
}

func (h *DLIHandler) GetDLIHistory(c *gin.Context) {
	greenhouseID := c.DefaultQuery("greenhouse_id", "GH001")
	durationStr := c.DefaultQuery("duration", "24h")

	duration, err := time.ParseDuration(durationStr)
	if err != nil {
		duration = 24 * time.Hour
	}

	history, err := h.dliRepo.FindHistory(
		greenhouseID,
		time.Now().Add(-duration),
		time.Now(),
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	result := make([]*dto.DLIDTO, 0, len(history))
	for _, d := range history {
		result = append(result, dto.ToDLIDTO(d))
	}

	c.JSON(http.StatusOK, result)
}

func (h *DLIHandler) GetActivePlan(c *gin.Context) {
	plan := h.coordinator.GetActivePlan()
	if plan == nil {
		greenhouseID := c.DefaultQuery("greenhouse_id", "GH001")
		active, err := h.planRepo.FindActive(greenhouseID)
		if err != nil || active == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "no active supplement plan"})
			return
		}
		plan = active
	}

	c.JSON(http.StatusOK, dto.ToLightPlanDTO(plan))
}

func (h *DLIHandler) GetPlanHistory(c *gin.Context) {
	greenhouseID := c.DefaultQuery("greenhouse_id", "GH001")
	durationStr := c.DefaultQuery("duration", "72h")

	duration, err := time.ParseDuration(durationStr)
	if err != nil {
		duration = 72 * time.Hour
	}

	history, err := h.planRepo.FindHistory(
		greenhouseID,
		time.Now().Add(-duration),
		time.Now(),
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	result := make([]*dto.LightPlanDTO, 0, len(history))
	for _, p := range history {
		result = append(result, dto.ToLightPlanDTO(p))
	}

	c.JSON(http.StatusOK, result)
}

func (h *DLIHandler) GetLEDDevices(c *gin.Context) {
	devices, err := h.ledRepo.FindAll()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	result := make([]dto.LEDDeviceDTO, 0, len(devices))
	for _, d := range devices {
		result = append(result, dto.ToLEDDeviceDTO(d))
	}

	c.JSON(http.StatusOK, result)
}

func (h *DLIHandler) GetLEDByZone(c *gin.Context) {
	zone := c.Param("zone")
	if zone == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "zone parameter required"})
		return
	}

	devices, err := h.ledRepo.FindByZone(zone)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	result := make([]dto.LEDDeviceDTO, 0, len(devices))
	for _, d := range devices {
		result = append(result, dto.ToLEDDeviceDTO(d))
	}

	c.JSON(http.StatusOK, result)
}

func (h *DLIHandler) GetSunTimes(c *gin.Context) {
	sunCalc := h.coordinator.GetSunCalculator()
	now := time.Now()
	sunrise, sunset := sunCalc.CalculateSunTimes(now)

	c.JSON(http.StatusOK, dto.ToSunTimesDTO(sunrise, sunset, now))
}

func (h *DLIHandler) SetTargetDLI(c *gin.Context) {
	targetStr := c.Query("target")
	if targetStr == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "target parameter required"})
		return
	}

	target, err := strconv.ParseFloat(targetStr, 64)
	if err != nil || target <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid target value"})
		return
	}

	h.coordinator.SetTargetDLI(target)

	c.JSON(http.StatusOK, gin.H{
		"message":    "target DLI updated",
		"target_dli": target,
		"unit":       "mol/m²/d",
	})
}

func (h *DLIHandler) StartSupplement(c *gin.Context) {
	var req struct {
		TargetPower uint8 `json:"target_power"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		req.TargetPower = 80
	}

	if req.TargetPower == 0 {
		req.TargetPower = 80
	}
	if req.TargetPower > 100 {
		req.TargetPower = 100
	}

	if err := h.coordinator.ManualStartSupplement(req.TargetPower); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message":      "light supplement started",
		"target_power": req.TargetPower,
	})
}

func (h *DLIHandler) StopSupplement(c *gin.Context) {
	if err := h.coordinator.ManualStopSupplement(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "light supplement stopped"})
}

func (h *DLIHandler) GetDLIStats(c *gin.Context) {
	stats := h.coordinator.GetStats()
	c.JSON(http.StatusOK, stats)
}

func (h *DLIHandler) GetThresholdConfig(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"target_dli": gin.H{
			"value": entity.TomatoDLITarget,
			"unit":  "mol/m²/d",
			"crop":  "tomato/solanaceous",
		},
		"sunset_cutoff_hours": 2,
		"smooth_steps":        5,
		"step_duration_min":   10,
		"eval_interval_sec":   30,
	})
}

func (h *DLIHandler) GetDLIStatus(c *gin.Context) {
	current := h.coordinator.GetCurrentDLI()
	plan := h.coordinator.GetActivePlan()
	stats := h.coordinator.GetStats()

	response := gin.H{
		"stats": stats,
	}

	if current != nil {
		response["dli"] = dto.ToDLIDTO(current)

		progress := 0.0
		if current.TargetDLI > 0 {
			progress = (current.AccumulatedDLI / current.TargetDLI) * 100
		}
		if progress > 100 {
			progress = 100
		}
		response["progress_percent"] = progress
		response["needs_supplement"] = current.Deficit > 0 && current.IsNearSunset(entity.DefaultSunsetCutoff)
	}

	if plan != nil {
		response["active_plan"] = dto.ToLightPlanDTO(plan)
	}

	c.JSON(http.StatusOK, response)
}
