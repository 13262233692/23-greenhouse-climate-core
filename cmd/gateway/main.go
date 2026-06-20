package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"

	"greenhouse-climate-core/internal/application/service"
	"greenhouse-climate-core/internal/domain/entity"
	"greenhouse-climate-core/internal/domain/repository"
	"greenhouse-climate-core/internal/infrastructure/database"
	"greenhouse-climate-core/internal/infrastructure/plc"
	"greenhouse-climate-core/internal/infrastructure/sensor"
	"greenhouse-climate-core/internal/interfaces/api"
	"greenhouse-climate-core/internal/interfaces/middleware"
)

var (
	logger        *logrus.Logger
	ginMode       = flag.String("gin-mode", "release", "Gin mode (debug, release, test)")
	apiPort       = flag.String("api-port", "8080", "API server port")
	useMock       = flag.Bool("use-mock", true, "Use mock sensor servers")
	mockStartPort = flag.Int("mock-start-port", 5001, "Start port for mock sensor servers")
	plcAddress    = flag.String("plc-address", "127.0.0.1", "PLC server address")
	plcPort       = flag.Int("plc-port", 502, "PLC server port")
	greenhouseID  = flag.String("greenhouse-id", "GH-001", "Greenhouse ID")
	logLevel      = flag.String("log-level", "info", "Log level (trace, debug, info, warn, error)")
	latitude      = flag.Float64("latitude", 39.9042, "Greenhouse latitude for sun calculation")
	longitude     = flag.Float64("longitude", 116.4074, "Greenhouse longitude for sun calculation")
	ledAddress    = flag.String("led-address", "127.0.0.1", "LED controller address")
	ledPort       = flag.Int("led-port", 503, "LED controller port")
	targetDLI     = flag.Float64("target-dli", 15.0, "Target DLI in mol/m²/d (default 15 for tomato)")
)

func init() {
	logger = logrus.New()
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05",
	})
	logger.SetOutput(os.Stdout)
}

func main() {
	flag.Parse()

	setLogLevel(*logLevel)
	gin.SetMode(*ginMode)

	logger.Infof("Starting Greenhouse Climate Control Gateway...")
	logger.Infof("Greenhouse ID: %s", *greenhouseID)
	logger.Infof("Use mock sensors: %v", *useMock)
	logger.Infof("API Port: %s", *apiPort)
	logger.Infof("Location: lat=%.4f, lng=%.4f", *latitude, *longitude)
	logger.Infof("Target DLI: %.1f mol/m²/d", *targetDLI)

	sensorRepo := sensor.NewInMemorySensorRepository()
	readingRepo := sensor.NewInMemorySensorReadingRepository(10000)
	vpdRepo := database.NewInMemoryVPDRepository(10000)
	plcRepo := database.NewInMemoryPLCCommandRepository()
	tsRepo := database.NewInfluxDBClient(logger, true)

	dliRepo := database.NewInMemoryDLIRepository()
	planRepo := database.NewInMemoryLightPlanRepository()
	ledRepo := database.NewInMemoryLEDDeviceRepository()

	sensorFactory := sensor.NewSensorFactory()
	sensors := sensorFactory.Create50Sensors()
	for _, s := range sensors {
		if err := sensorRepo.Save(s); err != nil {
			logger.Fatalf("Failed to save sensor: %v", err)
		}
	}
	logger.Infof("Initialized %d sensors", len(sensors))

	var parSensorIDs []uint16
	for _, s := range sensors {
		if s.Type == entity.SensorTypePAR {
			parSensorIDs = append(parSensorIDs, s.ID)
		}
	}
	logger.Infof("Found %d PAR sensors for DLI calculation", len(parSensorIDs))

	var mockServers []*sensor.MockSensorServer
	if *useMock {
		mockServers = startMockServers(sensors)
	}

	sensorClient := sensor.NewTCPSensorClient(logger)
	plcClient := plc.NewPLCClient(*plcAddress, *plcPort, logger)
	ledController := plc.NewLEDController(*ledAddress, *ledPort, logger, ledRepo)

	sunCalc := service.NewSunCalculator(*latitude, *longitude, time.Local)

	vpdCalculator := service.NewVPDCalculatorService(vpdRepo, readingRepo, tsRepo, logger).(*service.VPDCalculatorService)
	ruleEngine := service.NewVPDRuleEngineService(vpdRepo, logger).(*service.VPDRuleEngineService)
	commandGenerator := service.NewPLCCommandGeneratorService().(*service.PLCCommandGeneratorService)

	dliCoordinator := service.NewDLICoordinator(
		dliRepo,
		planRepo,
		ledRepo,
		ledController,
		sunCalc,
		logger,
		*greenhouseID,
		parSensorIDs,
	)
	dliCoordinator.SetTargetDLI(*targetDLI)

	controller := service.NewClimateController(
		sensorClient,
		sensorRepo,
		readingRepo,
		vpdRepo,
		plcRepo,
		tsRepo,
		vpdCalculator,
		ruleEngine,
		commandGenerator,
		plcClient,
		dliCoordinator,
		logger,
		*greenhouseID,
	)

	if err := controller.Start(); err != nil {
		logger.Fatalf("Failed to start climate controller: %v", err)
	}

	r := setupRouter(
		sensorRepo,
		readingRepo,
		sensorClient,
		vpdRepo,
		vpdCalculator,
		ruleEngine,
		plcRepo,
		plcClient,
		dliRepo,
		planRepo,
		ledRepo,
		dliCoordinator,
		controller,
		mockServers,
	)

	srv := &http.Server{
		Addr:    ":" + *apiPort,
		Handler: r,
	}

	go func() {
		logger.Infof("API server starting on port %s", *apiPort)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatalf("Failed to start API server: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.Info("Shutting down gateway...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		logger.Errorf("Server shutdown error: %v", err)
	}

	controller.Stop()

	for _, mockServer := range mockServers {
		mockServer.Stop()
	}

	if influxClient, ok := tsRepo.(influxDBClient); ok {
		influxClient.Close()
	}

	logger.Info("Gateway shutdown complete")
}

func setLogLevel(level string) {
	switch level {
	case "trace":
		logger.SetLevel(logrus.TraceLevel)
	case "debug":
		logger.SetLevel(logrus.DebugLevel)
	case "info":
		logger.SetLevel(logrus.InfoLevel)
	case "warn":
		logger.SetLevel(logrus.WarnLevel)
	case "error":
		logger.SetLevel(logrus.ErrorLevel)
	default:
		logger.SetLevel(logrus.InfoLevel)
	}
}

func startMockServers(sensors []*entity.Sensor) []*sensor.MockSensorServer {
	var servers []*sensor.MockSensorServer
	started := make(map[int]bool)

	for _, s := range sensors {
		if !started[s.Port] {
			mockServer := sensor.NewMockSensorServer("127.0.0.1", s.Port, logger)
			if err := mockServer.Start(); err != nil {
				logger.Warnf("Failed to start mock server on port %d: %v", s.Port, err)
				continue
			}
			servers = append(servers, mockServer)
			started[s.Port] = true
		}
	}

	logger.Infof("Started %d mock sensor servers", len(servers))
	return servers
}

type influxDBClient interface {
	Close()
}

func setupRouter(
	sensorRepo repository.SensorRepository,
	readingRepo repository.SensorReadingRepository,
	sensorClient *sensor.TCPSensorClient,
	vpdRepo repository.VPDRepository,
	vpdCalculator *service.VPDCalculatorService,
	ruleEngine *service.VPDRuleEngineService,
	plcRepo repository.PLCCommandRepository,
	plcClient *plc.PLCClient,
	dliRepo repository.DLIRepository,
	planRepo repository.LightSupplementPlanRepository,
	ledRepo repository.LEDDeviceRepository,
	dliCoordinator *service.DLICoordinator,
	controller *service.ClimateController,
	mockServers []*sensor.MockSensorServer,
) *gin.Engine {
	r := gin.New()

	r.Use(middleware.LoggerMiddleware(logger))
	r.Use(gin.Recovery())
	r.Use(middleware.CORSMiddleware())

	var mockServer *sensor.MockSensorServer
	if len(mockServers) > 0 {
		mockServer = mockServers[0]
	}

	sensorHandler := api.NewSensorHandler(sensorRepo, readingRepo, sensorClient, controller, logger)
	vpdHandler := api.NewVPDHandler(vpdRepo, vpdCalculator, ruleEngine, controller, logger)
	plcHandler := api.NewPLCHandler(plcRepo, plcClient, controller, logger)
	systemHandler := api.NewSystemHandler(controller, mockServer, logger, *useMock)
	dliHandler := api.NewDLIHandler(dliRepo, planRepo, ledRepo, dliCoordinator, logger)

	apiV1 := r.Group("/api/v1")
	{
		sensors := apiV1.Group("/sensors")
		{
			sensors.GET("", sensorHandler.GetSensors)
			sensors.GET("/status", sensorHandler.GetSensorStatus)
			sensors.GET("/readings", sensorHandler.GetAllLatestReadings)
			sensors.GET("/:id", sensorHandler.GetSensorByID)
			sensors.GET("/:id/reading", sensorHandler.GetLatestReading)
			sensors.POST("/:id/reconnect", sensorHandler.ReconnectSensor)
		}

		vpd := apiV1.Group("/vpd")
		{
			vpd.GET("", vpdHandler.GetLatestVPD)
			vpd.GET("/history", vpdHandler.GetVPDHistory)
			vpd.GET("/deviation", vpdHandler.GetDeviationStatus)
			vpd.GET("/threshold", vpdHandler.GetThresholdConfig)
			vpd.POST("/calculate", vpdHandler.CalculateVPD)
			vpd.POST("/threshold", vpdHandler.SetThreshold)
			vpd.POST("/reset-cooldown", vpdHandler.ResetCooldown)
		}

		plc := apiV1.Group("/plc")
		{
			plc.GET("/status", plcHandler.GetPLCStatus)
			plc.GET("/commands", plcHandler.GetCommandHistory)
			plc.GET("/commands/pending", plcHandler.GetPendingCommands)
			plc.GET("/commands/:id", plcHandler.GetCommandByID)
			plc.POST("/mist-cooling", plcHandler.TriggerMistCooling)
			plc.POST("/co2-control", plcHandler.TriggerCO2Control)
			plc.POST("/stop/:device_id", plcHandler.StopDevice)
		}

		dli := apiV1.Group("/dli")
		{
			dli.GET("", dliHandler.GetCurrentDLI)
			dli.GET("/status", dliHandler.GetDLIStatus)
			dli.GET("/history", dliHandler.GetDLIHistory)
			dli.GET("/stats", dliHandler.GetDLIStats)
			dli.GET("/threshold", dliHandler.GetThresholdConfig)
			dli.GET("/sun-times", dliHandler.GetSunTimes)
			dli.POST("/target", dliHandler.SetTargetDLI)

			plans := dli.Group("/plans")
			{
				plans.GET("/active", dliHandler.GetActivePlan)
				plans.GET("/history", dliHandler.GetPlanHistory)
				plans.POST("/start", dliHandler.StartSupplement)
				plans.POST("/stop", dliHandler.StopSupplement)
			}

			leds := dli.Group("/leds")
			{
				leds.GET("", dliHandler.GetLEDDevices)
				leds.GET("/zone/:zone", dliHandler.GetLEDByZone)
			}
		}

		system := apiV1.Group("/system")
		{
			system.GET("/health", systemHandler.GetHealth)
			system.GET("/status", systemHandler.GetStatus)
			if *useMock {
				system.GET("/mock-environment", systemHandler.GetMockEnvironment)
				system.POST("/mock-environment", systemHandler.SetMockEnvironment)
			}
		}
	}

	return r
}
