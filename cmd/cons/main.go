package main

import (
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"syscall"

	"github.com/ArtemKVD/WB-TechL0/internal/cache"
	"github.com/ArtemKVD/WB-TechL0/internal/config"
	"github.com/ArtemKVD/WB-TechL0/internal/logger"
	"github.com/ArtemKVD/WB-TechL0/internal/server"
	database "github.com/ArtemKVD/WB-TechL0/internal/storage"
	"github.com/ArtemKVD/WB-TechL0/pkg/models"
	"github.com/ArtemKVD/WB-TechL0/pkg/validator"
	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

func main() {
	logger.Init()
	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	kafkaReader := Kafkainit(cfg)
	defer closeKafka(kafkaReader)

	cacheService := cache.NewCache()
	dbStorage := Databaseinit(cfg)
	defer closeDatabase(dbStorage)

	loadCache(cacheService, dbStorage)
	startServer(cacheService, dbStorage, cfg)
	processMessages(ctx, kafkaReader, cacheService, dbStorage)
}

func Kafkainit(cfg *config.Config) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{cfg.Kafka.Broker},
		GroupID: cfg.Kafka.GroupID,
		Topic:   cfg.Kafka.Topic,
	})
}

func Databaseinit(cfg *config.Config) *database.Database {
	dbStorage := database.NewDatabase(cfg.Database)
	err := dbStorage.Connect()
	if err != nil {
		logger.Log.Fatal("Error connecting to DB: ", err)
	}
	return dbStorage
}

func closeKafka(reader *kafka.Reader) {
	err := reader.Close()
	if err != nil {
		logger.Log.Error("Close Kafka reader error: ", err)
	}
}

func closeDatabase(dbStorage *database.Database) {
	err := dbStorage.Close()
	if err != nil {
		logger.Log.Error("Close DB error: ", err)
	}
}

func loadCache(cacheService *cache.Cache, dbStorage *database.Database) {
	err := cacheService.LoadCacheFromDB(dbStorage)
	if err != nil {
		logger.Log.Error("Error loading cache: ", err)
	}
}

func startServer(cacheService *cache.Cache, dbStorage *database.Database, cfg *config.Config) {
	httpServer := server.NewServer(cacheService, dbStorage, cfg.HTTP)
	go httpServer.Start()
}

func processMessages(ctx context.Context, kafkaReader *kafka.Reader, cacheService *cache.Cache, dbStorage *database.Database) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			processMessage(ctx, kafkaReader, cacheService, dbStorage)
		}
	}
}

func processMessage(ctx context.Context, kafkaReader *kafka.Reader, cacheService *cache.Cache, dbStorage *database.Database) {
	message, err := kafkaReader.ReadMessage(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		logger.Log.Error("Error reading message: ", err)
		return
	}

	var order models.Order
	err = json.Unmarshal(message.Value, &order)
	if err != nil {
		logger.Log.Error("Error unmarshaling message: ", err)
		return
	}

	err = validator.ValidateOrder(order)
	if err != nil {
		logger.Log.Error("Validation failed: ", err)
		return
	}

	cacheService.Set(order)
	logger.Log.Info("Order cached: ", order.OrderUID)

	err = dbStorage.SaveOrder(order)
	if err != nil {
		logger.Log.Error("Error saving order: ", err)
		return
	}
	logger.Log.Info("Order saved to DB: ", order.OrderUID)
}
