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
	logger.Log.Info("Consumer starting")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	topic := cfg.Kafka.Topic
	broker := cfg.Kafka.Broker
	groupID := cfg.Kafka.GroupID

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker},
		GroupID: groupID,
		Topic:   topic,
	})
	defer func() {
		err := r.Close()
		if err != nil {
			logger.Log.Error("Failed to close Kafka reader: ", err)
		}
	}()

	cacheService := cache.NewCache()

	dbStorage := database.NewDatabase(cfg.Database)
	err := dbStorage.Connect()
	if err != nil {
		logger.Log.Fatal("error connecting to db: ", err)
	}

	defer func() {
		err := dbStorage.Close()
		if err != nil {
			logger.Log.Error("Failed to close dbstorage: ", err)
		}
	}()

	err = cacheService.LoadCacheFromDB(dbStorage)
	if err != nil {
		logger.Log.Warn("error load cache from db: ", err)
	}

	httpServer := server.NewServer(cacheService, dbStorage, cfg.HTTP)
	go httpServer.Start()

	go func() {
		<-ctx.Done()
		logger.Log.Info("shutting down gracefully")

		err := r.Close()
		if err != nil {
			logger.Log.Error("error closing kafka reader: ", err)
		}

		os.Exit(0)
	}()

	for {
		message, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			logger.Log.Error("Error read message from kafka: ", err)
			continue
		}

		var order models.Order
		err = json.Unmarshal(message.Value, &order)
		if err != nil {
			logger.Log.Error("Unmarshal message error: ", err)
			continue
		}

		err = validator.ValidateOrder(order)
		if err != nil {
			logger.Log.Error("Order validation failed: ", err)
			continue
		}

		cacheService.Set(order)
		logger.Log.Info("Order saved in cache")

		err = dbStorage.SaveOrder(order)
		if err != nil {
			logger.Log.Error("save order error: ", err)
			continue
		}
		logger.Log.Info("Order saved in database")
	}
}
