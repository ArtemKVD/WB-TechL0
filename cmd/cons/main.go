package main

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/ArtemKVD/WB-TechL0/internal/cache"
	"github.com/ArtemKVD/WB-TechL0/internal/config"
	"github.com/ArtemKVD/WB-TechL0/internal/logger"
	"github.com/ArtemKVD/WB-TechL0/internal/server"
	database "github.com/ArtemKVD/WB-TechL0/internal/storage"
	"github.com/ArtemKVD/WB-TechL0/pkg/models"
	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

func main() {
	cfg := config.Load()
	logger.Init()
	logger.Log.Info("Consumer starting")

	topic := cfg.Kafka.Topic
	broker := cfg.Kafka.Broker
	groupID := cfg.Kafka.GroupID

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker},
		GroupID: groupID,
		Topic:   topic,
	})

	defer r.Close()

	cache := cache.NewCache()

	db, err := sql.Open("postgres", database.GetConnString(cfg.Database))
	if err != nil {
		logger.Log.Fatal("error connect to db")
	}

	defer db.Close()

	err = cache.LoadCacheFromDB(db)

	if err != nil {
		logger.Log.Warn("error load cache from db", err)
	}
	httpServer := server.NewServer(cache, db, cfg.HTTP)
	go httpServer.Start()

	for {
		message, err := r.ReadMessage(context.Background())
		if err != nil {
			logger.Log.Error("Error read message from kafka", err)
		}

		var order models.Order
		err = json.Unmarshal(message.Value, &order)
		if err != nil {
			logger.Log.Error("Unmarshal message error", err)
		}

		cache.Set(order)
		logger.Log.Info("Order saved in cache")
		err = database.SaveOrder(db, order)
		if err != nil {
			logger.Log.Error("save order error", err)
			continue
		}
		logger.Log.Info("Order saved in database")
	}
}
