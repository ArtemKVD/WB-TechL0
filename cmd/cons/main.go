package main

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/ArtemKVD/WB-TechL0/internal/api"
	"github.com/ArtemKVD/WB-TechL0/internal/cache"
	"github.com/ArtemKVD/WB-TechL0/internal/config"
	"github.com/ArtemKVD/WB-TechL0/internal/logger"
	database "github.com/ArtemKVD/WB-TechL0/internal/storage"
	"github.com/ArtemKVD/WB-TechL0/pkg/models"
	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

func loadCacheFromDB(db *sql.DB, cache *cache.Cache) error {
	tempCache := make(map[string]models.Order)

	err := database.LoadOrdersFromDB(db, tempCache)
	if err != nil {
		return err
	} else {
		logger.Log.Info("Cache loaded")
	}

	for _, order := range tempCache {
		cache.Set(order)
	}

	return nil
}

func startHTTPServer(cache *cache.Cache, db *sql.DB, cfg config.HTTPConfig) {
	router := gin.Default()
	handler := api.NewHandler(cache, db)

	router.LoadHTMLGlob("web/templates/*.html")

	router.GET("/", handler.IndexPage)

	router.GET("/order", handler.GetOrder)

	logger.Log.WithFields(logrus.Fields{
		"port": cfg.Port,
	}).Info("Starting HTTP server")

	err := router.Run(":" + cfg.Port)
	if err != nil {
		logger.Log.WithFields(logrus.Fields{
			"error": err.Error(),
			"port":  cfg.Port,
		}).Fatal("HTTP server failed to start")
	}
}

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

	err = loadCacheFromDB(db, cache)

	if err != nil {
		logger.Log.Warn("error load cache from db", err)
	}
	go startHTTPServer(cache, db, cfg.HTTP)

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
