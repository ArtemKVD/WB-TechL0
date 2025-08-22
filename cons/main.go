package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"

	"github.com/ArtemKVD/WB-TechL0/cache"
	"github.com/ArtemKVD/WB-TechL0/models"
	database "github.com/ArtemKVD/WB-TechL0/storage"
	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

func loadCacheFromDB(db *sql.DB, cache *cache.Cache) error {
	tempCache := make(map[string]models.Order)

	err := database.LoadRecentOrdersFromDB(db, tempCache)
	if err != nil {
		return err
	} else {
		log.Printf("cache loaded")
	}

	for _, order := range tempCache {
		cache.Set(order)
	}

	return nil
}
func main() {
	topic := "orders"
	broker := "kafka:29092"
	groupID := "order-consumers"

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker},
		GroupID: groupID,
		Topic:   topic,
	})

	defer r.Close()

	cache := cache.NewCache()

	db, err := sql.Open("postgres", database.GetConnString())
	if err != nil {
		log.Printf("db connection error: %v", err)
	}

	defer db.Close()

	err = loadCacheFromDB(db, cache)

	if err != nil {
		log.Print("error load cache from db", err)
	}

	for {
		message, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Fatalf("read message error: %v", err)
		}

		var order models.Order
		err = json.Unmarshal(message.Value, &order)
		if err != nil {
			log.Printf("unmarshal message error: %v", err)
		}

		cache.Set(order)
		log.Printf("data in map")
		err = database.SaveOrder(db, order)
		if err != nil {
			log.Printf("save order error: %v, %v", order.OrderUID, err)
			continue
		}
		log.Printf("data in db")
	}
}
