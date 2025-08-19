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

func main() {
	topic := "orders"
	broker := "localhost:9092"
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
		err = database.SaveOrder(db, order)
		if err != nil {
			log.Printf("save order error: %v, %v", order.OrderUID, err)
			continue
		}
	}
}
