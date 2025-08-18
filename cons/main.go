package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/ArtemKVD/WB-TechL0/models"
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

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Fatalf("read message error: %v", err)
		}

		var order models.Order
		err = json.Unmarshal(m.Value, &order)
		if err != nil {
			log.Printf("unmarshal message error: %v", err)
		}

		log.Printf(order.OrderUID)
	}
}
