package main

import (
	"context"
	"encoding/json"

	"github.com/ArtemKVD/WB-TechL0/internal/config"
	"github.com/ArtemKVD/WB-TechL0/internal/logger"
	models "github.com/ArtemKVD/WB-TechL0/pkg/models"

	"github.com/segmentio/kafka-go"
)

func main() {
	cfg := config.Load()
	topic := cfg.Kafka.Topic
	broker := "localhost:9092"

	w := &kafka.Writer{
		Addr:     kafka.TCP(broker),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}

	defer w.Close()

	order := createTestOrder()

	SendOrder, err := json.Marshal(order)
	if err != nil {
		logger.Log.Error("marshal order error ", err)
	}

	err = w.WriteMessages(context.Background(),
		kafka.Message{
			Value: SendOrder,
		},
	)

	if err != nil {
		logger.Log.Error("write message error ", err)
	}

	logger.Log.Info("order write")
}

func createTestOrder() models.Order {
	return models.Order{
		OrderUID:    "a563feb7b2b84b6test",
		TrackNumber: "WBILMTESTTRACK",
		Entry:       "WBIL",
		Delivery: models.Delivery{
			Name:    "Test Testov",
			Phone:   "+9720000000",
			Zip:     "2639809",
			City:    "Kiryat Mozkin",
			Address: "Ploshad Mira 15",
			Region:  "Kraiot",
			Email:   "test@gmail.com",
		},
		Payment: models.Payment{
			Transaction:  "b563feb7b2b84b6test",
			RequestID:    "",
			Currency:     "USD",
			Provider:     "wbpay",
			Amount:       1817,
			PaymentDt:    1637907727,
			Bank:         "alpha",
			DeliveryCost: 1500,
			GoodsTotal:   317,
			CustomFee:    0,
		},
		Items: []models.Item{
			{
				ChrtID:      9934930,
				TrackNumber: "WBILMTESTTRACK",
				Price:       453,
				RID:         "ab4219087a764ae0btest",
				Name:        "Mascaras",
				Sale:        30,
				Size:        "0",
				TotalPrice:  317,
				NmID:        2389212,
				Brand:       "Vivienne Sabo",
				Status:      202,
			},
		},
		Locale:            "en",
		InternalSignature: "",
		CustomerID:        "test",
		DeliveryService:   "meest",
		ShardKey:          "9",
		SMID:              99,
		DateCreated:       "2021-11-26T06:22:19Z",
		OOFShard:          "1",
	}
}
