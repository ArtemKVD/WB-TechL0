package main

import (
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ArtemKVD/WB-TechL0/internal/config"
	"github.com/ArtemKVD/WB-TechL0/internal/logger"
	"github.com/ArtemKVD/WB-TechL0/pkg/faker"

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

	defer func() {
		err := w.Close()
		if err != nil {
			logger.Log.Error("Error close kafka writer:", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	orders := faker.GenerateTestOrders(10)

	for _, order := range orders {
		select {
		case <-ctx.Done():
			logger.Log.Info("Shutting down producer")
			return
		default:
			sendOrder, err := json.Marshal(order)
			if err != nil {
				logger.Log.Error("marshal order error: ", err)
				continue
			}

			err = w.WriteMessages(ctx,
				kafka.Message{
					Value: sendOrder,
				},
			)

			if err != nil {
				logger.Log.Error("write message error: ", err)
				continue
			}

			logger.Log.WithField("order_uid", order.OrderUID).Info("order written to kafka")
			time.Sleep(1 * time.Microsecond)
		}
	}
}
