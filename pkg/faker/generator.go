package faker

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/ArtemKVD/WB-TechL0/pkg/models"
	"github.com/brianvoe/gofakeit/v6"
)

func GenerateTestOrders(count int) []models.Order {
	var orders []models.Order

	for i := 0; i < count; i++ {
		orders = append(orders, generateOrder())
	}

	return orders
}

func generateOrder() models.Order {
	orderUID := gofakeit.UUID()

	return models.Order{
		OrderUID:          orderUID,
		TrackNumber:       fmt.Sprintf("WBIL%08d", rand.Intn(100000000)),
		Entry:             "WBIL",
		Locale:            "en",
		InternalSignature: "",
		CustomerID:        gofakeit.UUID(),
		DeliveryService:   "meest",
		ShardKey:          fmt.Sprintf("%d", rand.Intn(10)),
		SMID:              rand.Intn(100),
		DateCreated:       time.Now().Format(time.RFC3339),
		OOFShard:          "1",
		Delivery: models.Delivery{
			Name:    gofakeit.Name(),
			Phone:   fmt.Sprintf("+%d", 79000000000+rand.Intn(1000000000)),
			Zip:     gofakeit.Zip(),
			City:    gofakeit.City(),
			Address: gofakeit.Street() + " " + gofakeit.DigitN(2),
			Region:  gofakeit.State(),
			Email:   gofakeit.Email(),
		},
		Payment: models.Payment{
			Transaction:  gofakeit.UUID(),
			RequestID:    "",
			Currency:     "USD",
			Provider:     "wbpay",
			Amount:       rand.Intn(10000) + 100,
			PaymentDt:    int(time.Now().Unix()),
			Bank:         "alpha",
			DeliveryCost: rand.Intn(2000) + 500,
			GoodsTotal:   rand.Intn(1000) + 100,
			CustomFee:    0,
		},
		Items: generateItems(orderUID, rand.Intn(3)+1),
	}
}

func generateItems(orderUID string, count int) []models.Item {
	var items []models.Item

	for i := 0; i < count; i++ {
		items = append(items, models.Item{
			ChrtID:      rand.Intn(10000000),
			TrackNumber: fmt.Sprintf("WBIL%08d", rand.Intn(100000000)),
			Price:       rand.Intn(1000) + 50,
			RID:         gofakeit.UUID(),
			Name:        gofakeit.ProductName(),
			Sale:        rand.Intn(50),
			Size:        fmt.Sprintf("%d", rand.Intn(5)),
			TotalPrice:  rand.Intn(500) + 50,
			NmID:        rand.Intn(1000000),
			Brand:       gofakeit.Company(),
			Status:      202,
		})
	}

	return items
}
