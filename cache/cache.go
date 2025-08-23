package cache

import (
	"sync"

	"github.com/ArtemKVD/WB-TechL0/logger"
	"github.com/ArtemKVD/WB-TechL0/models"
)

type Cache struct {
	mu     sync.RWMutex
	orders map[string]models.Order
}

func NewCache() *Cache {
	logger.Log.Info("cache initialized")
	return &Cache{
		orders: make(map[string]models.Order),
	}
}

func (cache *Cache) Set(order models.Order) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.orders[order.OrderUID] = order
	logger.Log.WithField(
		"order_uid", order.OrderUID,
	).Info("Order cached")
}

func (cache *Cache) Get(orderUID string) (models.Order, bool) {
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	order, exists := cache.orders[orderUID]
	if exists {
		logger.Log.WithField(
			"order_uid", order.OrderUID,
		).Info("Order found in cache")
	} else {
		logger.Log.WithField(
			"order_uid", order.OrderUID,
		).Info("Order not found in cache")
	}
	return order, exists
}
