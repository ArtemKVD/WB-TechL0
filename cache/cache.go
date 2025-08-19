package cache

import (
	"sync"

	"github.com/ArtemKVD/WB-TechL0/models"
)

type Cache struct {
	mu     sync.RWMutex
	orders map[string]models.Order
}

func NewCache() *Cache {
	return &Cache{
		orders: make(map[string]models.Order),
	}
}

func (cache *Cache) Set(order models.Order) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.orders[order.OrderUID] = order
}
