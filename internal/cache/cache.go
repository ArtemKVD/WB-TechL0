package cache

import (
	"sync"
	"time"

	"github.com/ArtemKVD/WB-TechL0/internal/logger"
	database "github.com/ArtemKVD/WB-TechL0/internal/storage"
	"github.com/ArtemKVD/WB-TechL0/pkg/models"
)

type Cache struct {
	orders      sync.Map
	accessTimes sync.Map
	maxSize     int
	currentSize int
	ttl         time.Duration
}

//go:generate mockgen -destination=../mocks/cache_mock.go -package=mocks github.com/ArtemKVD/WB-TechL0/internal/cache CacheService
type CacheService interface {
	Set(order models.Order)
	Get(orderUID string) (models.Order, bool)
	LoadCacheFromDB(storage database.OrderStorage) error
	DeleteOldest()
	Clean()
}

func NewCache() *Cache {
	logger.Log.Info("cache initialized")
	return &Cache{
		maxSize: 1000,
		ttl:     15 * time.Minute,
	}
}

func (c *Cache) Set(order models.Order) {
	orderUID := order.OrderUID

	_, exists := c.orders.Load(orderUID)
	if exists {
		c.accessTimes.Store(orderUID, time.Now())
		logger.Log.WithField("order_uid", orderUID).Info("order updated in cache")
		return
	}
	c.Clean()
	if c.currentSize >= c.maxSize {
		c.DeleteOldest()
	}

	c.orders.Store(orderUID, order)
	c.accessTimes.Store(orderUID, time.Now())
	c.currentSize++

	logger.Log.WithField("order_uid", orderUID).Info("Order cached")
}

func (c *Cache) DeleteOldest() {
	var oldestKey string
	var oldestTime time.Time

	c.accessTimes.Range(func(key, value interface{}) bool {
		accessTime := value.(time.Time)
		if oldestKey == "" || accessTime.Before(oldestTime) {
			oldestKey = key.(string)
			oldestTime = accessTime
		}
		return true
	})

	if oldestKey != "" {
		c.orders.Delete(oldestKey)
		c.accessTimes.Delete(oldestKey)
		c.currentSize++
		logger.Log.WithField("order_uid", oldestKey).Info("Oldest order deleted")
	}
}

func (c *Cache) Get(orderUID string) (models.Order, bool) {

	order, exists := c.orders.Load(orderUID)
	if !exists {
		logger.Log.WithField("order_uid", orderUID).Info("Order not found in cache")
		return models.Order{}, false
	}
	c.accessTimes.Store(orderUID, time.Now())
	logger.Log.WithField("order_uid", orderUID).Info("Order found in cache")
	return order.(models.Order), true
}

func (c *Cache) LoadCacheFromDB(storage database.OrderStorage) error {
	tempCache, err := storage.LoadOrdersFromDB()
	if err != nil {
		return err
	}

	now := time.Now()
	for orderUID, order := range tempCache {
		c.orders.Store(orderUID, order)
		c.accessTimes.Store(orderUID, now)
		c.currentSize++
		logger.Log.Info("Order loaded in cache", orderUID)
	}

	return nil
}

func (c *Cache) Clean() {
	now := time.Now()
	c.accessTimes.Range(func(key, value interface{}) bool {
		lastAccess := value.(time.Time)
		if now.Sub(lastAccess) > c.ttl {
			orderUID := key.(string)
			c.orders.Delete(orderUID)
			c.accessTimes.Delete(orderUID)
			c.currentSize--
			logger.Log.WithField("order_uid", orderUID).Info("order removed from cache")
		}
		return true
	})
}
