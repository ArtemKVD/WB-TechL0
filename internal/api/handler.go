package api

import (
	"errors"
	"net/http"

	"github.com/ArtemKVD/WB-TechL0/internal/cache"
	"github.com/ArtemKVD/WB-TechL0/internal/logger"
	database "github.com/ArtemKVD/WB-TechL0/internal/storage"
	"github.com/gin-gonic/gin"
)

type Handler struct {
	cache   cache.CacheService
	storage database.OrderStorage
}

func NewHandler(c cache.CacheService, storage database.OrderStorage) *Handler {
	logger.Log.Info("Handler initialized")
	return &Handler{
		cache:   c,
		storage: storage,
	}
}

func (h *Handler) IndexPage(c *gin.Context) {
	c.HTML(http.StatusOK, "index.html", gin.H{})
}

func (h *Handler) GetOrder(c *gin.Context) {
	orderUID := c.Query("id")

	order, found := h.cache.Get(orderUID)
	if !found {
		var err error
		order, err = h.storage.GetOrder(orderUID)
		if err != nil {
			if errors.Is(err, database.ErrNotFound) {
				c.JSON(http.StatusNotFound, gin.H{"error": "Order not found"})
			} else {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
			}
			return
		}
		h.cache.Set(order)
	}
	logger.Log.Info("order request completed")
	c.HTML(http.StatusOK, "order.html", order)
}
