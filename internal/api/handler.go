package api

import (
	"database/sql"
	"errors"
	"net/http"

	"github.com/ArtemKVD/WB-TechL0/internal/cache"
	"github.com/ArtemKVD/WB-TechL0/internal/logger"
	database "github.com/ArtemKVD/WB-TechL0/internal/storage"
	"github.com/gin-gonic/gin"
)

type Handler struct {
	cache *cache.Cache
	db    *sql.DB
}

func NewHandler(cache *cache.Cache, db *sql.DB) *Handler {
	logger.Log.Info("Handler initialized")
	return &Handler{
		cache: cache,
		db:    db,
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
		order, err = database.GetOrderFromDB(h.db, orderUID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				c.JSON(http.StatusNotFound, gin.H{"error": "Order not found"})
			} else {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
			}
			return
		}
		h.cache.Set(order)
	}
	logger.Log.Info("Order request completed successfully")
	c.HTML(http.StatusOK, "order.html", order)
}
