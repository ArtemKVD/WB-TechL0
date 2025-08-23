package api

import (
	"database/sql"
	"net/http"

	"github.com/ArtemKVD/WB-TechL0/cache"
	"github.com/ArtemKVD/WB-TechL0/logger"
	database "github.com/ArtemKVD/WB-TechL0/storage"
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
	c.File("./templates/index.html")
}

func (h *Handler) GetOrder(c *gin.Context) {
	orderUID := c.Query("id")

	order, found := h.cache.Get(orderUID)
	if !found {
		var err error
		order, err = database.GetOrderFromDB(h.db, orderUID)
		if err != nil {
			if err == sql.ErrNoRows {
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
