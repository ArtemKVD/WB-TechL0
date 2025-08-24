package server

import (
	"database/sql"

	"github.com/ArtemKVD/WB-TechL0/internal/api"
	"github.com/ArtemKVD/WB-TechL0/internal/cache"
	"github.com/ArtemKVD/WB-TechL0/internal/config"
	"github.com/ArtemKVD/WB-TechL0/internal/logger"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

type Server struct {
	router *gin.Engine
	cache  *cache.Cache
	db     *sql.DB
	cfg    config.HTTPConfig
}

func NewServer(cache *cache.Cache, db *sql.DB, cfg config.HTTPConfig) *Server {
	router := gin.Default()
	handler := api.NewHandler(cache, db)

	router.LoadHTMLGlob("web/templates/*.html")
	router.GET("/", handler.IndexPage)
	router.GET("/order", handler.GetOrder)

	return &Server{
		router: router,
		cache:  cache,
		db:     db,
		cfg:    cfg,
	}
}

func (s *Server) Start() {
	logger.Log.WithFields(logrus.Fields{
		"port": s.cfg.Port,
	}).Info("Starting HTTP server")

	err := s.router.Run(":" + s.cfg.Port)
	if err != nil {
		logger.Log.WithFields(logrus.Fields{
			"error": err.Error(),
			"port":  s.cfg.Port,
		}).Fatal("HTTP server failed to start")
	}
}
