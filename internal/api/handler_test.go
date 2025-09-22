package api_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ArtemKVD/WB-TechL0/internal/api"
	"github.com/ArtemKVD/WB-TechL0/internal/mocks"
	database "github.com/ArtemKVD/WB-TechL0/internal/storage"
	"github.com/ArtemKVD/WB-TechL0/pkg/models"
	"github.com/gin-gonic/gin"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func setupTestRouter(handler *api.Handler) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()

	router.LoadHTMLGlob("../../web/templates/*")

	router.GET("/", handler.IndexPage)
	router.GET("/order", handler.GetOrder)

	return router
}

func TestHandler_GetOrder(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCache := mocks.NewMockCacheService(ctrl)
	mockStorage := mocks.NewMockOrderStorage(ctrl)

	handler := api.NewHandler(mockCache, mockStorage)
	router := setupTestRouter(handler)

	t.Run("order found in cache", func(t *testing.T) {
		expectedOrder := models.Order{
			OrderUID:    "test1",
			TrackNumber: "WBILMTESTTRACK",
			Entry:       "WBIL",
		}

		mockCache.EXPECT().
			Get("test1").
			Return(expectedOrder, true).
			Times(1)

		w := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/order?id=test1", nil)
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "test1")
	})

	t.Run("order not found in cache and found in database", func(t *testing.T) {
		expectedOrder := models.Order{
			OrderUID:    "test2",
			TrackNumber: "WBILMTESTTRACK",
			Entry:       "WBIL",
		}

		mockCache.EXPECT().
			Get("test2").
			Return(models.Order{}, false).
			Times(1)

		mockStorage.EXPECT().
			GetOrder("test2").
			Return(expectedOrder, nil).
			Times(1)

		mockCache.EXPECT().
			Set(expectedOrder).
			Times(1)

		w := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/order?id=test2", nil)
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "test2")
	})

	t.Run("order not found", func(t *testing.T) {
		mockCache.EXPECT().
			Get("test3").
			Return(models.Order{}, false).
			Times(1)

		mockStorage.EXPECT().
			GetOrder("test3").
			Return(models.Order{}, database.ErrNotFound).
			Times(1)

		w := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/order?id=test3", nil)
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusNotFound, w.Code)
		assert.Contains(t, w.Body.String(), "Order not found")
	})

	t.Run("database error", func(t *testing.T) {
		mockCache.EXPECT().
			Get("test4").
			Return(models.Order{}, false).
			Times(1)

		mockStorage.EXPECT().
			GetOrder("test4").
			Return(models.Order{}, errors.New("database connection failed")).
			Times(1)

		w := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/order?id=test4", nil)
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
		assert.Contains(t, w.Body.String(), "Database error")
	})
}

func TestHandler_IndexPage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCache := mocks.NewMockCacheService(ctrl)
	mockStorage := mocks.NewMockOrderStorage(ctrl)

	handler := api.NewHandler(mockCache, mockStorage)
	router := setupTestRouter(handler)

	t.Run("index page returns HTML form", func(t *testing.T) {
		w := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/", nil)
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Header().Get("Content-Type"), "text/html")
		assert.Contains(t, w.Body.String(), "form")
	})
}
