package database

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/ArtemKVD/WB-TechL0/internal/config"
	"github.com/ArtemKVD/WB-TechL0/internal/logger"
	"github.com/ArtemKVD/WB-TechL0/pkg/models"
	"github.com/joho/godotenv"
)

type OrderStorage interface {
	SaveOrder(order models.Order) error
	GetOrder(orderUID string) (models.Order, error)
	LoadOrdersFromDB() (map[string]models.Order, error)
	GetConnString() string
	Connect() error
	Close() error
}

type Database struct {
	db  *sql.DB
	cfg config.DatabaseConfig
}

func NewDatabase(cfg config.DatabaseConfig) *Database {
	return &Database{cfg: cfg}
}

func (d *Database) GetConnString() string {
	err := godotenv.Load()
	if err != nil {
		logger.Log.Error("godotenv error: ", err)
	}
	return getConnString(d.cfg)
}

func (d *Database) SaveOrder(order models.Order) error {
	return saveOrder(d.db, order)
}

func (d *Database) GetOrder(orderUID string) (models.Order, error) {
	return getOrderFromDB(d.db, orderUID)
}

func (d *Database) LoadOrdersFromDB() (map[string]models.Order, error) {
	cache := make(map[string]models.Order)
	err := loadOrdersFromDB(d.db, cache)
	return cache, err
}

func (d *Database) Close() error {
	if d.db != nil {
		return d.db.Close()
	}
	return nil
}

func getConnString(cfg config.DatabaseConfig) string {
	err := godotenv.Load()
	if err != nil {
		logger.Log.Error("godotenv error: ", err)
	}

	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.Name, cfg.SSLMode)
}

func (d *Database) Connect() error {
	db, err := sql.Open("postgres", d.GetConnString())
	if err != nil {
		return err
	}
	d.db = db
	return db.Ping()
}

func saveOrder(db *sql.DB, order models.Order) error {
	tx, err := db.Begin()
	if err != nil {
		logger.Log.Error("Begin transaction error ", err)
		return err
	}
	defer func() {
		err := tx.Rollback()
		if err != nil {
			logger.Log.Error("Rollback error: ", err)
		}
	}()

	_, err = tx.Exec(
		`INSERT INTO orders (order_uid, track_number, entry, locale, internal_signature, customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		order.OrderUID, order.TrackNumber, order.Entry, order.Locale, order.InternalSignature, order.CustomerID, order.DeliveryService, order.ShardKey, order.SMID, order.DateCreated, order.OOFShard,
	)
	if err != nil {
		return err
	}

	_, err = tx.Exec(
		`INSERT INTO delivery (order_uid, name, phone, zip, city, address, region, email)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		order.OrderUID, order.Delivery.Name, order.Delivery.Phone, order.Delivery.Zip, order.Delivery.City, order.Delivery.Address, order.Delivery.Region, order.Delivery.Email,
	)
	if err != nil {
		return err
	}

	_, err = tx.Exec(
		`INSERT INTO payment (order_uid, transaction, request_id, currency, provider, amount, payment_dt, bank, delivery_cost, goods_total, custom_fee)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		order.OrderUID, order.Payment.Transaction, order.Payment.RequestID, order.Payment.Currency, order.Payment.Provider, order.Payment.Amount, order.Payment.PaymentDt, order.Payment.Bank, order.Payment.DeliveryCost, order.Payment.GoodsTotal, order.Payment.CustomFee,
	)
	if err != nil {
		return err
	}

	for _, item := range order.Items {
		_, err = tx.Exec(
			`INSERT INTO items (order_uid, chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, brand, status)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
			order.OrderUID, item.ChrtID, item.TrackNumber, item.Price, item.RID, item.Name, item.Sale, item.Size, item.TotalPrice, item.NmID, item.Brand, item.Status,
		)
		if err != nil {
			return err
		}
	}
	err = tx.Commit()
	if err != nil {
		logger.Log.Error("Commit transaction error", err)
		return err
	}
	return nil
}

func getOrderFromDB(db *sql.DB, orderUID string) (models.Order, error) {
	tx, err := db.Begin()
	if err != nil {
		logger.Log.Error("Begin transaction error", err)
		return models.Order{}, err
	}
	defer func() {
		err := tx.Rollback()
		if err != nil {
			logger.Log.Error("Rollback error: ", err)
		}
	}()

	query := `
		SELECT 
			o.order_uid, o.track_number, o.entry, o.locale, o.internal_signature,
			o.customer_id, o.delivery_service, o.shardkey, o.sm_id, o.date_created, o.oof_shard,
			d.name, d.phone, d.zip, d.city, d.address, d.region, d.email,
			p.transaction, p.request_id, p.currency, p.provider, p.amount, p.payment_dt,
			p.bank, p.delivery_cost, p.goods_total, p.custom_fee,
			i.chrt_id, i.track_number as item_track_number, i.price, i.rid, i.name as item_name,
			i.sale, i.size, i.total_price, i.nm_id, i.brand, i.status
		FROM orders o
		INNER JOIN delivery d ON o.order_uid = d.order_uid
		INNER JOIN payment p ON o.order_uid = p.order_uid
		LEFT JOIN items i ON o.order_uid = i.order_uid
		WHERE o.order_uid = $1
		ORDER BY i.chrt_id
	`

	rows, err := tx.Query(query, orderUID)
	if err != nil {
		return models.Order{}, err
	}
	defer func() {
		err := rows.Close()
		if err != nil {
			logger.Log.Error("Rows close error: ", err)
		}
	}()

	var order models.Order
	var currentOrderUID string
	itemsMap := make(map[int]models.Item)

	for rows.Next() {
		var (
			orderUID, trackNumber, entry, locale, internalSignature string
			customerID, deliveryService, shardkey, oofShard         string
			smID                                                    int
			dateCreated                                             string
			name, phone, zip, city, address, region, email          string
			transaction, requestID, currency, provider, bank        string
			amount, deliveryCost, goodsTotal, customFee             int
			paymentDt                                               int
			chrtID                                                  int
			itemTrackNumber, rid, itemName, size, brand             string
			price, sale, totalPrice, nmID, status                   int
		)

		err := rows.Scan(
			&orderUID, &trackNumber, &entry, &locale, &internalSignature,
			&customerID, &deliveryService, &shardkey, &smID, &dateCreated, &oofShard,
			&name, &phone, &zip, &city, &address, &region, &email,
			&transaction, &requestID, &currency, &provider, &amount, &paymentDt,
			&bank, &deliveryCost, &goodsTotal, &customFee,
			&chrtID, &itemTrackNumber, &price, &rid, &itemName,
			&sale, &size, &totalPrice, &nmID, &brand, &status,
		)
		err = rows.Err()
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				logger.Log.Error("Order not found ", err)
				return models.Order{}, err
			}
			logger.Log.Error("Iterating rows error ", err)
			return models.Order{}, err
		}

		if currentOrderUID != orderUID {
			order = models.Order{
				OrderUID:          orderUID,
				TrackNumber:       trackNumber,
				Entry:             entry,
				Locale:            locale,
				InternalSignature: internalSignature,
				CustomerID:        customerID,
				DeliveryService:   deliveryService,
				ShardKey:          shardkey,
				SMID:              smID,
				DateCreated:       dateCreated,
				OOFShard:          oofShard,
				Delivery: models.Delivery{
					Name:    name,
					Phone:   phone,
					Zip:     zip,
					City:    city,
					Address: address,
					Region:  region,
					Email:   email,
				},
				Payment: models.Payment{
					Transaction:  transaction,
					RequestID:    requestID,
					Currency:     currency,
					Provider:     provider,
					Bank:         bank,
					Amount:       amount,
					PaymentDt:    paymentDt,
					DeliveryCost: deliveryCost,
					GoodsTotal:   goodsTotal,
					CustomFee:    customFee,
				},
				Items: []models.Item{},
			}
			currentOrderUID = orderUID
		}

		if chrtID != 0 {
			item := models.Item{
				ChrtID:      chrtID,
				TrackNumber: itemTrackNumber,
				Price:       price,
				RID:         rid,
				Name:        itemName,
				Sale:        sale,
				Size:        size,
				TotalPrice:  totalPrice,
				NmID:        nmID,
				Brand:       brand,
				Status:      status,
			}
			itemsMap[chrtID] = item
		}
	}

	for _, item := range itemsMap {
		order.Items = append(order.Items, item)
	}

	err = tx.Commit()
	if err != nil {
		logger.Log.Error("commit transaction error ", err)
		return models.Order{}, err
	}

	return order, nil
}

func loadOrdersFromDB(db *sql.DB, cache map[string]models.Order) error {
	limit := 5
	tx, err := db.Begin()
	if err != nil {
		logger.Log.Error("begin transaction error", err)
		return err
	}
	defer func() {
		err := tx.Rollback()
		if err != nil {
			logger.Log.Error("Rollback error: ", err)
		}
	}()

	query := `
		SELECT 
			o.order_uid, o.track_number, o.entry, o.locale, o.internal_signature,
			o.customer_id, o.delivery_service, o.shardkey, o.sm_id, o.date_created, o.oof_shard,
			d.name, d.phone, d.zip, d.city, d.address, d.region, d.email,
			p.transaction, p.request_id, p.currency, p.provider, p.amount, p.payment_dt,
			p.bank, p.delivery_cost, p.goods_total, p.custom_fee,
			i.chrt_id, i.track_number as item_track_number, i.price, i.rid, i.name as item_name,
			i.sale, i.size, i.total_price, i.nm_id, i.brand, i.status
		FROM orders o
		INNER JOIN delivery d ON o.order_uid = d.order_uid
		INNER JOIN payment p ON o.order_uid = p.order_uid
		LEFT JOIN items i ON o.order_uid = i.order_uid
		WHERE o.order_uid IN (
			SELECT order_uid 
			FROM orders 
			ORDER BY date_created DESC 
			LIMIT $1
		)
		ORDER BY o.order_uid, i.chrt_id
	`

	rows, err := tx.Query(query, limit)
	if err != nil {
		return err
	}
	defer func() {
		err := rows.Close()
		if err != nil {
			logger.Log.Error("Rows close error: ", err)
		}
	}()

	for rows.Next() {
		var orderUID, trackNumber, entry, locale, internalSignature, customerID, deliveryService, shardkey, oofShard, dateCreated, name, phone, zip, city, address, region, email, transaction, requestID, currency, provider, bank, itemTrackNumber, rid, itemName, size, brand string
		var smID, amount, deliveryCost, goodsTotal, customFee, paymentDt, chrtID, price, sale, totalPrice, nmID, status int

		err := rows.Scan(
			&orderUID, &trackNumber, &entry, &locale, &internalSignature, &customerID, &deliveryService, &shardkey, &smID, &dateCreated, &oofShard,
			&name, &phone, &zip, &city, &address, &region, &email, &transaction, &requestID, &currency, &provider, &amount, &paymentDt,
			&bank, &deliveryCost, &goodsTotal, &customFee, &chrtID, &itemTrackNumber, &price, &rid, &itemName, &sale, &size, &totalPrice, &nmID, &brand, &status,
		)
		if err != nil {
			logger.Log.Error("Error scanning row ", err)
			return err
		}
		err = rows.Err()
		if err != nil {
			logger.Log.Error("rows iterating error")
			return fmt.Errorf("rows iterating error")
		}

		order, exists := cache[orderUID]
		if !exists {
			order = models.Order{
				OrderUID:          orderUID,
				TrackNumber:       trackNumber,
				Entry:             entry,
				Locale:            locale,
				InternalSignature: internalSignature,
				CustomerID:        customerID,
				DeliveryService:   deliveryService,
				ShardKey:          shardkey,
				SMID:              smID,
				DateCreated:       dateCreated,
				OOFShard:          oofShard,
				Delivery: models.Delivery{
					Name:    name,
					Phone:   phone,
					Zip:     zip,
					City:    city,
					Address: address,
					Region:  region,
					Email:   email,
				},
				Payment: models.Payment{
					Transaction:  transaction,
					RequestID:    requestID,
					Currency:     currency,
					Provider:     provider,
					Bank:         bank,
					Amount:       amount,
					PaymentDt:    paymentDt,
					DeliveryCost: deliveryCost,
					GoodsTotal:   goodsTotal,
					CustomFee:    customFee,
				},
				Items: []models.Item{},
			}
		}

		{
			item := models.Item{
				ChrtID:      chrtID,
				TrackNumber: itemTrackNumber,
				Price:       price,
				RID:         rid,
				Name:        itemName,
				Sale:        sale,
				Size:        size,
				TotalPrice:  totalPrice,
				NmID:        nmID,
				Brand:       brand,
				Status:      status,
			}
			order.Items = append(order.Items, item)
		}

		cache[orderUID] = order
	}

	err = tx.Commit()
	if err != nil {
		logger.Log.Error("transaction commit error", err)
		return err
	}

	logger.Log.Info("orders load in cache")
	return nil
}
