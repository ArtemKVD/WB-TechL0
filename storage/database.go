package database

import (
	"database/sql"
	"fmt"

	"github.com/ArtemKVD/WB-TechL0/config"
	"github.com/ArtemKVD/WB-TechL0/logger"
	"github.com/ArtemKVD/WB-TechL0/models"
	"github.com/joho/godotenv"
)

func GetConnString(cfg config.DatabaseConfig) string {
	godotenv.Load()

	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.Name, cfg.SSLMode)
}

func SaveOrder(db *sql.DB, order models.Order) error {
	tx, err := db.Begin()
	if err != nil {
		logger.Log.Error("Begin transaction error ", err)
		return err
	}
	defer tx.Rollback()

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

func GetOrderFromDB(db *sql.DB, orderUID string) (models.Order, error) {
	tx, err := db.Begin()
	if err != nil {
		logger.Log.Error("Begin transaction error", err)
		return models.Order{}, err
	}
	defer tx.Rollback()

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
	defer rows.Close()

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
		if err != nil {
			logger.Log.Error("Error scan row", err)
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
