package database

import (
	"database/sql"

	"github.com/ArtemKVD/WB-TechL0/internal/logger"
	"github.com/ArtemKVD/WB-TechL0/pkg/models"
)

func LoadOrdersFromDB(db *sql.DB, cache map[string]models.Order) error {
	limit := 5
	tx, err := db.Begin()
	if err != nil {
		logger.Log.Error("begin transaction error", err)
		return err
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
	defer rows.Close()

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
			continue
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
