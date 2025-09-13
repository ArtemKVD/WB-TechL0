package validator

import (
	"fmt"
	"regexp"
	"time"

	"github.com/ArtemKVD/WB-TechL0/pkg/models"
)

var (
	emailRegex = regexp.MustCompile(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`)
	phoneRegex = regexp.MustCompile(`^\+[1-9]\d{1,14}$`)
)

func ValidateOrder(order models.Order) error {
	err := validateOrderBasic(order)
	if err != nil {
		return err
	}
	err = validateDelivery(order.Delivery)
	if err != nil {
		return err
	}
	err = validatePayment(order.Payment)
	if err != nil {
		return err
	}
	err = validateItems(order.Items)
	if err != nil {
		return err
	}
	return nil
}

func validateOrderBasic(order models.Order) error {
	if order.OrderUID == "" {
		return fmt.Errorf("order UID is empty")
	}
	if order.TrackNumber == "" {
		return fmt.Errorf("track number empty")
	}
	if order.Entry == "" {
		return fmt.Errorf("entry empty")
	}
	if _, err := time.Parse(time.RFC3339, order.DateCreated); err != nil {
		return fmt.Errorf("invalid date: %v", err)
	}
	return nil
}

func validateDelivery(delivery models.Delivery) error {
	if delivery.Name == "" {
		return fmt.Errorf("delivery name is empty")
	}
	if delivery.Phone != "" && !phoneRegex.MatchString(delivery.Phone) {
		return fmt.Errorf("invalid phone")
	}
	if delivery.Email != "" && !emailRegex.MatchString(delivery.Email) {
		return fmt.Errorf("invalid email")
	}
	return nil
}

func validatePayment(payment models.Payment) error {
	if payment.Transaction == "" {
		return fmt.Errorf("payment transaction empty")
	}
	if payment.Amount <= 0 {
		return fmt.Errorf("payment amount must <= 0")
	}
	if payment.DeliveryCost < 0 {
		return fmt.Errorf("delivery cost < 0")
	}
	return nil
}

func validateItems(items []models.Item) error {
	if len(items) == 0 {
		return fmt.Errorf("order must contain item")
	}
	for _, item := range items {
		if item.Name == "" {
			return fmt.Errorf("item name empty")
		}
		if item.Price <= 0 {
			return fmt.Errorf("item price <= 0")
		}
	}
	return nil
}
