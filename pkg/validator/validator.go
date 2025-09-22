package validator

import (
	"fmt"
	"log"
	"regexp"
	"time"

	"github.com/ArtemKVD/WB-TechL0/pkg/models"
	"github.com/go-playground/validator/v10"
)

var (
	validate   = validator.New()
	emailRegex = regexp.MustCompile(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`)
	phoneRegex = regexp.MustCompile(`^\+[1-9]\d{1,14}$`)
)

func init() {
	err := validate.RegisterValidation("phone", validatePhone)
	if err != nil {
		log.Println("Error register validation phone: ", err)
	}
	err = validate.RegisterValidation("email", validateEmail)
	if err != nil {
		log.Println("Error register validation email: ", err)
	}
	err = validate.RegisterValidation("timestamp", validateTimestamp)
	if err != nil {
		log.Println("Error register validation timestamp: ", err)
	}
}

func ValidateOrder(order models.Order) error {
	err := validate.Struct(order)
	if err != nil {
		return fmt.Errorf("order validation failed: %v", err)
	}
	err = validateItems(order.Items)
	if err != nil {
		return err
	}

	return nil
}

func validatePhone(fl validator.FieldLevel) bool {
	phone := fl.Field().String()
	return phoneRegex.MatchString(phone)
}

func validateEmail(fl validator.FieldLevel) bool {
	email := fl.Field().String()
	return emailRegex.MatchString(email)
}

func validateTimestamp(fl validator.FieldLevel) bool {
	timestamp := fl.Field().String()
	_, err := time.Parse(time.RFC3339, timestamp)
	return err == nil
}

func validateItems(items []models.Item) error {
	if len(items) == 0 {
		return fmt.Errorf("order item is nil")
	}

	for _, item := range items {
		if item.Price < 0 {
			return fmt.Errorf("item price < 0")
		}
	}

	return nil
}

func ValidateDelivery(delivery models.Delivery) error {
	return validate.Struct(delivery)
}

func ValidatePayment(payment models.Payment) error {
	return validate.Struct(payment)
}

func ValidateItem(item models.Item) error {
	return validate.Struct(item)
}
