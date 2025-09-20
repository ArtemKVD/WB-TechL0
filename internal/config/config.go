package config

import (
	"os"

	"github.com/ArtemKVD/WB-TechL0/internal/logger"
	"github.com/joho/godotenv"
)

type Config struct {
	HTTP     HTTPConfig
	Database DatabaseConfig
	Kafka    KafkaConfig
}

type HTTPConfig struct {
	Port string
}

type DatabaseConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	Name     string
	SSLMode  string
}

type KafkaConfig struct {
	Broker  string
	GroupID string
	Topic   string
}

func Load() *Config {
	err := godotenv.Load()
	if err != nil {
		logger.Log.Error("godotenv error: ", err)
	}

	return &Config{
		HTTP: HTTPConfig{
			Port: os.Getenv("HTTP_PORT"),
		},
		Database: DatabaseConfig{
			Host:     os.Getenv("POSTGRES_HOST"),
			Port:     os.Getenv("POSTGRES_PORT"),
			User:     os.Getenv("POSTGRES_USER"),
			Password: os.Getenv("POSTGRES_PASSWORD"),
			Name:     os.Getenv("POSTGRES_DB"),
			SSLMode:  os.Getenv("POSTGRES_SSLMODE"),
		},
		Kafka: KafkaConfig{
			Broker:  os.Getenv("KAFKA_BROKER"),
			GroupID: os.Getenv("KAFKA_GROUP_ID"),
			Topic:   os.Getenv("KAFKA_TOPIC"),
		},
	}
}
