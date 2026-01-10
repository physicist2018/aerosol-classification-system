// config/config.go
package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	// Redis
	RedisURL      string `mapstructure:"redis_url"`
	RedisPassword string `mapstructure:"redis_password"`
	RedisDB       int    `mapstructure:"redis_db"`
	StreamName    string `mapstructure:"redis_stream"`
	ConsumerGroup string `mapstructure:"redis_consumer_group"`

	// RethinkDB
	RethinkDBURL string `mapstructure:"rethinkdb_url"`
	DBName       string `mapstructure:"db_name"`
	TableName    string `mapstructure:"table_name"`

	// Server
	ServerPort string `mapstructure:"server_port"`
	HealthPort string `mapstructure:"health_port"`

	// Worker
	WorkerCount int           `mapstructure:"worker_count"`
	TaskTimeout time.Duration `mapstructure:"task_timeout"`
	BatchSize   int           `mapstructure:"batch_size"`
	MaxRetries  int           `mapstructure:"max_retries"`
}

func Load() (*Config, error) {
	// Устанавливаем значения по умолчанию
	viper.SetDefault("redis_url", "lidarbackup.dvo.ru:49155")
	viper.SetDefault("redis_password", "")
	viper.SetDefault("redis_db", 0)
	viper.SetDefault("redis_stream", "tasks-stream")
	viper.SetDefault("redis_consumer_group", "tasks-workers")
	viper.SetDefault("rethinkdb_url", "lidarbackup.dvo.ru:49160")
	viper.SetDefault("db_name", "aerosol_classification_system")
	viper.SetDefault("table_name", "classification_results")
	viper.SetDefault("server_port", ":8081")
	viper.SetDefault("health_port", ":8082")
	viper.SetDefault("max_retries", 3)
	viper.SetDefault("worker_count", 1)
	viper.SetDefault("task_timeout", 30*time.Minute)
	viper.SetDefault("batch_size", 10)

	// Чтение из файлов конфигурации (необязательно)
	viper.SetConfigName("config")        // имя файла конфигурации без расширения
	viper.SetConfigType("yaml")          // или "json", "toml"
	viper.AddConfigPath(".")             // поиск в текущей директории
	viper.AddConfigPath("./config")      // поиск в папке config
	viper.AddConfigPath("/etc/appname/") // глобальная конфигурация

	// Чтение переменных окружения
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Пытаемся прочитать конфигурационный файл (если есть)
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			// Файл найден, но есть другие ошибки
			return nil, fmt.Errorf("ошибка чтения конфигурационного файла: %w", err)
		}
		// Файл не найден - это нормально, используем значения по умолчанию и env
	}

	// Создаем структуру конфигурации
	var cfg Config

	// Распаковываем конфигурацию в структуру
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("ошибка разбора конфигурации: %w", err)
	}

	return &cfg, nil
}

// Альтернативная версия Load() с поддержкой только переменных окружения
func LoadFromEnv() (*Config, error) {
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Устанавливаем значения по умолчанию
	viper.SetDefault("REDIS_URL", "lidarbackup.dvo.ru:49155")
	viper.SetDefault("REDIS_PASSWORD", "")
	viper.SetDefault("REDIS_DB", 0)
	viper.SetDefault("REDIS_STREAM", "tasks-stream")
	viper.SetDefault("REDIS_CONSUMER_GROUP", "tasks-workers")
	viper.SetDefault("RETHINKDB_URL", "lidarbackup.dvo.ru:49160")
	viper.SetDefault("DB_NAME", "aerosol_classification_system")
	viper.SetDefault("TABLE_NAME", "classification_results")
	viper.SetDefault("SERVER_PORT", ":8081")
	viper.SetDefault("HEALTH_PORT", ":8082")
	viper.SetDefault("WORKER_COUNT", 1)
	viper.SetDefault("TASK_TIMEOUT", 30*time.Minute)
	viper.SetDefault("BATCH_SIZE", 1)
	viper.SetDefault("MAX_RETRIES", 3)

	var cfg Config

	// Вручную маппим переменные, так как имена env отличаются от тегов
	cfg.RedisURL = viper.GetString("REDIS_URL")
	cfg.RedisPassword = viper.GetString("REDIS_PASSWORD")
	cfg.RedisDB = viper.GetInt("REDIS_DB")
	cfg.StreamName = viper.GetString("REDIS_STREAM")
	cfg.ConsumerGroup = viper.GetString("REDIS_CONSUMER_GROUP")
	cfg.RethinkDBURL = viper.GetString("RETHINKDB_URL")
	cfg.DBName = viper.GetString("DB_NAME")
	cfg.TableName = viper.GetString("TABLE_NAME")
	cfg.ServerPort = viper.GetString("SERVER_PORT")
	cfg.HealthPort = viper.GetString("HEALTH_PORT")
	cfg.WorkerCount = viper.GetInt("WORKER_COUNT")
	cfg.TaskTimeout = viper.GetDuration("TASK_TIMEOUT")
	cfg.BatchSize = viper.GetInt("BATCH_SIZE")
	cfg.MaxRetries = viper.GetInt("MAX_RETRIES")

	return &cfg, nil
}

// // config/config.go
// package config

// import (
// 	"os"
// 	"strconv"
// 	"time"
// )

// type Config struct {
// 	// Redis
// 	RedisURL      string
// 	RedisPassword string
// 	RedisDB       int
// 	StreamName    string
// 	ConsumerGroup string

// 	// RethinkDB
// 	RethinkDBURL string
// 	DBName       string
// 	TableName    string

// 	// Server
// 	ServerPort string
// 	HealthPort string

// 	// Worker
// 	WorkerCount int
// 	TaskTimeout time.Duration
// 	BatchSize   int
// }

// func Load() *Config {
// 	return &Config{
// 		RedisURL:      getEnv("REDIS_URL", "lidarbackup.dvo.ru:49155"),
// 		RedisPassword: getEnv("REDIS_PASSWORD", ""),
// 		RedisDB:       getEnvAsInt("REDIS_DB", 0),
// 		StreamName:    getEnv("REDIS_STREAM", "tasks-stream"),
// 		ConsumerGroup: getEnv("REDIS_CONSUMER_GROUP", "tasks-workers"),
// 		RethinkDBURL:  getEnv("RETHINKDB_URL", "lidarbackup.dvo.ru:49160"),
// 		DBName:        getEnv("DB_NAME", "aerosol_classification_system"),
// 		TableName:     getEnv("TABLE_NAME", "classification_results"),
// 		ServerPort:    getEnv("SERVER_PORT", ":8081"),
// 		HealthPort:    getEnv("HEALTH_PORT", ":8082"),
// 		WorkerCount:   getEnvAsInt("WORKER_COUNT", 1),
// 		TaskTimeout:   getEnvAsDuration("TASK_TIMEOUT", 30*time.Minute),
// 		BatchSize:     getEnvAsInt("BATCH_SIZE", 10),
// 	}
// }

// func getEnv(key, defaultValue string) string {
// 	if value, exists := os.LookupEnv(key); exists {
// 		return value
// 	}
// 	return defaultValue
// }

// func getEnvAsInt(key string, defaultValue int) int {
// 	if value, exists := os.LookupEnv(key); exists {
// 		if intValue, err := strconv.Atoi(value); err == nil {
// 			return intValue
// 		}
// 	}
// 	return defaultValue
// }

// func getEnvAsDuration(key string, defaultValue time.Duration) time.Duration {
// 	if value, exists := os.LookupEnv(key); exists {
// 		if dur, err := time.ParseDuration(value); err == nil {
// 			return dur
// 		}
// 	}
// 	return defaultValue
// }
