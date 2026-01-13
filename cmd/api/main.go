// cmd/api/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"aerosol-system/internal/api"
	"aerosol-system/internal/config"
	"aerosol-system/internal/messaging"
	"aerosol-system/internal/repository"

	r "gopkg.in/rethinkdb/rethinkdb-go.v6"
)

func main() {
	// Настройка логгера
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	log.Println("=== Starting REST API Server (Redis) ===")

	// Загружаем конфигурацию
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}
	logConfig(cfg)

	// Создаем контекст для graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Подключаемся к RethinkDB
	rethinkSession, err := connectToRethinkDB(cfg)
	if err != nil {
		log.Fatalf("Failed to connect to RethinkDB: %v", err)
	}
	defer rethinkSession.Close()

	log.Println("✓ Connected to RethinkDB")

	// Инициализируем базу данных
	if err := setupDatabase(rethinkSession, cfg.DBName, cfg.TaskTableName, cfg.ResultTableName); err != nil {
		log.Fatalf("Failed to setup database: %v", err)
	}
	log.Println("✓ Database setup completed")

	// Подключаемся к Redis
	redisClient, err := connectToRedis(cfg)
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer redisClient.Close()

	log.Println("✓ Connected to Redis")

	// Создаем репозиторий
	repo := repository.NewTaskRepository(rethinkSession, cfg.TaskTableName)

	// Создаем API сервер
	apiServer := api.NewServer(repo, redisClient, cfg)

	// Запускаем health check сервер
	healthServer := startHealthServer(cfg.HealthPort, redisClient, rethinkSession)

	// Запускаем метрики сервер
	metricsServer := startMetricsServer(":9090")

	log.Printf("✓ Starting REST API server on %s", cfg.ServerPort)

	// Запускаем сервер в отдельной горутине
	serverErrors := make(chan error, 1)
	go func() {
		serverErrors <- apiServer.Start()
	}()

	// Ожидаем сигналов
	waitForShutdown(ctx, cancel, apiServer, healthServer, metricsServer, serverErrors)

	log.Println("=== API Server Stopped Gracefully ===")
}

func logConfig(cfg *config.Config) {
	log.Printf("Configuration:")
	log.Printf("  Redis URL: %s", cfg.RedisURL)
	log.Printf("  Redis Stream: %s", cfg.StreamName)
	log.Printf("  Consumer Group: %s", cfg.ConsumerGroup)
	log.Printf("  RethinkDB URL: %s", cfg.RethinkDBURL)
	log.Printf("  Database: %s", cfg.DBName)
	log.Printf("  Table: %s", cfg.TaskTableName)
	log.Printf("  Server Port: %s", cfg.ServerPort)
	log.Printf("  Health Port: %s", cfg.HealthPort)
	log.Printf("  Worker Count: %d", cfg.WorkerCount)
}

func connectToRethinkDB(cfg *config.Config) (*r.Session, error) {
	maxRetries := 10
	var session *r.Session
	var err error

	for i := 1; i <= maxRetries; i++ {
		log.Printf("Connecting to RethinkDB (attempt %d/%d)...", i, maxRetries)

		session, err = r.Connect(r.ConnectOpts{
			Address:    cfg.RethinkDBURL,
			Database:   cfg.DBName,
			MaxOpen:    20,
			InitialCap: 5,
			Timeout:    10 * time.Second,
		})

		if err == nil {
			if testErr := testRethinkDBConnection(session); testErr == nil {
				log.Println("RethinkDB connection successful")
				return session, nil
			}
			session.Close()
			//err = testErr
		}

		if i < maxRetries {
			waitTime := time.Duration(i) * 2 * time.Second
			log.Printf("Connection failed: %v. Retrying in %v...", err, waitTime)
			time.Sleep(waitTime)
		}
	}

	return nil, fmt.Errorf("failed to connect to RethinkDB after %d attempts: %w", maxRetries, err)
}

func testRethinkDBConnection(session *r.Session) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cursor, err := r.Now().Run(session, r.RunOpts{Context: ctx})
	if err != nil {
		return fmt.Errorf("connection test failed: %w", err)
	}
	defer cursor.Close()

	var result time.Time
	if err := cursor.One(&result); err != nil {
		return fmt.Errorf("failed to read server time: %w", err)
	}

	log.Printf("RethinkDB server time: %v", result)
	return nil
}

func setupDatabase(session *r.Session, dbName, taskTableName, resultTableName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	runOpts := r.RunOpts{Context: ctx}

	// Проверяем и создаем базу данных
	log.Printf("Setting up database '%s'...", dbName)

	cursor, err := r.DBList().Run(session, runOpts)
	if err != nil {
		return fmt.Errorf("failed to list databases: %w", err)
	}
	defer cursor.Close()

	var dbList []string
	if err := cursor.All(&dbList); err != nil {
		return fmt.Errorf("failed to read database list: %w", err)
	}

	dbExists := false
	for _, db := range dbList {
		if db == dbName {
			dbExists = true
			break
		}
	}

	if !dbExists {
		log.Printf("Creating database '%s'...", dbName)
		_, err := r.DBCreate(dbName).RunWrite(session, runOpts)
		if err != nil {
			return fmt.Errorf("failed to create database: %w", err)
		}
		log.Printf("Database '%s' created", dbName)
	}

	session.Use(dbName)

	// Проверяем и создаем таблицу
	log.Printf("Setting up table '%s'...", taskTableName)

	cursor2, err := r.TableList().Run(session, runOpts)
	if err != nil {
		return fmt.Errorf("failed to list tables: %w", err)
	}
	defer cursor2.Close()

	var tableList []string
	if err := cursor2.All(&tableList); err != nil {
		return fmt.Errorf("failed to read table list: %w", err)
	}

	tableExists := false
	for _, table := range tableList {
		if table == taskTableName {
			tableExists = true
			break
		}
	}

	if !tableExists {
		log.Printf("Creating table '%s'...", taskTableName)
		_, err := r.TableCreate(taskTableName).RunWrite(session, runOpts)
		if err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
		log.Printf("Table '%s' created", taskTableName)

		time.Sleep(1 * time.Second)

		if err := createIndexes(session, taskTableName, ctx); err != nil {
			log.Printf("Warning: Failed to create indexes: %v", err)
		}
	}

	// Проверяем и создаем таблицу
	log.Printf("Setting up table '%s'...", resultTableName)

	cursor2, err = r.TableList().Run(session, runOpts)
	if err != nil {
		return fmt.Errorf("failed to list tables: %w", err)
	}
	defer cursor2.Close()

	if err := cursor2.All(&tableList); err != nil {
		return fmt.Errorf("failed to read table list: %w", err)
	}

	tableExists = false
	for _, table := range tableList {
		if table == resultTableName {
			tableExists = true
			break
		}
	}

	if !tableExists {
		log.Printf("Creating table '%s'...", resultTableName)
		_, err := r.TableCreate(resultTableName).RunWrite(session, runOpts)
		if err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
		log.Printf("Table '%s' created", resultTableName)

		time.Sleep(1 * time.Second)

		if err := createIndexes(session, resultTableName, ctx); err != nil {
			log.Printf("Warning: Failed to create indexes: %v", err)
		}
	}
	return nil
}

func createIndexes(session *r.Session, tableName string, ctx context.Context) error {
	runOpts := r.RunOpts{Context: ctx}

	indexes := []string{"status", "created_at", "updated_at"}

	for _, index := range indexes {
		log.Printf("Creating index '%s'...", index)

		_, err := r.Table(tableName).IndexCreate(index).RunWrite(session, runOpts)
		if err != nil && !isIndexExistsError(err) {
			return fmt.Errorf("failed to create index %s: %w", index, err)
		}
	}

	_, err := r.Table(tableName).IndexWait().RunWrite(session, runOpts)
	if err != nil {
		return fmt.Errorf("failed to wait for indexes: %w", err)
	}

	log.Println("All indexes created successfully")
	return nil
}

func isIndexExistsError(err error) bool {
	errStr := err.Error()
	return strings.Contains(errStr, "already exists")
}

func connectToRedis(cfg *config.Config) (messaging.MessageClient, error) {
	maxRetries := 10
	var client messaging.MessageClient
	var err error

	for i := 1; i <= maxRetries; i++ {
		log.Printf("Connecting to Redis (attempt %d/%d)...", i, maxRetries)

		client, err = messaging.NewRedisClient(
			cfg.RedisURL,
			cfg.RedisPassword,
			cfg.RedisDB,
			cfg.StreamName,
			cfg.ConsumerGroup,
		)

		if err == nil {
			return client, nil
		}

		if i < maxRetries {
			waitTime := time.Duration(i) * 2 * time.Second
			log.Printf("Connection failed: %v. Retrying in %v...", err, waitTime)
			time.Sleep(waitTime)
		}
	}

	return nil, fmt.Errorf("failed to connect to Redis after %d attempts: %w", maxRetries, err)
}

func startHealthServer(port string, msgClient messaging.MessageClient, session *r.Session) *http.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, rr *http.Request) {
		// Проверяем Redis
		if err := msgClient.HealthCheck(); err != nil {
			http.Error(w, fmt.Sprintf("Redis: %v", err), http.StatusServiceUnavailable)
			return
		}

		// Проверяем RethinkDB
		ctx, cancel := context.WithTimeout(rr.Context(), 3*time.Second)
		defer cancel()

		cursor, err := r.Expr(1).Run(session, r.RunOpts{Context: ctx})
		if err != nil {
			http.Error(w, fmt.Sprintf("RethinkDB: %v", err), http.StatusServiceUnavailable)
			return
		}
		cursor.Close()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"status":"healthy","service":"api","timestamp":"%s"}`,
			time.Now().UTC().Format(time.RFC3339))
	})

	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ready"))
	})

	server := &http.Server{
		Addr:         port,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Printf("Health server listening on %s", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Health server error: %v", err)
		}
	}()

	return server
}

func startMetricsServer(port string) *http.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)

		// Здесь можно добавить метрики
		fmt.Fprintf(w, "# HELP api_requests_total Total API requests\n")
		fmt.Fprintf(w, "# TYPE api_requests_total counter\n")
		fmt.Fprintf(w, "api_requests_total 0\n")
	})

	server := &http.Server{
		Addr:    port,
		Handler: mux,
	}

	go func() {
		log.Printf("Metrics server listening on %s", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Metrics server error: %v", err)
		}
	}()

	return server
}

func waitForShutdown(ctx context.Context, cancel context.CancelFunc,
	apiServer *api.Server, healthServer, metricsServer *http.Server,
	serverErrors chan error) {

	osSignals := make(chan os.Signal, 1)
	signal.Notify(osSignals, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-serverErrors:
		log.Printf("Server error: %v", err)
		cancel()

	case sig := <-osSignals:
		log.Printf("Received signal: %v. Starting graceful shutdown...", sig)
		cancel()

		// Graceful shutdown всех серверов
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer shutdownCancel()

		if err := apiServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("API server shutdown error: %v", err)
		}

		servers := []*http.Server{healthServer, metricsServer}
		for _, server := range servers {
			if server != nil {
				if err := server.Shutdown(shutdownCtx); err != nil {
					log.Printf("Server shutdown error: %v", err)
				}
			}
		}

		time.Sleep(2 * time.Second)
	}
}
