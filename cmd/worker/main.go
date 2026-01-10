// cmd/worker/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"aerosol-system/internal/config"
	"aerosol-system/internal/messaging"
	"aerosol-system/internal/repository"
	"aerosol-system/internal/worker"

	r "gopkg.in/rethinkdb/rethinkdb-go.v6"
)

func main() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	log.Println("=== Starting Task Worker (Redis) ===")

	// Загружаем конфигурацию
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	log.Printf("Configuration:")
	log.Printf("  Redis URL: %s", cfg.RedisURL)
	log.Printf("  Redis Stream: %s", cfg.StreamName)
	log.Printf("  Consumer Group: %s", cfg.ConsumerGroup)
	log.Printf("  RethinkDB URL: %s", cfg.RethinkDBURL)
	log.Printf("  Database: %s", cfg.DBName)
	log.Printf("  Table: %s", cfg.TableName)
	log.Printf("  Worker Count: %d", cfg.WorkerCount)
	log.Printf("  Health Port: %s", cfg.HealthPort)

	// Создаем контекст
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Подключаемся к RethinkDB
	log.Println(cfg)
	rethinkSession, err := r.Connect(r.ConnectOpts{

		Address:  cfg.RethinkDBURL,
		Database: cfg.DBName,
		MaxOpen:  20,
	})
	if err != nil {
		log.Fatalf("Failed to connect to RethinkDB: %v", err)
	}
	defer rethinkSession.Close()

	log.Println("✓ Connected to RethinkDB")

	// Проверяем подключение
	if _, err := r.DB(cfg.DBName).TableList().Run(rethinkSession); err != nil {
		log.Printf("Warning: Database/table might not exist: %v", err)
	}

	// Создаем репозиторий
	repo := repository.NewTaskRepository(rethinkSession, cfg.TableName)

	// Подключаемся к Redis
	redisClient, err := connectToRedis(cfg)
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer redisClient.Close()

	log.Println("✓ Connected to Redis")

	// Запускаем health server
	healthServer := startHealthServer(cfg.HealthPort, redisClient, rethinkSession)

	// Создаем и запускаем воркеров
	workers := createWorkers(cfg.WorkerCount, repo, redisClient, cfg)
	startWorkers(ctx, workers)

	log.Printf("✓ Started %d workers", len(workers))
	log.Println("Press Ctrl+C to stop")

	// Ожидаем сигналов
	waitForShutdown(cancel, workers, healthServer)

	log.Println("=== Worker stopped gracefully ===")
}

// ... (функции connectToRethinkDB, testRethinkDBConnection, setupDatabase, createIndexes остаются такими же) ...

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

func createWorkers(count int, repo repository.TaskRepository,
	msgClient messaging.MessageClient, cfg *config.Config) []*worker.Worker {

	workers := make([]*worker.Worker, count)
	hostname, _ := os.Hostname()

	for i := 0; i < count; i++ {
		workerID := fmt.Sprintf("%s-%d-%d", hostname, os.Getpid(), i+1)
		w := worker.NewWorker(workerID, repo, msgClient, cfg)
		workers[i] = w
		log.Printf("Created worker: %s", workerID)
	}

	return workers
}

func startWorkers(ctx context.Context, workers []*worker.Worker) {
	for i, w := range workers {
		go func(idx int, worker *worker.Worker) {
			log.Printf("Starting worker %d", idx+1)

			if err := worker.Start(ctx); err != nil {
				log.Printf("Worker %d stopped with error: %v", idx+1, err)
			}
		}(i, w)
	}
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
		fmt.Fprintf(w, `{"status":"healthy","service":"worker","timestamp":"%s"}`,
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

func waitForShutdown(cancel context.CancelFunc, workers []*worker.Worker, healthServer *http.Server) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	log.Printf("Received signal: %v", sig)

	// Начинаем graceful shutdown
	log.Println("Initiating graceful shutdown...")

	cancel()

	// Останавливаем воркеров
	stopWorkers(workers)

	// Останавливаем health server
	if healthServer != nil {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()

		if err := healthServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("Health server shutdown error: %v", err)
		}
	}

	// Даем время на завершение
	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(1)

	go func() {
		defer waitGroup.Done()
		time.Sleep(3 * time.Second)
	}()
	waitGroup.Wait()
	select {

	case sig := <-sigChan:
		log.Printf("Received second signal: %v - forcing shutdown", sig)
	case <-time.After(10 * time.Second):
		log.Println("Shutdown timeout")
	}

	log.Println("Shutdown completed")
}

func stopWorkers(workers []*worker.Worker) {
	log.Printf("Stopping %d workers...", len(workers))

	for i, w := range workers {
		if w != nil {
			w.Stop()
			log.Printf("Stopped worker %d", i+1)
		}
	}
}
