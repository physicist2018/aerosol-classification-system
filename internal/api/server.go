// api/server.go
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"aerosol-system/internal/config"
	"aerosol-system/internal/domain"
	"aerosol-system/internal/infrastructure"
	"aerosol-system/internal/messaging"
	"aerosol-system/internal/repository"

	"github.com/go-playground/validator/v10"
	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

type Server struct {
	router    *mux.Router
	repo      repository.TaskRepository
	msgClient messaging.MessageClient
	config    *config.Config
	validator *validator.Validate
	server    *http.Server
}

func NewServer(repo repository.TaskRepository, msgClient messaging.MessageClient, cfg *config.Config) *Server {
	s := &Server{
		router:    mux.NewRouter(),
		repo:      repo,
		msgClient: msgClient,
		config:    cfg,
		validator: validator.New(),
	}

	s.setupRoutes()
	s.setupMiddleware()

	return s
}

func (s *Server) setupRoutes() {
	// API v1
	apiRouter := s.router.PathPrefix("/api/v1").Subrouter()

	// Tasks endpoints
	apiRouter.HandleFunc("/tasks", s.createTask).Methods("POST")
	apiRouter.HandleFunc("/tasks", s.listTasks).Methods("GET")
	apiRouter.HandleFunc("/tasks/{id}", s.getTask).Methods("GET")
	apiRouter.HandleFunc("/tasks/{id}", s.updateTask).Methods("PUT")
	apiRouter.HandleFunc("/tasks/{id}", s.deleteTask).Methods("DELETE")

	// Health endpoint
	s.router.HandleFunc("/health", s.healthCheck).Methods("GET")

	// Swagger/OpenAPI docs
	s.router.HandleFunc("/docs", s.apiDocs).Methods("GET")

	// Default 404 handler
	s.router.NotFoundHandler = http.HandlerFunc(s.notFoundHandler)
}

func (s *Server) setupMiddleware() {
	// CORS middleware
	s.router.Use(s.corsMiddleware)

	// Logging middleware
	s.router.Use(s.loggingMiddleware)

	// Recovery middleware
	s.router.Use(s.recoveryMiddleware)
}

// Middleware
func (s *Server) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Пропускаем health checks из логов
		if r.URL.Path != "/health" {
			log.Printf("Request: %s %s %s", r.Method, r.URL.Path, r.RemoteAddr)
		}

		next.ServeHTTP(w, r)

		if r.URL.Path != "/health" {
			log.Printf("Response: %s %s completed in %v", r.Method, r.URL.Path, time.Since(start))
		}
	})
}

func (s *Server) recoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				log.Printf("Panic recovered: %v", err)
				s.respondWithError(w, http.StatusInternalServerError, "Internal server error")
			}
		}()

		next.ServeHTTP(w, r)
	})
}

// Handlers
func (s *Server) createTask(w http.ResponseWriter, r *http.Request) {
	// Parse multipart form (max 10 MB of files)
	err := r.ParseMultipartForm(10 << 20)

	if err != nil {
		s.respondWithError(w, http.StatusBadRequest, err.Error())
		return
	}

	var req domain.CreateTaskRequest

	// Get JSON field
	jsonStr := r.FormValue("json_data")

	err = json.Unmarshal([]byte(jsonStr), &req)
	if err != nil {
		s.respondWithError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if err := s.validator.Struct(req); err != nil {
		s.respondWithError(w, http.StatusBadRequest, err.Error())
		return
	}

	depFile := ""
	flCapFile := ""
	volFile := ""
	backFile := ""

	// Get files
	formFiles := r.MultipartForm.File
	for fileName, fileHeaders := range formFiles {
		for _, fileHeader := range fileHeaders {
			file, err := fileHeader.Open()
			if err != nil {
				s.respondWithError(w, http.StatusInternalServerError, err.Error())
				return
			}
			defer file.Close()

			// Read file content
			content, err := io.ReadAll(file)
			if err != nil {
				s.respondWithError(w, http.StatusInternalServerError, err.Error())
				return
			}

			// Process file (save, parse, etc.)
			fmt.Printf("Received file: %s, size: %d bytes\n", fileName, len(content))
			if fileName == "dep" {
				depFile = string(content)
			} else if fileName == "flcap" {
				flCapFile = string(content)
			} else if fileName == "vol" {
				volFile = string(content)
			} else if fileName == "beta" {
				backFile = string(content)
			}
		}
	}

	reader := infrastructure.NewTXTFileReader(zap.L())
	depMat, err := reader.ReadMatrix(depFile)

	if err != nil {
		s.respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}

	flCapMat, err := reader.ReadMatrix(flCapFile)
	if err != nil {
		s.respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}

	volMat, err := reader.ReadMatrix(volFile)
	if err != nil {
		s.respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}

	backMat, err := reader.ReadMatrix(backFile)
	if err != nil {
		s.respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Создаем задачу
	task := &domain.Task{
		Data:     req.Data,
		DepMat:   depMat,
		FlCapMat: flCapMat,
		VolMat:   volMat,
		BackMat:  backMat,
		Status:   domain.TaskStatusPending,
	}

	ctx := r.Context()
	if err := s.repo.CreateTask(ctx, task); err != nil {
		log.Printf("Failed to create task: %v", err)
		s.respondWithError(w, http.StatusInternalServerError, "Failed to create task")
		return
	}

	// Отправляем событие в NATS
	if err := s.msgClient.PublishTask(ctx, task.ID); err != nil {
		log.Printf("Failed to publish task to NATS: %v", err)
		// Задача создана, но событие не отправлено
		// Можно добавить в очередь повторных отправок или логировать
	}

	s.respondWithJSON(w, http.StatusCreated, task)
}

func (s *Server) getTask(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	taskID := vars["id"]

	task, err := s.repo.GetTask(r.Context(), taskID)
	if err != nil {
		s.respondWithError(w, http.StatusNotFound, "Task not found")
		return
	}

	s.respondWithJSON(w, http.StatusOK, task)
}

func (s *Server) listTasks(w http.ResponseWriter, r *http.Request) {
	// Получаем параметры запроса
	limit := 50
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if l, err := parseInt(limitStr); err == nil && l > 0 && l <= 100 {
			limit = l
		}
	}

	tasks, err := s.repo.ListTasks(r.Context(), limit)
	if err != nil {
		log.Printf("Failed to list tasks: %v", err)
		s.respondWithError(w, http.StatusInternalServerError, "Failed to fetch tasks")
		return
	}

	response := map[string]any{
		"tasks": tasks,
		"count": len(tasks),
		"limit": limit,
	}

	s.respondWithJSON(w, http.StatusOK, response)
}

func (s *Server) updateTask(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	taskID := vars["id"]

	var updates map[string]any
	if err := json.NewDecoder(r.Body).Decode(&updates); err != nil {
		s.respondWithError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	// Запрещаем обновление некоторых полей
	delete(updates, "id")
	delete(updates, "created_at")
	delete(updates, "completed_at")

	if err := s.repo.UpdateTask(r.Context(), taskID, updates); err != nil {
		log.Printf("Failed to update task: %v", err)
		s.respondWithError(w, http.StatusInternalServerError, "Failed to update task")
		return
	}

	s.respondWithJSON(w, http.StatusOK, map[string]string{"message": "Task updated"})
}

func (s *Server) deleteTask(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	taskID := vars["id"]

	// Вместо удаления, помечаем как удаленное
	updates := map[string]any{
		"status":     "deleted",
		"deleted_at": time.Now().UTC(),
	}

	if err := s.repo.UpdateTask(r.Context(), taskID, updates); err != nil {
		log.Printf("Failed to delete task: %v", err)
		s.respondWithError(w, http.StatusInternalServerError, "Failed to delete task")
		return
	}

	s.respondWithJSON(w, http.StatusOK, map[string]string{"message": "Task deleted"})
}

func (s *Server) healthCheck(w http.ResponseWriter, r *http.Request) {
	response := map[string]any{
		"status":    "healthy",
		"service":   "task-api",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"version":   "1.0.0",
	}

	s.respondWithJSON(w, http.StatusOK, response)
}

func (s *Server) apiDocs(w http.ResponseWriter, r *http.Request) {
	docs := map[string]any{
		"title":       "Task API",
		"description": "Asynchronous task processing API",
		"version":     "1.0.0",
		"endpoints": map[string]any{
			"POST /api/v1/tasks":        "Create a new task",
			"GET /api/v1/tasks":         "List tasks",
			"GET /api/v1/tasks/{id}":    "Get task by ID",
			"PUT /api/v1/tasks/{id}":    "Update task",
			"DELETE /api/v1/tasks/{id}": "Delete task",
		},
		"status_codes": []string{
			"pending",
			"processing",
			"success",
			"error",
			"deleted",
		},
	}

	s.respondWithJSON(w, http.StatusOK, docs)
}

func (s *Server) notFoundHandler(w http.ResponseWriter, r *http.Request) {
	s.respondWithError(w, http.StatusNotFound, "Endpoint not found")
}

// Helper functions
func (s *Server) respondWithJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	if err := json.NewEncoder(w).Encode(payload); err != nil {	
		log.Printf("Failed to encode response: %v", err)
	}
}

func (s *Server) respondWithError(w http.ResponseWriter, status int, message string) {
	response := map[string]string{"error": message}
	s.respondWithJSON(w, status, response)
}

func parseInt(str string) (int, error) {
	var n int
	_, err := fmt.Sscanf(str, "%d", &n)
	return n, err
}

// Server lifecycle
func (s *Server) Start() error {
	s.server = &http.Server{
		Addr:         s.config.ServerPort,
		Handler:      s.router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	log.Printf("Starting REST API server on %s", s.config.ServerPort)
	return s.server.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	if s.server != nil {
		log.Println("Shutting down API server...")
		return s.server.Shutdown(ctx)
	}
	return nil
}
