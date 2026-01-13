// worker/worker.go
package worker

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"aerosol-system/internal/config"
	"aerosol-system/internal/domain"
	"aerosol-system/internal/infrastructure"
	"aerosol-system/internal/messaging"
	"aerosol-system/internal/repository"
	"aerosol-system/pkg/aerosol"
)

type Worker struct {
	id         string
	repo       repository.TaskRepository
	resultRepo repository.ResultRepository
	msgClient  messaging.MessageClient
	cfg        *config.Config
	stopChan   chan struct{}
	wg         sync.WaitGroup
	isRunning  atomic.Bool
	processed  atomic.Int64
	failed     atomic.Int64
	processing atomic.Int32 // Количество задач в обработке
}

func NewWorker(id string, repo repository.TaskRepository, resultRepo repository.ResultRepository,
	msgClient messaging.MessageClient, cfg *config.Config) *Worker {

	return &Worker{
		id:         id,
		repo:       repo,
		resultRepo: resultRepo,
		msgClient:  msgClient,
		cfg:        cfg,
		stopChan:   make(chan struct{}),
	}
}

func (w *Worker) Start(ctx context.Context) error {
	w.isRunning.Store(true)
	log.Printf("Worker %s starting...", w.id)

	// Подписываемся на задачи
	if err := w.msgClient.SubscribeToTasks(ctx, w.handleTask); err != nil {
		return fmt.Errorf("failed to subscribe to tasks: %w", err)
	}

	// Запускаем мониторинг
	go w.runMonitor(ctx)

	// Ждем сигнал остановки
	<-w.stopChan
	w.isRunning.Store(false)

	// Ждем завершения всех задач
	w.wg.Wait()

	log.Printf("Worker %s stopped. Stats: processed=%d, failed=%d",
		w.id, w.processed.Load(), w.failed.Load())
	return nil
}

func (w *Worker) runMonitor(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stats := w.GetStats()
			log.Printf("Worker %s stats: %+v", w.id, stats)
		case <-w.stopChan:
			return
		}
	}
}

func (w *Worker) handleTask(taskID string) {
	w.wg.Add(1)
	w.processing.Add(1)

	defer func() {
		w.processing.Add(-1)
		w.wg.Done()
	}()

	start := time.Now()

	// Создаем контекст с таймаутом
	ctx, cancel := context.WithTimeout(context.Background(), w.cfg.TaskTimeout)
	defer cancel()

	// Обрабатываем задачу
	err := w.processTaskWithRetry(ctx, taskID)

	duration := time.Since(start)
	if err != nil {
		log.Printf("Worker %s failed task %s after %v: %v",
			w.id, taskID, duration, err)
		w.failed.Add(1)
	} else {
		log.Printf("Worker %s completed task %s in %v",
			w.id, taskID, duration)
		w.processed.Add(1)
	}
}

func (w *Worker) processTaskWithRetry(ctx context.Context, taskID string) error {
	maxRetries := w.cfg.MaxRetries

	for attempt := 1; attempt <= maxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context canceled")
		default:
			err := w.processSingleTask(ctx, taskID, attempt)
			if err == nil {
				return nil // Успех
			}

			if attempt < maxRetries {
				log.Printf("Worker %s retrying task %s (attempt %d/%d): %v",
					w.id, taskID, attempt, maxRetries, err)
				time.Sleep(time.Duration(attempt) * time.Second) // Экспоненциальная задержка
			} else {
				return fmt.Errorf("failed after %d attempts: %w", maxRetries, err)
			}
		}
	}

	return fmt.Errorf("max retries exceeded")
}

func (w *Worker) processSingleTask(ctx context.Context, taskID string, attempt int) error {
	// Обновляем статус задачи
	updateData := map[string]any{
		"status":     domain.TaskStatusProcessing,
		"updated_at": time.Now(),
		"worker_id":  w.id,
		"attempt":    attempt,
	}

	if err := w.repo.UpdateTask(ctx, taskID, updateData); err != nil {
		return fmt.Errorf("failed to update task status: %w", err)
	}

	// Получаем задачу
	task, err := w.repo.GetTask(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get task: %w", err)
	}

	// Выполняем обработку
	result, err := w.executeTask(task)
	if err != nil {
		// Обновляем статус ошибки
		w.updateTaskError(ctx, taskID, err, attempt)
		return fmt.Errorf("task execution failed: %w", err)
	}
	log.Println(*result)
	err = w.resultRepo.CreateResult(ctx, &domain.Result{
		TaskID: taskID,
		Nd: &infrastructure.MatrixData{
			Rows:         task.DepMat.Rows,
			Cols:         task.DepMat.Cols,
			HeightLabels: task.DepMat.HeightLabels,
			TimeLabels:   task.DepMat.TimeLabels,
			FlatData:     result.Nd.RawMatrix().Data,
		},
		Nu: &infrastructure.MatrixData{
			Rows:         task.DepMat.Rows,
			Cols:         task.DepMat.Cols,
			HeightLabels: task.DepMat.HeightLabels,
			TimeLabels:   task.DepMat.TimeLabels,
			FlatData:     result.Nu.RawMatrix().Data,
		},
		Ns: &infrastructure.MatrixData{
			Rows:         task.DepMat.Rows,
			Cols:         task.DepMat.Cols,
			HeightLabels: task.DepMat.HeightLabels,
			TimeLabels:   task.DepMat.TimeLabels,
			FlatData:     result.Ns.RawMatrix().Data,
		},
		Residuals: &infrastructure.MatrixData{
			Rows:         task.DepMat.Rows,
			Cols:         task.DepMat.Cols,
			HeightLabels: task.DepMat.HeightLabels,
			TimeLabels:   task.DepMat.TimeLabels,
			FlatData:     result.Residuals.RawMatrix().Data,
		},
		Sd:        result.Sd,
		Su:        result.Su,
		Ss:        result.Ss,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	})

	if err != nil {
		return fmt.Errorf("Error updating results %w", err)
	}

	// Обновляем успешный результат
	return w.updateTaskSuccess(ctx, taskID, "Ok")
}

// TODO update this function
// here must be called solution logic
func (w *Worker) executeTask(task *domain.Task) (*aerosol.ClassificationResults, error) {
	// Имитация обработки

	processTime := time.Duration(rand.Intn(2000)+10000) * time.Millisecond // 1-3 секунды
	time.Sleep(processTime)

	// call classification subroutine
	aerosolClassifier, err := aerosol.NewClassifier(aerosol.ClassifierConfig{
		UrbanAerosol:  aerosol.AerosolParams{},
		DustAerosol:   aerosol.AerosolParams{},
		SmogAerosol:   aerosol.AerosolParams{},
		NIters:        100,
		BestSolutions: 10,
		Logger:        nil,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create aerosol classifier: %w", err)
	}

	clsRes, err := aerosolClassifier.Classify(task.DepMat.Matrix(),
		task.FlCapMat.Matrix(),
		task.VolMat.Matrix(),
		task.BackMat.Matrix())
	if err != nil {
		return nil, fmt.Errorf("failed to classify aerosol: %w", err)
	}

	return &clsRes, nil
}

func (w *Worker) updateTaskError(ctx context.Context, taskID string, err error, attempt int) {
	errorData := map[string]interface{}{
		"status":     domain.TaskStatusError,
		"error":      err.Error(),
		"updated_at": time.Now(),
		"worker_id":  w.id,
		"attempt":    attempt,
	}

	if updateErr := w.repo.UpdateTask(ctx, taskID, errorData); updateErr != nil {
		log.Printf("Worker %s failed to update error status for task %s: %v",
			w.id, taskID, updateErr)
	}
}

func (w *Worker) updateTaskSuccess(ctx context.Context, taskID string, result string) error {
	successData := map[string]interface{}{
		"status":       domain.TaskStatusSuccess,
		"result":       result,
		"updated_at":   time.Now(),
		"completed_at": time.Now(),
		"worker_id":    w.id,
	}

	if err := w.repo.UpdateTask(ctx, taskID, successData); err != nil {
		return fmt.Errorf("failed to update success status: %w", err)
	}

	return nil
}

func (w *Worker) Stop() {
	if w.isRunning.CompareAndSwap(true, false) {
		log.Printf("Stopping worker %s...", w.id)
		close(w.stopChan)
	}
}

func (w *Worker) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"id":         w.id,
		"running":    w.isRunning.Load(),
		"processed":  w.processed.Load(),
		"failed":     w.failed.Load(),
		"processing": w.processing.Load(),
	}
}

func (w *Worker) IsRunning() bool {
	return w.isRunning.Load()
}
