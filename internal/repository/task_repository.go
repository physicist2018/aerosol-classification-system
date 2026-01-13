package repository

import (
	"aerosol-system/internal/domain"
	"context"
	"errors"
	"fmt"
	"time"

	r "gopkg.in/rethinkdb/rethinkdb-go.v6"
)

type TaskRepository interface {
	CreateTask(ctx context.Context, task *domain.Task) error
	GetTask(ctx context.Context, id string) (*domain.Task, error)
	UpdateTask(ctx context.Context, id string, updates map[string]any) error
	ListTasks(ctx context.Context, limit int) ([]domain.Task, error)
}

type rethinkDBRepository struct {
	session *r.Session
	table   string
}

func NewTaskRepository(session *r.Session, table string) TaskRepository {
	return &rethinkDBRepository{
		session: session,
		table:   table,
	}
}

func (repo *rethinkDBRepository) CreateTask(ctx context.Context, task *domain.Task) error {
	task.CreatedAt = time.Now()
	task.UpdatedAt = time.Now()

	result, err := r.Table(repo.table).Insert(task).RunWrite(repo.session)
	if err != nil {
		return fmt.Errorf("failed to create task: %w", err)
	}

	if len(result.GeneratedKeys) > 0 {
		task.ID = result.GeneratedKeys[0]
	}

	return nil
}

func (repo *rethinkDBRepository) GetTask(ctx context.Context, id string) (*domain.Task, error) {
	cursor, err := r.Table(repo.table).Get(id).Run(repo.session)
	if err != nil {
		return nil, fmt.Errorf("failed to get task: %w", err)
	}
	defer cursor.Close()

	if cursor.IsNil() {
		return nil, errors.New("task not found")
	}

	var task domain.Task
	if err := cursor.One(&task); err != nil {
		return nil, fmt.Errorf("failed to decode task: %w", err)
	}

	return &task, nil
}

func (repo *rethinkDBRepository) UpdateTask(ctx context.Context, id string, updates map[string]interface{}) error {
	updates["updated_at"] = time.Now()

	_, err := r.Table(repo.table).Get(id).Update(updates).RunWrite(repo.session)
	if err != nil {
		return fmt.Errorf("failed to update task: %w", err)
	}

	return nil
}

func (repo *rethinkDBRepository) ListTasks(ctx context.Context, limit int) ([]domain.Task, error) {
	cursor, err := r.Table(repo.table).
		OrderBy(r.Desc("created_at")).
		Limit(limit).
		Without([]string{"dep_file", "flcap_file", "vol_file"}).
		Run(repo.session)
	if err != nil {
		return nil, fmt.Errorf("failed to list tasks: %w", err)
	}
	defer cursor.Close()

	var tasks []domain.Task
	if err := cursor.All(&tasks); err != nil {
		return nil, fmt.Errorf("failed to decode tasks: %w", err)
	}

	return tasks, nil
}
