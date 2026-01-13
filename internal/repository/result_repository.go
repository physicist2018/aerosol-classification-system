package repository

import (
	"aerosol-system/internal/domain"
	"context"
	"errors"
	"fmt"
	"time"

	r "gopkg.in/rethinkdb/rethinkdb-go.v6"
)

type ResultRepository interface {
	CreateResult(ctx context.Context, result *domain.Result) error
	GetResult(ctx context.Context, id string) (*domain.Result, error)
	UpdateResult(ctx context.Context, id string, updates map[string]any) error
	ListResult(ctx context.Context, limit int) ([]domain.Result, error)
}

func NewResultRepository(session *r.Session, table string) ResultRepository {
	return &rethinkDBRepository{
		session: session,
		table:   table,
	}
}

func (repo *rethinkDBRepository) CreateResult(ctx context.Context, res *domain.Result) error {
	res.CreatedAt = time.Now()
	res.UpdatedAt = time.Now()

	result, err := r.Table(repo.table).Insert(res).RunWrite(repo.session)
	if err != nil {
		return fmt.Errorf("failed to create task: %w", err)
	}

	if len(result.GeneratedKeys) > 0 {
		res.ID = result.GeneratedKeys[0]
	}

	return nil
}

func (repo *rethinkDBRepository) GetResult(ctx context.Context, id string) (*domain.Result, error) {
	cursor, err := r.Table(repo.table).Get(id).Run(repo.session)
	if err != nil {
		return nil, fmt.Errorf("failed to get task: %w", err)
	}
	defer cursor.Close()

	if cursor.IsNil() {
		return nil, errors.New("task not found")
	}

	var result domain.Result
	if err := cursor.One(&result); err != nil {
		return nil, fmt.Errorf("failed to decode result: %w", err)
	}

	return &result, nil
}

func (repo *rethinkDBRepository) UpdateResult(ctx context.Context, id string, updates map[string]any) error {
	updates["updated_at"] = time.Now()

	_, err := r.Table(repo.table).Get(id).Update(updates).RunWrite(repo.session)
	if err != nil {
		return fmt.Errorf("failed to update result: %w", err)
	}

	return nil
}

func (repo *rethinkDBRepository) ListResult(ctx context.Context, limit int) ([]domain.Result, error) {
	cursor, err := r.Table(repo.table).
		OrderBy(r.Desc("created_at")).
		Limit(limit).
		Without([]string{"dep_file", "flcap_file", "vol_file"}).
		Run(repo.session)
	if err != nil {
		return nil, fmt.Errorf("failed to list tasks: %w", err)
	}
	defer cursor.Close()

	var results []domain.Result
	if err := cursor.All(&results); err != nil {
		return nil, fmt.Errorf("failed to decode results: %w", err)
	}

	return results, nil
}
