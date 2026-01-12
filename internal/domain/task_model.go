package domain

import (
	"aerosol-system/internal/infrastructure"
	"time"
)

type TaskStatus string

const (
	TaskStatusPending    TaskStatus = "pending"
	TaskStatusProcessing TaskStatus = "processing"
	TaskStatusSuccess    TaskStatus = "success"
	TaskStatusError      TaskStatus = "error"
	TaskStatusCancelled  TaskStatus = "cancelled"
)

type Task struct {
	ID          string                     `gorethink:"id,omitempty" json:"id"`
	Data        string                     `gorethink:"data" json:"data"`
	Status      TaskStatus                 `gorethink:"status" json:"status"`
	Result      string                     `gorethink:"result,omitempty" json:"result,omitempty"`
	Error       string                     `gorethink:"error,omitempty" json:"error,omitempty"`
	CreatedAt   time.Time                  `gorethink:"created_at" json:"created_at"`
	UpdatedAt   time.Time                  `gorethink:"updated_at" json:"updated_at"`
	CompletedAt *time.Time                 `gorethink:"completed_at,omitempty" json:"completed_at,omitempty"`
	DepMat      *infrastructure.MatrixData `gorethink:"dep_mat" json:"dep_mat"`
	FlCapMat    *infrastructure.MatrixData `gorethink:"flcap_mat" json:"flcap_mat"`
	VolMat      *infrastructure.MatrixData `gorethink:"vol_mat" json:"vol_mat"`
	BackMat     *infrastructure.MatrixData `gorethink:"back_mat" json:"back_mat"`
}

type CreateTaskRequest struct {
	Data string `json:"data" validate:"required"`
}
