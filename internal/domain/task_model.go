package domain

import "time"

type TaskStatus string

const (
	TaskStatusPending    TaskStatus = "pending"
	TaskStatusProcessing TaskStatus = "processing"
	TaskStatusSuccess    TaskStatus = "success"
	TaskStatusError      TaskStatus = "error"
	TaskStatusCancelled  TaskStatus = "cancelled"
)

type Task struct {
	ID          string     `gorethink:"id,omitempty" json:"id"`
	Data        string     `gorethink:"data" json:"data"`
	Status      TaskStatus `gorethink:"status" json:"status"`
	Result      string     `gorethink:"result,omitempty" json:"result,omitempty"`
	Error       string     `gorethink:"error,omitempty" json:"error,omitempty"`
	CreatedAt   time.Time  `gorethink:"created_at" json:"created_at"`
	UpdatedAt   time.Time  `gorethink:"updated_at" json:"updated_at"`
	CompletedAt *time.Time `gorethink:"completed_at,omitempty" json:"completed_at,omitempty"`
	Dep_file    string     `gorethink:"dep_file" json:"dep_file"`
	FlCap_file  string     `gorethink:"flcap_file" json:"flcap_file"`
	Vol_file    string     `gorethink:"vol_file" json:"vol_file"`
}

type CreateTaskRequest struct {
	Data string `json:"data" validate:"required"`
}
