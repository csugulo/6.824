package mr

import (
	"log"
)

const GET_TASK_URL = "http://127.0.0.1:3921/get_task"

type TaskType int8

const (
	MAP_TASK    TaskType = 1
	REDUCE_TASK TaskType = 2
)

func (taskType TaskType) String() string {
	switch taskType {
	case MAP_TASK:
		return "MAP"
	case REDUCE_TASK:
		return "REDUCE"
	default:
		return "UNKNOWN"
	}
}

type TaskStatus int8

const (
	TASK_SUCCESS TaskStatus = 0
	TASK_FAILED  TaskStatus = -1
	TASK_PENDING TaskStatus = 1
	TASK_RUNNING TaskStatus = 2
)

func (taskStatus TaskStatus) String() string {
	switch taskStatus {
	case TASK_SUCCESS:
		return "SUCCESS"
	case TASK_FAILED:
		return "FAILED"
	case TASK_PENDING:
		return "PENDING"
	case TASK_RUNNING:
		return "RUNNING"
	default:
		return "UNKNOWN"
	}
}

type Task struct {
	ID         string     `json:"id"`
	Type       TaskType   `json:"type"`
	FileName   string     `json:"file_name"`
	PluginPath string     `json:"plugin_path"`
	NReduce    int        `json:"n_reduce"`
	Status     TaskStatus `json:"status"`
}

func (task *Task) Run() TaskStatus {
	log.Printf("Task: %v is running", task.ID)
	return TASK_SUCCESS
}
