package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

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
	Paths      []string   `json:"paths"`
	PluginPath string     `json:"plugin_path"`
	NReduce    int        `json:"n_reduce"`
	Status     TaskStatus `json:"status"`
	WorkerID   string     `json:"worker_id"`
	ReduceID   int        `json:"reduce_id"`
}

func (task *Task) Map() TaskStatus {
	if len(task.Paths) != 1 {
		return TASK_FAILED
	}

	var mapf MapFunc
	var err error
	if mapf, _, err = loadPlugin(task.PluginPath); err != nil {
		log.Printf("load plugin failed, err: %v", err)
		return TASK_FAILED
	}
	var file *os.File
	if file, err = os.Open(task.Paths[0]); err != nil {
		log.Printf("open file: %v failed, err: %v", task.Paths, err)
		return TASK_FAILED
	}
	content, _ := ioutil.ReadAll(file)
	file.Close()

	mapResult := mapf(task.Paths[0], string(content))

	splitResults := make([][]KeyValue, task.NReduce)

	for _, kv := range mapResult {
		idx := ihash(kv.Key) % task.NReduce
		splitResults[idx] = append(splitResults[idx], kv)
	}
	for idx := 0; idx < task.NReduce; idx++ {
		taskDir := fmt.Sprintf("tmp/%v/%v", task.WorkerID, task.ID)
		if err := os.MkdirAll(taskDir, 0700); err != nil {
			log.Printf("create dir: %v failed, err: %v", taskDir, err)
			return TASK_FAILED
		}
		var ofile *os.File
		ofileName := fmt.Sprintf("%v/%v", taskDir, idx)
		if ofile, err = os.Create(ofileName); err != nil {
			log.Printf("failed to create file: %v, err: %v", ofileName, err)
			return TASK_FAILED
		}
		defer ofile.Close()
		data, _ := json.Marshal(splitResults[idx])
		fmt.Fprint(ofile, string(data))
	}

	return TASK_SUCCESS
}

func (task *Task) Reduce() TaskStatus {
	if len(task.Paths) == 0 {
		return TASK_FAILED
	}

	var reducef ReduceFunc
	var err error
	if _, reducef, err = loadPlugin(task.PluginPath); err != nil {
		log.Printf("load plugin failed, err: %v", err)
		return TASK_FAILED
	}

	ofile, err := os.Create(fmt.Sprintf("mr-out-%v", task.ReduceID))
	defer ofile.Close()
	keyValues := make(map[string][]string)
	for _, path := range task.Paths {
		var file *os.File
		filePath := fmt.Sprintf("%v/%v", path, task.ReduceID)
		if file, err = os.Open(filePath); err != nil {
			log.Printf("open file: %v failed, err: %v", filePath, err)
			return TASK_FAILED
		}
		content, _ := ioutil.ReadAll(file)
		var lines []KeyValue
		json.Unmarshal(content, &lines)
		for _, kv := range lines {
			keyValues[kv.Key] = append(keyValues[kv.Key], kv.Value)
		}
	}
	for key, values := range keyValues {
		result := reducef(key, values)
		fmt.Fprintln(ofile, fmt.Sprintf("%v %v", key, result))
	}

	return TASK_SUCCESS
}

func (task *Task) Run() TaskStatus {
	switch task.Type {
	case MAP_TASK:
		return task.Map()
	case REDUCE_TASK:
		return task.Reduce()
	}
	return TASK_FAILED
}
