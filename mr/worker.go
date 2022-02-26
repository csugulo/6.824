package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	MAX_UNFINISHED_TASK = 10
)

type PluginFuncs struct {
	MapFunc    MapFunc
	ReduceFunc ReduceFunc
}

type Worker struct {
	pluginFuncs map[string]PluginFuncs
	workerID    string
	tasks       map[string]*Task

	httpClient *http.Client
	lock       sync.Mutex
}

func (w *Worker) ReportTask(task *Task) error {
	if task == nil {
		return fmt.Errorf("can not report nil task")
	}
	payload, _ := json.Marshal(Request{
		WorkerID: w.workerID,
		Tasks: map[string]Task{
			task.ID: *task,
		},
	})
	log.Printf("Worker: %v, send POST: %v", w.workerID, string(payload))
	httpRequest, _ := http.NewRequest(http.MethodPost, GET_TASK_URL, bytes.NewBuffer(payload))
	if httpResponse, err := w.httpClient.Do(httpRequest); err != nil {
		return fmt.Errorf("Coordinator is down")
	} else {
		body, _ := ioutil.ReadAll(httpResponse.Body)
		log.Printf("Worker: %v, get response: %v", w.workerID, string(body))
		var response Response
		json.Unmarshal(body, &response)
		if response.StatusCode != 0 {
			return fmt.Errorf("report task status failed, task: %v, statusMessage: %v", task.ID, response.StatusMessage)
		}
	}
	return nil
}

func (w *Worker) fetch(fetchTask bool) error {
	payload, _ := json.Marshal(Request{
		WorkerID:  w.workerID,
		FetchTask: fetchTask,
	})
	log.Printf("Worker: %v, send POST: %v", w.workerID, string(payload))
	httpRequest, _ := http.NewRequest(http.MethodPost, GET_TASK_URL, bytes.NewBuffer(payload))
	if httpResponse, err := w.httpClient.Do(httpRequest); err != nil {
		return fmt.Errorf("Coordinator is down")
	} else {
		body, _ := ioutil.ReadAll(httpResponse.Body)
		log.Printf("Worker: %v, get response: %v", w.workerID, string(body))
		var response Response
		json.Unmarshal(body, &response)
		if response.StatusCode != 0 {
			return fmt.Errorf("Worker: %v get task failed", w.workerID)
		}
		if response.Task != nil {
			response.Task.Status = TASK_PENDING
			w.tasks[response.Task.ID] = response.Task
			log.Printf("Worker: %v get task: %v", w.workerID, response.Task)
		}
	}
	return nil
}

func (w *Worker) sync() {
	tick := time.NewTicker(time.Second * 1)
	log.Printf("Worker: %v sync()", w.workerID)
	for {
		select {
		case <-tick.C:
			w.lock.Lock()
			defer w.lock.Unlock()
			unfinishedTaskNum := 0
			for _, task := range w.tasks {
				switch task.Status {
				case TASK_PENDING, TASK_RUNNING:
					unfinishedTaskNum++
				}
			}
			w.fetch(unfinishedTaskNum < MAX_UNFINISHED_TASK)
		}
	}
}

func (w *Worker) pickPendingTask() *Task {
	for _, t := range w.tasks {
		if t.Status == TASK_PENDING {
			return t
		}
	}
	return nil
}

func (w *Worker) work() {
	tick := time.NewTicker(time.Second * 1)
	log.Printf("Worker: %v work()", w.workerID)
	for {
		select {
		case <-tick.C:
			w.lock.Lock()
			defer w.lock.Unlock()
			task := w.pickPendingTask()
			log.Printf("Worker: %v pick task: %v", w.workerID, task)
			if task != nil {
				task.Status = task.Run()
				w.ReportTask(task)
			}
		}
	}
}

func (w *Worker) Run() {
	go w.sync()
	w.work()
}

func MakeWorker() *Worker {
	worker := &Worker{
		pluginFuncs: make(map[string]PluginFuncs),
		workerID:    uuid.New().String(),
		httpClient:  &http.Client{Timeout: 5 * time.Second},
		tasks:       make(map[string]*Task),
	}
	return worker
}
