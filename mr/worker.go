package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	TASK_QUEUE_SIZE = 1
	COORDINATE_URL  = "http://127.0.0.1:3921/coordinate"
)

type PluginFuncs struct {
	MapFunc    MapFunc
	ReduceFunc ReduceFunc
}

type Worker struct {
	pluginFuncs map[string]PluginFuncs
	workerID    string
	tasks       chan Task

	tmpDirName string

	httpClient *http.Client
	lock       sync.Mutex

	done chan bool
}

func (w *Worker) coordinate(task *Task, fetchTask bool) error {
	request := Request{
		WorkerID:  w.workerID,
		FetchTask: fetchTask,
	}
	if task != nil {
		request.Tasks = map[string]Task{
			task.ID: *task,
		}
	}
	payload, _ := json.Marshal(request)
	log.Printf("request: %v", string(payload))
	httpRequest, _ := http.NewRequest(http.MethodPost, COORDINATE_URL, bytes.NewBuffer(payload))
	if httpResponse, err := w.httpClient.Do(httpRequest); err != nil {
		return fmt.Errorf("coordinator is down")
	} else {
		body, _ := ioutil.ReadAll(httpResponse.Body)
		log.Printf("response: %v", string(body))
		var response Response
		json.Unmarshal(body, &response)
		if response.StatusCode != 0 {
			return fmt.Errorf("fetch task failed")
		}
		if response.Task != nil {
			response.Task.Status = TASK_PENDING
			response.Task.WorkerID = w.workerID
			w.tasks <- *response.Task
			log.Printf("fetch task: %v", response.Task)
		}
	}
	return nil
}

func (w *Worker) sync() error {
	log.Println("sync")
	w.lock.Lock()
	w.lock.Unlock()
	if len(w.tasks) == cap(w.tasks) {
		log.Println("task queue is full, only send heatbeat to coordinator")
		return w.coordinate(nil, false)
	} else {
		return w.coordinate(nil, true)
	}
}

func (w *Worker) startSyncScheduer() {
	go func() {
		log.Println("start sync scheduler")
		tick := time.NewTicker(time.Second * 1)
		for {
			select {
			case <-tick.C:
				if err := w.sync(); err != nil {
					log.Printf("sync failed: %v", err)
					w.done <- true
				}
			}
		}
	}()
}

func (w *Worker) startWorkScheduler() {
	if err := os.MkdirAll(w.tmpDirName, 0700); err != nil {
		log.Printf("create dir: %v failed, err: %v", w.tmpDirName, err)
		w.done <- true
	}

	go func() {
		for {
			select {
			case t := <-w.tasks:
				t.Status = t.Run()
				w.coordinate(&t, false)
			default:
				log.Println("task queue is empty, wait...")
				time.Sleep(time.Second * 1)
			}
		}
	}()
}

func (w *Worker) Run() {
	w.startSyncScheduer()
	w.startWorkScheduler()
	<-w.done
}

func MakeWorker() *Worker {
	worker := &Worker{
		pluginFuncs: make(map[string]PluginFuncs),
		workerID:    uuid.New().String(),
		httpClient:  &http.Client{Timeout: 5 * time.Second},
		tasks:       make(chan Task, TASK_QUEUE_SIZE),
		done:        make(chan bool),
	}
	worker.tmpDirName = fmt.Sprintf("tmp/%v", worker.workerID)
	return worker
}
