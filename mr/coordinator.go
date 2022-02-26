package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"reflect"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type WorkerInfo struct {
	workID   string
	tasks    map[string]Task
	heatBeat time.Time
}

type Coordinator struct {
	pluginPath string
	fileNames  []string
	nReduce    int
	nMap       int

	activeWorkers map[string]WorkerInfo

	mapTasks    chan Task
	reduceTasks chan Task

	finishedMapTaskIDs    []string
	finishedReduceTaskIDs []string

	reduceFileMap map[int][]string

	router *gin.Engine

	lock sync.Mutex

	done chan bool
}

func (c *Coordinator) String() string {
	return fmt.Sprintf("Coordinator{pluginPath: %v, fileNames: %v, nMap: %v, nReduce: %v}", c.pluginPath, c.fileNames, c.nMap, c.nReduce)
}

func (c *Coordinator) getNextTask() *Task {
	if len(c.finishedMapTaskIDs) != c.nMap {
		if len(c.mapTasks) != 0 {
			t := <-c.mapTasks
			return &t
		}
		return nil
	}
	if len(c.reduceTasks) != 0 {
		t := <-c.reduceTasks
		t.Paths = c.reduceFileMap[t.ReduceID]
		return &t
	}
	return nil
}

func (c *Coordinator) checkJobFinish() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	log.Printf("map tasks: [%v/%v], reduce tasks: [%v/%v]", len(c.finishedMapTaskIDs), c.nMap, len(c.finishedReduceTaskIDs), c.nReduce)
	if len(c.finishedReduceTaskIDs) == c.nReduce {
		return true
	}
	return false
}

func (c *Coordinator) startCheckJobFinishScheduler() {
	go func() {
		log.Println("start check job finish scheduer")
		tick := time.NewTicker(time.Second * 5)
		for {
			select {
			case <-tick.C:
				if done := c.checkJobFinish(); done {
					log.Println("job is finish")
					c.done <- true
				}
			}
		}
	}()
}

func (c *Coordinator) checkWorkersAlive() {
	c.lock.Lock()
	defer c.lock.Unlock()
	if len(c.activeWorkers) == 0 {
		log.Println("no alive worker")
	} else {
		for workerID, workerInfo := range c.activeWorkers {
			now := time.Now()
			if now.Sub(workerInfo.heatBeat) > 10*time.Second {
				log.Printf("worker: %v, last heatBeat from now: %v, removed", workerID, now.Sub(workerInfo.heatBeat))
				for _, task := range workerInfo.tasks {
					task.Status = TASK_PENDING
					switch task.Type {
					case MAP_TASK:
						c.mapTasks <- task
					case REDUCE_TASK:
						c.reduceTasks <- task
					}
				}
				delete(c.activeWorkers, workerID)
			}
		}
		log.Printf("alive workers: %v", reflect.ValueOf(c.activeWorkers).MapKeys())
	}
}

func (c *Coordinator) startCheckWorkerAliveScheduler() {
	go func() {
		log.Print("start check worker alive scheduler")
		tick := time.NewTicker(time.Second * 1)
		for {
			select {
			case <-tick.C:
				c.checkWorkersAlive()
			}
		}
	}()
}

func (c *Coordinator) startRPCServer() {
	log.Print("start rpc server")
	gin.SetMode(gin.ReleaseMode)
	c.router = gin.Default()
	c.router.POST("/coordinate", func(ctx *gin.Context) {
		var request Request
		var response Response
		body, _ := ioutil.ReadAll(ctx.Request.Body)
		log.Printf("request: %v", string(body))
		if err := json.Unmarshal(body, &request); err != nil {
			response = Response{
				Task:          nil,
				StatusCode:    -1,
				StatusMessage: err.Error(),
			}
			data, _ := json.Marshal(response)
			ctx.Writer.Write(data)
		}
		c.lock.Lock()
		defer c.lock.Unlock()
		if _, ok := c.activeWorkers[request.WorkerID]; !ok {
			log.Printf("new worker %v join cluster", request.WorkerID)
			c.activeWorkers[request.WorkerID] = WorkerInfo{
				workID:   request.WorkerID,
				tasks:    make(map[string]Task),
				heatBeat: time.Now(),
			}
		}
		// 1. update heart beat
		workerInfo := c.activeWorkers[request.WorkerID]
		workerInfo.heatBeat = time.Now()
		c.activeWorkers[request.WorkerID] = workerInfo
		log.Printf("receive worker: %v heatbeat", request.WorkerID)
		// 2. update task status
		for taskID, task := range request.Tasks {
			if _, ok := c.activeWorkers[request.WorkerID].tasks[taskID]; !ok {
				log.Printf("worker: %v report unknown task: %v, continue", request.WorkerID, taskID)
				continue
			}
			switch task.Status {
			case TASK_SUCCESS:
				delete(c.activeWorkers[request.WorkerID].tasks, taskID)
				switch task.Type {
				case MAP_TASK:
					c.finishedMapTaskIDs = append(c.finishedMapTaskIDs, taskID)
					for idx := 0; idx < c.nReduce; idx++ {
						c.reduceFileMap[idx] = append(c.reduceFileMap[idx], fmt.Sprintf("tmp/%v/%v", task.WorkerID, task.ID))
					}
				case REDUCE_TASK:
					c.finishedReduceTaskIDs = append(c.finishedReduceTaskIDs, taskID)
				}
				log.Printf("worker: %v report task %v success", request.WorkerID, task)
			case TASK_FAILED:
				delete(c.activeWorkers[request.WorkerID].tasks, taskID)
				task.Status = TASK_PENDING
				switch task.Type {
				case MAP_TASK:
					c.mapTasks <- task
				case REDUCE_TASK:
					c.reduceTasks <- task
				}
				log.Printf("worker: %v report task: %v success", request.WorkerID, task)
			}
		}
		var nextTask *Task
		if request.FetchTask {
			nextTask = c.getNextTask()
			if nextTask != nil {
				c.activeWorkers[request.WorkerID].tasks[nextTask.ID] = *nextTask
			}

		}
		response = Response{
			Task:          nextTask,
			StatusCode:    0,
			StatusMessage: "",
		}
		data, _ := json.Marshal(response)
		log.Printf("response: %v", string(data))
		ctx.Writer.Write(data)
	})
	go c.router.Run(":3921")
}

func (c *Coordinator) Server() {
	c.startCheckWorkerAliveScheduler()
	c.startCheckJobFinishScheduler()
	c.startRPCServer()
	<-c.done
}

func MakeCoordinator(pluginPath string, nReduce int, fileNames []string) *Coordinator {
	c := &Coordinator{
		pluginPath:    pluginPath,
		fileNames:     fileNames,
		nReduce:       nReduce,
		nMap:          len(fileNames),
		activeWorkers: make(map[string]WorkerInfo),
		done:          make(chan bool),
		mapTasks:      make(chan Task, len(fileNames)),
		reduceTasks:   make(chan Task, nReduce),
		reduceFileMap: make(map[int][]string),
	}
	// create map tasks
	for _, fileName := range fileNames {
		c.mapTasks <- Task{
			ID:         fmt.Sprintf("%v_%v", MAP_TASK, uuid.New()),
			Type:       MAP_TASK,
			Paths:      []string{fileName},
			PluginPath: pluginPath,
			NReduce:    nReduce,
			Status:     TASK_PENDING,
		}
	}
	// create reduce tasks
	for i := 0; i < nReduce; i++ {
		c.reduceTasks <- Task{
			ID:         fmt.Sprintf("%v_%v", REDUCE_TASK, uuid.New()),
			Type:       REDUCE_TASK,
			PluginPath: pluginPath,
			Status:     TASK_PENDING,
			NReduce:    nReduce,
			ReduceID:   i,
		}
	}
	log.Printf("MakeCoordinator: %v", c)
	return c
}
