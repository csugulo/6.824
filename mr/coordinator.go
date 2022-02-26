package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type WorkerInfo struct {
	workID       string
	runningTasks map[string]*Task
	heatBeat     time.Time
}

type Coordinator struct {
	pluginPath string
	fileNames  []string
	nReduce    int

	activeWorkers      map[string]WorkerInfo
	pendingMapTasks    []*Task
	pendingReduceTasks []*Task

	finishedMapTaskIDs    []string
	finishedReduceTaskIDs []string

	router *gin.Engine

	lock sync.Mutex
}

func (c *Coordinator) getNextTask() *Task {
	if len(c.finishedMapTaskIDs) != len(c.fileNames) {
		if len(c.pendingMapTasks) == 0 {
			return nil
		}
		t := c.pendingMapTasks[0]
		c.pendingMapTasks = c.pendingMapTasks[1:]
		return t
	}
	if len(c.pendingReduceTasks) == 0 {
		return nil
	}
	t := c.pendingReduceTasks[0]
	c.pendingReduceTasks = c.pendingReduceTasks[1:]
	return t
}

func (c *Coordinator) CheckJobFinished() {
	tick := time.NewTicker(time.Second * 10)
	log.Print("Coordinator CheckJobFinished()")
	for {
		select {
		case <-tick.C:
			c.lock.Lock()
			defer c.lock.Unlock()
			if c.Done() {
				log.Println("mr job is done!")
				os.Exit(0)
			}
		default:
		}
	}
}

func (c *Coordinator) CheckWorkersAlive() {
	tick := time.NewTicker(time.Second * 1)
	log.Print("Coordinator CheckWorkersAlive()")
	for {
		select {
		case <-tick.C:
			c.lock.Lock()
			defer c.lock.Unlock()
			for workerID, workerInfo := range c.activeWorkers {
				now := time.Now()
				if now.Sub(workerInfo.heatBeat) > 10*time.Second {
					log.Printf("workerID: %v, last heatBeat from now: %v, removed", workerID, now.Sub(workerInfo.heatBeat))
					for _, task := range workerInfo.runningTasks {
						switch task.Type {
						case MAP_TASK:
							c.pendingMapTasks = append(c.pendingMapTasks, task)
						case REDUCE_TASK:
							c.pendingReduceTasks = append(c.pendingReduceTasks, task)
						}
					}
					delete(c.activeWorkers, workerID)
				}
			}
		default:
		}
	}
}

func (c *Coordinator) Server() {
	log.Print("Coordinator Server()")
	gin.SetMode(gin.ReleaseMode)
	c.router = gin.Default()
	c.router.POST("/get_task", func(ctx *gin.Context) {
		var request Request
		var response Response
		body, _ := ioutil.ReadAll(ctx.Request.Body)
		log.Printf("Request: %v", string(body))
		if err := json.Unmarshal(body, &request); err == nil {
			c.lock.Lock()
			defer c.lock.Unlock()
			if _, ok := c.activeWorkers[request.WorkerID]; !ok {
				log.Printf("New worker join cluster, worker: %v", request.WorkerID)
				c.activeWorkers[request.WorkerID] = WorkerInfo{
					workID:       request.WorkerID,
					runningTasks: make(map[string]*Task),
				}
			}
			// 1. update heart beat
			workerInfo := c.activeWorkers[request.WorkerID]
			workerInfo.heatBeat = time.Now()
			log.Printf("Receive worker heatbeat, worker: %v", request.WorkerID)
			// 2. update task status
			for taskID, task := range request.Tasks {

				if _, ok := c.activeWorkers[request.WorkerID].runningTasks[taskID]; !ok {
					log.Printf("Worker: %v report unknown task: %v, continue", request.WorkerID, taskID)
					continue
				}
				switch task.Status {
				case TASK_SUCCESS:
					delete(c.activeWorkers[request.WorkerID].runningTasks, taskID)
					switch task.Type {
					case MAP_TASK:
						c.finishedMapTaskIDs = append(c.finishedMapTaskIDs, taskID)
					case REDUCE_TASK:
						c.finishedReduceTaskIDs = append(c.finishedReduceTaskIDs, taskID)
					}
					log.Printf("Worker report task success, worker: %v, task: %v", request.WorkerID, task)
				case TASK_FAILED:
					delete(c.activeWorkers[request.WorkerID].runningTasks, taskID)
					switch task.Type {
					case MAP_TASK:
						c.pendingMapTasks = append(c.pendingMapTasks, &task)
					case REDUCE_TASK:
						c.pendingReduceTasks = append(c.pendingReduceTasks, &task)
					}
					log.Printf("Worker report task failed, worker: %v, task: %v", request.WorkerID, task)
				}
			}
			if !request.FetchTask {
				response = Response{
					Task:          nil,
					StatusCode:    0,
					StatusMessage: "",
				}
				data, _ := json.Marshal(response)
				ctx.Writer.Write(data)
			} else {
				// 3. return new task
				response = Response{
					Task:          c.getNextTask(),
					StatusCode:    0,
					StatusMessage: "",
				}
				if response.Task != nil {
					c.activeWorkers[request.WorkerID].runningTasks[response.Task.ID] = response.Task
				}
				data, _ := json.Marshal(response)
				log.Println(string(data))
				ctx.Writer.Write(data)
			}
		} else {
			response = Response{
				Task:          nil,
				StatusCode:    -1,
				StatusMessage: err.Error(),
			}
			data, _ := json.Marshal(response)
			ctx.Writer.Write(data)
		}
	})
	c.router.Run(":3921")
}

func (c *Coordinator) Done() bool {
	if len(c.finishedReduceTaskIDs) == c.nReduce {
		return true
	}
	return false
}

func MakeCoordinator(pluginPath string, nReduce int, fileNames []string) *Coordinator {
	c := &Coordinator{
		pluginPath:    pluginPath,
		fileNames:     fileNames,
		nReduce:       nReduce,
		activeWorkers: make(map[string]WorkerInfo),
	}
	// create map tasks
	for _, fileName := range fileNames {
		c.pendingMapTasks = append(c.pendingMapTasks,
			&Task{
				ID:         fmt.Sprintf("%v_%v", MAP_TASK, uuid.New()),
				Type:       MAP_TASK,
				FileName:   fileName,
				PluginPath: pluginPath,
				NReduce:    nReduce,
			},
		)
	}
	// create reduce tasks
	for i := 0; i < nReduce; i++ {
		c.pendingReduceTasks = append(c.pendingReduceTasks,
			&Task{
				ID:         fmt.Sprintf("%v_%v", REDUCE_TASK, uuid.New()),
				Type:       REDUCE_TASK,
				PluginPath: pluginPath,
			},
		)
	}
	go c.CheckWorkersAlive()
	go c.CheckJobFinished()
	c.Server()
	return c
}
