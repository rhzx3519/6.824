package mr

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Coordinator struct {
	// Your definitions here.
	mapTasks	[]MapTask
	reduceTasks []ReduceTask
	files    	[]string
	nReduce 	int
	stage 		Stage
	mutex   	sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) ApplyTask(args *ApplyTaskArgs, reply *ApplyTaskReply) (err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	//wait := make(chan string)
	//go c.handleRequest(wait)
	//x := <- wait

	switch c.stage {
	case Map:
		err = c.handleApplyMapTask(reply)
	case Reduce:
		err = c.handleApplyReduceTask(reply)
	}
	if err != nil {
		//log.Println("ApplyTask error...", err)
	}
	//log.Println("Coordinator's current CurrentStage is", c.stage)
	return
}

//
func (c *Coordinator) ReportTaskResult(args *TaskResultArgs, reply *TaskResultReply) (err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	switch args.TaskType {
	case Map:
		c.handleMapReport(args, reply)
	case Reduce:
		c.handleReduceReport(args, reply)
	}
	return
}

//--------------------------------------------------------------------------------
// private func

// call in lock
func (c *Coordinator) handleApplyMapTask(reply *ApplyTaskReply) (err error) {
	task, err := c.fetchMapTask()
	if err != nil {
		reply.Result = false
		reply.Message = "No Map task left..."
		return err
	}
	reply.Result = true
	reply.TaskType = Map
	reply.MapTaskId = task.id
	reply.Filename = task.filename
	reply.NumberOfReduce = c.nReduce

	// wait 10s if task is still unfinished, set task'status to Todo
	time.AfterFunc(time.Second*10, func() {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		if c.mapTasks[task.id].status != Done {
			//log.Printf("Reset map task Todo, mapTaskId: %v\n", task.id)
			c.mapTasks[task.id].status = Todo
		}
	})
	return
}

func (c *Coordinator) handleApplyReduceTask(reply *ApplyTaskReply) (err error) {
	task, err := c.fetchReduceTask()
	if err != nil {
		reply.Result = false
		reply.Message = err.Error()
		return err
	}
	reply.Result = true
	reply.TaskType = Reduce
	reply.ReduceTaskId = task.id
	reply.NumberOfReduce = c.nReduce

	// wait 10s if task is still unfinished, set task'status to Todo
	time.AfterFunc(time.Second*10, func() {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		if c.reduceTasks[task.id].status != Done {
			//log.Printf("Reset reduce task Todo, reduceTaskId: %v\n", task.id)
			c.reduceTasks[task.id].status = Todo
		}
	})
	return
}

// call in lock
func (c *Coordinator) handleMapReport(args *TaskResultArgs, reply *TaskResultReply) (err error) {
	task := &c.mapTasks[args.MapTaskId]
	if args.Success {
		task.status = Done
		c.tryNextStage()
	} else {
		task.status = Todo
	}
	reply.CurrentStage = c.stage

	//log.Printf("Map task is executed by worker. id: %d, filename: %s, status: %v.\n",
	//	task.id, task.filename, task.status)
	return
}

func (c *Coordinator) handleReduceReport(args *TaskResultArgs, reply *TaskResultReply) (err error) {
	task := &c.reduceTasks[args.ReduceTaskId]
	if args.Success {
		task.status = Done
		c.tryNextStage()
	} else {
		task.status = Todo
	}
	reply.CurrentStage = c.stage

	//log.Printf("Reduce task is executed by worker. id: %d, status: %v.\n",
	//	task.id, task.status)
	return
}

func (c *Coordinator) fetchMapTask() (task MapTask, err error) {
	for i := range c.mapTasks {
		if c.mapTasks[i].status == Todo {
			c.mapTasks[i].status = Doing
			task = c.mapTasks[i]
			return
		}
	}
	err = errors.New("No map task needs to do...")
	return
}

func (c *Coordinator) fetchReduceTask() (task ReduceTask, err error) {
	for i := range c.reduceTasks {
		if c.reduceTasks[i].status == Todo {
			c.reduceTasks[i].status = Doing
			task = c.reduceTasks[i]
			return
		}
	}
	err = errors.New("No reduce task needs to do...")
	return
}

func (c *Coordinator) tryNextStage() {
	switch c.stage {
	case STAGE_MAP:
		var next = true
		for _, task := range c.mapTasks {
			if task.status != Done {
				next = false
				break
			}
		}
		if next {
			c.stage = STAGE_REDUCE
		}
	case STAGE_REDUCE:
		var next = true
		for _, task := range c.reduceTasks {
			if task.status != Done {
				next = false
				break
			}
		}
		if next {
			c.stage = STAGE_END
		}
	}
}

//
// handle rpc request
//
func (c *Coordinator) handleRequest(ch chan string) {

}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.stage == STAGE_END {
		log.Println("MapReduce job is done...")
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce mapTasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapTasks := []MapTask{}
	for i, file := range files {
		mapTasks = append(mapTasks, MapTask{
			id: i,
			filename: file,
			status: Todo,
		})
	}

	reduceTasks := []ReduceTask{}
	for i := 0; i < nReduce; i++ {
		reduceTasks = append(reduceTasks, ReduceTask{
			id: i,
			status: Todo,
		})
	}

	c := Coordinator{
		files:    files,
		nReduce:  nReduce,
		mapTasks: mapTasks,
		reduceTasks: reduceTasks,
		stage:    STAGE_MAP,
	}

	fmt.Println("Coordinator started up...", c.nReduce)
	// Your code here.


	c.server()
	return &c
}
