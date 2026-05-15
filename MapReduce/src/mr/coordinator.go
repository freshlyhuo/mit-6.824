package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// The map phase and reduce phase of the job.

type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex

	files   []string
	nReduce int
	nMap    int

	MapTasks    []Task
	ReduceTasks []Task

	phase Phase
}

type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	AllDone
)

type Task struct {
	Id        int
	Status    TaskStatus
	filename  string
	startTime time.Time
}

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.NReduce = c.nReduce
	reply.NMap = c.nMap
	reply.Phase = c.phase

	if c.phase == MapPhase {
		for i, mapTask := range c.MapTasks {
			if (mapTask.Status == Idle) || (mapTask.Status == InProgress && time.Since(mapTask.startTime) > 10*time.Second) {
				c.MapTasks[i].Status = InProgress
				c.MapTasks[i].startTime = time.Now()
				reply.TaskType = "map"
				reply.TaskId = mapTask.Id
				reply.Filename = mapTask.filename
				return nil
			}
		}
		reply.TaskType = "wait"
	} else if c.phase == ReducePhase {
		for i, reduceTask := range c.ReduceTasks {
			if (reduceTask.Status == Idle) || (reduceTask.Status == InProgress && time.Since(reduceTask.startTime) > 10*time.Second) {
				c.ReduceTasks[i].Status = InProgress
				c.ReduceTasks[i].startTime = time.Now()
				reply.TaskType = "reduce"
				reply.TaskId = reduceTask.Id
				return nil
			}
		}
		reply.TaskType = "wait"
	}
	reply.TaskType = "wait"
	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.Phase == MapPhase {
		c.MapTasks[args.TaskId].Status = Completed
	} else if args.Phase == ReducePhase {
		c.ReduceTasks[args.TaskId].Status = Completed
	}
	c.CheckTaskStatus()
	return nil
}

func (c *Coordinator) CheckTaskStatus() {
	if c.phase == MapPhase {
		for _, mapTask := range c.MapTasks {
			if mapTask.Status != Completed {
				return
			}
		}
		c.phase = ReducePhase
	} else if c.phase == ReducePhase {
		for _, reduceTask := range c.ReduceTasks {
			if reduceTask.Status != Completed {
				return
			}
		}
		c.phase = AllDone
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	if err := rpc.Register(c); err != nil {
		log.Fatalf("rpc register error: %v", err)
	}
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go func() {
		if err := http.Serve(l, nil); err != nil {
			log.Fatalf("http serve error: %v", err)
		}
	}()
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.phase == AllDone {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.nReduce = nReduce
	c.nMap = len(files)
	c.phase = MapPhase
	c.MapTasks = make([]Task, len(files))
	for i, filename := range files {
		c.MapTasks[i] = Task{Id: i, Status: Idle, filename: filename}
	}
	c.ReduceTasks = make([]Task, nReduce)
	for i := 0; i < nReduce; i++ {
		c.ReduceTasks[i] = Task{Id: i, Status: Idle}
	}

	c.server(sockname)
	return &c
}
