package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	Intermediate   []KeyValue
	Files          []string
	ArrangedFiles  map[int]int
	DoneFiles      int
	NReduce        int
	mu             sync.Mutex
	reduceFinished int
	reduceLog      map[int]int
}

func (c *Coordinator) Map(args *MapArgs, reply *MapReply) error {
	// Your code here.
	c.mu.Lock()
	if len(c.Files) == c.DoneFiles {
		reply.MapDone = true
		reply.taskType = 2
		reply.NReduce = c.NReduce
	} else {
		reply.MapDone = false
		if len(c.ArrangedFiles)+c.DoneFiles != len(c.Files) {
			reply.filename = c.Files[len(c.ArrangedFiles)+c.DoneFiles]
			reply.NReduce = c.NReduce
			reply.taskType = 0
			reply.TaskNumber = len(c.ArrangedFiles) + c.DoneFiles
			c.ArrangedFiles[len(c.ArrangedFiles)+c.DoneFiles] = 1
		} else {
			reply.taskType = -1
			for i := 0; i < len(c.ArrangedFiles); i++ {
				if c.ArrangedFiles[i] == 50 {
					reply.filename = c.Files[i]
					reply.NReduce = c.NReduce
					reply.taskType = 0
					reply.TaskNumber = i
					c.ArrangedFiles[i] = 1
					break
				} else {
					c.ArrangedFiles[i]++
				}
			}
		}
	}
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) Reduce(args *ReduceArgs, reply *ReduceReply) error {
	reply.NReduce = c.NReduce
	reply.NMap = len(c.Files)
	c.mu.Lock()
	if c.reduceFinished == c.NReduce {
		reply.taskType = 2
		reply.ReduceDone = true
	} else {
		if len(c.reduceLog)+c.reduceFinished != c.NReduce {
			reply.taskType = 1
			reply.TaskNumber = len(c.reduceLog) + c.reduceFinished
		} else {
			reply.taskType = -1
			for i := 0; i < len(c.reduceLog); i++ {
				if c.reduceLog[i] == 50 {
					reply.taskType = 1
					reply.TaskNumber = i
					c.reduceLog[i] = 1
					break
				} else {
					c.reduceLog[i]++
				}
			}
		}
	}
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) FinishMap(args *MapArgs, reply *MapReply) error {
	c.mu.Lock()
	c.DoneFiles++
	delete(c.ArrangedFiles, reply.TaskNumber)
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) FinishReduce(args *MapArgs, reply *MapReply) error {
	c.mu.Lock()
	c.reduceFinished++
	delete(c.reduceLog, reply.TaskNumber)
	c.mu.Unlock()
	return nil
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	if c.DoneFiles == len(c.Files) && c.reduceFinished == c.NReduce {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.Files = files
	c.NReduce = nReduce

	c.server()
	return &c
}
