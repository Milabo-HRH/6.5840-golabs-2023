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
		reply.TaskType = 2
		reply.NReduce = c.NReduce
	} else {
		reply.MapDone = false
		if len(c.ArrangedFiles)+c.DoneFiles != len(c.Files) {
			print("DoneFiles:", c.DoneFiles, "\n")
			print("ArrangedFiles:", len(c.ArrangedFiles), "\n")
			if len(c.ArrangedFiles)+c.DoneFiles > len(c.Files) {
				for k := range c.ArrangedFiles {
					print(k, " ")
				}
				print("\n")
			}
			reply.Filename = c.Files[len(c.ArrangedFiles)+c.DoneFiles]
			reply.NReduce = c.NReduce
			reply.TaskType = 0
			reply.TaskNumber = len(c.ArrangedFiles) + c.DoneFiles
			c.ArrangedFiles[len(c.ArrangedFiles)+c.DoneFiles] = 1
		} else {
			reply.TaskType = -1
			for k, v := range c.ArrangedFiles {
				if v == 50 {
					reply.Filename = c.Files[k]
					reply.NReduce = c.NReduce
					reply.TaskType = 0
					reply.TaskNumber = k
					c.ArrangedFiles[k] = 1
					break
				} else {
					c.ArrangedFiles[k]++
				}
			}
			//for i := 0; i < len(c.ArrangedFiles); i++ {
			//	if c.ArrangedFiles[i] == 50 {
			//		reply.Filename = c.Files[i]
			//		reply.NReduce = c.NReduce
			//		reply.TaskType = 0
			//		reply.TaskNumber = i
			//		c.ArrangedFiles[i] = 1
			//		break
			//	} else {
			//		c.ArrangedFiles[i]++
			//	}
			//}
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
		reply.TaskType = 2
		reply.ReduceDone = true
	} else {
		if len(c.reduceLog)+c.reduceFinished != c.NReduce {
			reply.TaskType = 1
			reply.TaskNumber = len(c.reduceLog) + c.reduceFinished
			c.reduceLog[len(c.reduceLog)+c.reduceFinished] = 1
		} else {
			reply.TaskType = -1
			for i, v := range c.reduceLog {
				if v == 50 {
					reply.TaskType = 1
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

func (c *Coordinator) FinishMap(args *MapReply, reply *MapArgs) error {
	c.mu.Lock()
	c.DoneFiles++
	//print("TaskNO: ", args.TaskNumber)
	//for k := range c.ArrangedFiles {
	//	print(k, "\n")
	//}
	delete(c.ArrangedFiles, args.TaskNumber)
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) FinishReduce(args *ReduceReply, reply *MapArgs) error {
	c.mu.Lock()
	c.reduceFinished++
	print("TaskNO: ", args.TaskNumber, "\n")
	for k := range c.reduceLog {
		print(k, "\n")
	}
	delete(c.reduceLog, args.TaskNumber)
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
	c.ArrangedFiles = make(map[int]int)
	c.reduceLog = make(map[int]int)
	c.server()
	return &c
}
