package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	CallMaps(mapf, reducef)
	CallReduce(mapf, reducef)

}

func CallReduce(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	args := ReduceArgs{}
	reply := ReduceReply{}
	args.Filename = ""
	reply.ReduceDone = false
	for reply.ReduceDone == false {

		ok := call("Coordinator.Reduce", &args, &reply)
		if ok {
			if reply.ReduceDone == true {
				break
			} else if reply.TaskType == -1 {
				time.Sleep(1 * time.Second)
			} else {
				intermediate := []KeyValue{}
				for i := 0; i < reply.NMap; i++ {
					iname := fmt.Sprintf("mr-%v-%v", i, reply.TaskNumber)
					file, err := os.Open(iname)
					if err != nil {
						fmt.Printf("cannot open %v", iname)
					} else {
						content, err := ioutil.ReadAll(file)
						if err != nil {
							fmt.Printf("cannot read %v", iname)
						} else {
							file.Close()
							kva := mapf(iname, string(content))
							intermediate = append(intermediate, kva...)
						}
					}
				}
				sort.Sort(ByKey(intermediate))
				oname := fmt.Sprintf("mr-out-%v", reply.TaskNumber)
				ofile, _ := os.Create(oname)
				i := 0
				for i < len(intermediate) {
					j := i + 1
					for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, intermediate[k].Value)
					}
					output := reducef(intermediate[i].Key, values)
					fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
					i = j
				}
				ofile.Close()
				call("Coordinator.FinishReduce", &reply, nil)
			}
		}
	}
}

func CallMaps(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// declare an argument structure.
	args := MapArgs{}

	// fill in the argument(s).
	args.Mapf = mapf
	args.Reducef = reducef
	args.MachineID = os.Getpid()
	// declare a reply structure.
	reply := MapReply{}
	reply.MapDone = false

	// send the RPC request, wait for the reply.
	for reply.MapDone == false {
		ok := call("Coordinator.Map", &args, &reply)
		if ok {
			// reply.Y should be 100.
			if reply.MapDone == true {
				break
			} else if reply.TaskType == 0 {
				file, err := os.Open(reply.Filename)
				if err != nil {
					fmt.Printf("cannot open %v", reply.Filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					fmt.Printf("cannot read %v", reply.Filename)
				}
				file.Close()

				kva := mapf(reply.Filename, string(content))
				buckets := make([][]KeyValue, reply.NReduce)
				for _, kv := range kva {
					bucket := ihash(kv.Key) % reply.NReduce
					buckets[bucket] = append(buckets[bucket], kv)
				}
				for i := 0; i < reply.NReduce; i++ {
					filename := fmt.Sprintf("mr-%v-%v", reply.TaskNumber, i)
					file, _ := os.Create(filename)
					for _, kv := range buckets[i] {
						fmt.Fprintf(file, "%v %v\n", kv.Key, kv.Value)
					}
					file.Close()

				}
				b := false
				for b == false {
					b = call("Coordinator.FinishMap", &reply, nil)
					if b == false {
						time.Sleep(time.Second)
					}
				}
			} else if reply.TaskType == 2 {
				break
			} else if reply.TaskType == -1 {

			}
			time.Sleep(1 * time.Second)
		} else {
			fmt.Printf("call failed!\n")
		}
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
