package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

func (kvs ByKey) Len() int           { return len(kvs) }
func (kvs ByKey) Swap(i, j int)      { kvs[i], kvs[j] = kvs[j], kvs[i] }
func (kvs ByKey) Less(i, j int) bool { return kvs[i].Key < kvs[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		AssignTaskargs := AssignTaskArgs{}
		AssignTaskreply := AssignTaskReply{}
		ok := call("Coordinator.AssignTask", &AssignTaskargs, &AssignTaskreply)
		if !ok {
			log.Printf("%d: call failed!", os.Getpid())
			return
		}

		if AssignTaskreply.TaskType == "map" {
			file, err := os.Open(AssignTaskreply.Filename)
			if err != nil {
				log.Fatalf("cannot open %v", AssignTaskreply.Filename)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", AssignTaskreply.Filename)
			}
			file.Close()
			kva := mapf(AssignTaskreply.Filename, string(content))

			bucket := make([][]KeyValue, AssignTaskreply.NReduce)
			for _, kv := range kva {
				reduceTaskNum := ihash(kv.Key) % AssignTaskreply.NReduce
				bucket[reduceTaskNum] = append(bucket[reduceTaskNum], kv)
			}

			for i := 0; i < AssignTaskreply.NReduce; i++ {
				writerfilename := fmt.Sprintf("mr-%d-%d", AssignTaskreply.TaskId, i)
				writer, err := os.Create(writerfilename)
				if err != nil {
					log.Fatalf("cannot create file %v", writerfilename)
				}
				enc := json.NewEncoder(writer)
				for _, kv := range bucket[i] {
					err := enc.Encode(&kv)
					if err != nil {
						log.Fatalf("cannot encode kv %v", kv)
					}
				}
				writer.Close()
			}

		} else if AssignTaskreply.TaskType == "reduce" {
			intermediate := []KeyValue{}
			for i := 0; i < AssignTaskreply.NMap; i++ {
				filename := fmt.Sprintf("mr-%d-%d", i, AssignTaskreply.TaskId)
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open file %v", filename)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					err := dec.Decode(&kv)
					if err != nil {
						if err == io.EOF {
							break
						} else {
							log.Fatalf("cannot decode file %v", filename)
						}
					}
					intermediate = append(intermediate, kv)
				}
				file.Close()
			}
			sort.Sort(ByKey(intermediate))
			oname := fmt.Sprintf("mr-out-%d", AssignTaskreply.TaskId)
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

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			ofile.Close()
		} else {
			continue
		}
		ReportTaskArgs := ReportTaskArgs{
			Phase:  AssignTaskreply.Phase,
			TaskId: AssignTaskreply.TaskId,
		}
		ReportTaskReply := ReportTaskReply{}
		ok = call("Coordinator.ReportTask", &ReportTaskArgs, &ReportTaskReply)
		if !ok {
			log.Printf("%d: call failed!", os.Getpid())
			return
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	} else {
		log.Printf("%d: call failed err %v", os.Getpid(), err)
	}
	return false
}
