package mr

import (
	"fmt"
	"io"
	"os"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func keyReduceIndex(key string, nReduce int) int {
	// use ihash(key) % NReduce to choose the reduce
	return ihash(key) % nReduce
}

const pathPrefix = "./"

func makePgFileName(name *string) string {
	// make a filename for a pg-*.txt file
	return "pg-" + *name + ".txt"
}

func makeIntermediateFromFile(filename string, mapf func(string, string) []KeyValue) []KeyValue {
	// read file and return intermediate
	path := filename
	file, err := os.Open(path)

	if err != nil {
		log.Fatalf("can't open file %v", path)
	}

	content, err := io.ReadAll(file)

	if err != nil {
		log.Fatalf("can't read file %v", filename)
	}
	file.Close()

	return mapf(filename, string(content))
}

type Aworker struct {
	// a struct for worker
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
	// true: Map
	// false: Reduce
	MapOrReduce bool
	//exit if true
	DoneFlag bool

	WorkerId int
}

func (worker *Aworker) logPrintf(format string, vars ...interface{}) {
	log.Printf("worker %d: "+format, worker.WorkerId, vars)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// a function for worker, it will run forever until all tasks are done
	// Your worker implementation here.
	worker := Aworker{}
	worker.mapf = mapf
	worker.reducef = reducef
	// map task first
	worker.MapOrReduce = true
	worker.DoneFlag = false
	worker.WorkerId = -1

	worker.logPrintf("Program Initialized!\n")

	for !worker.DoneFlag {
		worker.process()
	}
	//Done!
	worker.logPrintf("No more tasks, all jobs done, exiting...")
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func (worker *Aworker) askMapTask() *MapTaskReply {
	/* ask for a map task then return the reply */
	args := MapTaskArgs{}
	args.WorkerId = worker.WorkerId
	reply := MapTaskReply{}

	worker.logPrintf("Asking for a map task...")
	call("Coordinator.GiveMapTask", &args, &reply)

	// obtain a worker id
	worker.WorkerId = reply.WorkerId
	worker.logPrintf("Got a map task, filename: %s, fileId: %d", reply.FileName, reply.FileId)

	if reply.FileId == -1 {
		// refused to give a task
		if reply.DoneFlag {
			worker.logPrintf("No more map tasks, switching to reduce tasks...")
			return nil
		} else {
			worker.logPrintf("No map tasks available, waiting...")
			return &reply
		}
	}
	worker.logPrintf("got a map task, filename: %s, fileId: %d", reply.FileName, reply.FileId)
	return &reply
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
