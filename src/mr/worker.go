package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
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
	worker := Aworker{
		mapf:    mapf,
		reducef: reducef,
		// map task first
		MapOrReduce: true,
		DoneFlag:    false,
		WorkerId:    -1,
	}

	worker.logPrintf("Program Initialized!\n")

	for !worker.DoneFlag {
		worker.process()
	}
	//Done!
	worker.logPrintf("No more tasks, all jobs done, exiting...\n")
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func (worker *Aworker) askMapTask() *MapTaskReply {
	/* ask for a map task then return the reply */
	args := MapTaskArgs{
		WorkerId: worker.WorkerId,
	}
	reply := MapTaskReply{}

	worker.logPrintf("Asking for a map task...\n")
	call("Coordinator.GiveMapTask", &args, &reply)

	// obtain a worker id
	worker.WorkerId = reply.WorkerId
	worker.logPrintf("Got a map task, filename: %s, fileId: %d", reply.FileName, reply.FileId)

	if reply.FileId == -1 {
		// refused to give a task
		if reply.DoneFlag {
			worker.logPrintf("No more map tasks, switching to reduce tasks...\n")
			return nil
		} else {
			worker.logPrintf("No map tasks available, waiting...\n")
			return &reply
		}
	}
	worker.logPrintf("got a map task, filename: %s, fileId: %d\n", reply.FileName, reply.FileId)
	return &reply
}

func (worker *Aworker) writeToFile(fileId int, nReduce int, intermediate []KeyValue) {
	// write intermediate to file
	kvas := make([][]KeyValue, nReduce)
	//for i := 0; i < nReduce; i++ {
	//	kvas[i] = make([]KeyValue, 0)
	//}
	for _, kv := range intermediate {
		index := keyReduceIndex(kv.Key, nReduce)
		kvas[index] = append(kvas[index], kv)
	}
	for i := 0; i < nReduce; i++ {
		// ioutils deprecated
		tempfile, err := os.CreateTemp(".", "mrtemp")
		if err != nil {
			worker.logPrintf("create tempfile failed\n")
			log.Fatal(err)
		}
		encode := json.NewEncoder(tempfile)
		for _, kv := range kvas[i] {
			err := encode.Encode(&kv)
			if err != nil {
				worker.logPrintf("encode failed\n")
				log.Fatal(err)
			}
		}
		outname := fmt.Sprintf("mr-%d-%d", fileId, i)
		err = os.Rename(tempfile.Name(), outname)
		if err != nil {
			worker.logPrintf("rename tempfile failed for %v\n", outname)
			log.Fatal(err)
		}
		tempfile.Close()
	}
}

func (worker *Aworker) joinMapTask(fileId int) {
	// notify coordinator that map task is done, the worker can join another map task
	args := MapTaskJoinArgs{
		FileId:   fileId,
		WorkerId: worker.WorkerId,
	}

	reply := MapTaskJoinReply{}
	worker.logPrintf("Joining map task...\n")
	call("Coordinator.JoinMapTask", &args, &reply)
	if reply.Accepted {
		worker.logPrintf("\033[1;32;40mAccepted\033[0m to join map task!\n")
	} else {
		worker.logPrintf("Failed to join map task!\n")
	}
}

func (worker *Aworker) executeMap(reply *MapTaskReply) {
	intermediate := makeIntermediateFromFile(reply.FileName, worker.mapf)

	worker.logPrintf("writing map result to file...\n")
	worker.writeToFile(reply.FileId, reply.NReduce, intermediate)
	worker.joinMapTask(reply.FileId)
}

/*
 * Reduce Task
 */

func (worker *Aworker) askReduceTask() *ReduceTaskReply {
	// ask for a reduce task then return the reply
	args := ReduceTaskArgs{
		WorkerId: worker.WorkerId,
	}
	reply := ReduceTaskReply{}

	worker.logPrintf("Asking for a reduce task...\n")
	call("Coordinator.GiveReduceTask", &args, &reply)

	if reply.RIndex == -1 {
		// refused to give a task
		// notify the caller
		if reply.DoneFlag {
			worker.logPrintf("No more reduce tasks, shutting down worker..\n")
			return nil
		} else {
			worker.logPrintf("No reduce tasks available, waiting...\n")
			return &reply
		}
	}
	worker.logPrintf("Got a reduce task on %vth cluster\n", reply.RIndex)
	return &reply
}

// ByKey sorting by key
type ByKey []KeyValue

func (bk ByKey) Len() int { return len(bk) }

func (bk ByKey) Swap(i, j int) { bk[i], bk[j] = bk[j], bk[i] }

func (bk ByKey) Less(i, j int) bool { return bk[i].Key < bk[j].Key }

func reduceKVSlice(intermediate []KeyValue, reducef func(string, []string) string, osfile io.Writer) {
	// sort, then reduce and write to file
	sort.Sort(ByKey(intermediate))
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && (intermediate)[j].Key == (intermediate)[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, (intermediate)[k].Value)
		}
		output := reducef((intermediate)[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(osfile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
}

func readIntermediates(fileId int, reduceId int) []KeyValue {
	// read intermediate from file
	filename := fmt.Sprintf("mr-%d-%d", fileId, reduceId)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	decode := json.NewDecoder(file)
	intermediate := make([]KeyValue, 0)
	for {
		var kv KeyValue
		if err := decode.Decode(&kv); err != nil {
			break
		}
		intermediate = append(intermediate, kv)
	}
	file.Close()
	return intermediate
}

func (worker *Aworker) joinReduceTask(rindex int) {
	args := ReduceTaskJoinArgs{
		RIndex:   rindex,
		WorkerId: worker.WorkerId,
	}

	reply := ReduceTaskJoinReply{}
	call("Coordinator.JoinReduceTask", &args, &reply)

	if reply.Accepted {
		worker.logPrintf("\033[1;32;40mAccepted\033[0m to join reduce task!\n")
	} else {
		worker.logPrintf("Failed to join reduce task!\n")
	}
}

func (worker *Aworker) executeReduce(reply *ReduceTaskReply) {
	outname := fmt.Sprintf("mr-out-%d", reply.RIndex)

	intermediate := make([]KeyValue, 0)

	for i := 0; i < reply.FileCount; i++ {
		worker.logPrintf("generating intermediate from file %d\n", i)
		intermediate = append(intermediate, readIntermediates(i, reply.RIndex)...)
	}

	worker.logPrintf("Intermediate count: %d\n", len(intermediate))
	// ioutils deprecated
	tempfile, err := os.CreateTemp(".", "mrtemp")
	if err != nil {
		worker.logPrintf("create tempfile failed for %v\n", outname)
		log.Fatal(err)
	}

	reduceKVSlice(intermediate, worker.reducef, tempfile)
	tempfile.Close()

	err = os.Rename(tempfile.Name(), outname)
	if err != nil {
		worker.logPrintf("rename tempfile failed for %v\n", outname)
		log.Fatal(err)
	}
	tempfile.Close()
	worker.joinReduceTask(reply.RIndex)
}

func (worker *Aworker) process() {
	if worker.DoneFlag {
		// must exit
		// checked by caller
	}
	if worker.MapOrReduce {
		// map task
		reply := worker.askMapTask()
		if reply == nil {
			// no more map tasks, switch to reduce tasks
			worker.MapOrReduce = false
		} else {
			worker.executeMap(reply)
		}
	} else {
		// reduce task
		reply := worker.askReduceTask()
		if reply == nil {
			// no more reduce tasks, exit
			worker.DoneFlag = true
		} else {
			worker.executeReduce(reply)
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
