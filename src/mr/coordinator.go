package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const maxTaxTime = 10 // seconds

type MapTaskState struct {
	beginSecond int64
	workerId    int
	fileId      int
}

type Coordinator struct {
	// Your definitions here.
	fileNames   []string
	nReduce     int
	nowWorkerId int

	// unIssuedMapTasks: a queue of unissued map tasks
	unIssuedMapTasks *BlockQueue
	// issuedMapTasks: a map from fileId to a queue of issued map tasks
	issuedMapTasks *MapSet
	// issueMapMutex: a mutex for issuedMapTasks
	issueMapMutex sync.Mutex

	// unIssuedReduceTasks: a queue of unissued reduce tasks
	unIssuedReduceTasks *BlockQueue
	// issuedReduceTasks: a map from fileId to a queue of issued reduce tasks
	issuedReduceTasks *MapSet
	// issueReduceMutex: a mutex for issuedReduceTasks
	issueReduceMutex sync.Mutex

	// task states
	mapTasks    []MapTaskState
	reduceTasks []ReduceTaskState

	// done flags
	mapDone  bool
	doneFlag bool
}

type MapTaskReply struct {
	// worker passes this to the os package
	FileName string

	// marks a unique file for mapping
	// gives -1 for no more fileId
	FileId int

	// for reduce tasks
	NReduce int

	// assign worker id as this reply is the first sent to workers
	WorkerId int

	// whether this kind of tasks are all done
	// if not, and fileId is -1, the worker waits
	DoneFlag bool
}

type MapTaskArgs struct {
	// -1 if no more tasks
	WorkerId int
}

type MapTaskJoinArgs struct {
	// args that a worker sends to join a map task
	FilleId  int
	WorkerId int
}

type MapTaskJoinReply struct {
	// reply that a worker gets if it joins a map task
	Accepted bool
}

func mapDoneProcess(reply *MapTaskReply) {
	log.Println("All map tasks are done! Telling workers to switch to reduce tasks...")
	reply.DoneFlag = true
	reply.FileId = -1
}

func (c *Coordinator) GiveMapTask(args *MapTaskArgs, reply *MapTaskReply) error {
	if args.WorkerId == -1 {
		// allocate a new worker id
		reply.WorkerId = c.nowWorkerId
		c.nowWorkerId++
	} else {
		reply.WorkerId = args.WorkerId
	}
	log.Printf("Worker %d asks for a map task...\n", reply.WorkerId)

	// lock the mutex to keep the map task queue safe
	c.issueMapMutex.Lock()

	if c.mapDone {
		mapDoneProcess(reply)
		c.issueMapMutex.Unlock()
		// notify in yellow color
		log.Printf("\033[33mWorker %d: All map tasks are done! Telling workers to switch to reduce tasks...\033[0m\n", reply.WorkerId)
		return nil
	}

	if c.unIssuedMapTasks.size() == 0 && c.issuedMapTasks.Size() == 0 {
		// no more map tasks
		c.mapDone = true
		mapDoneProcess(reply)
		c.issueMapMutex.Unlock()
		// notify in yellow color
		log.Printf("\033[33mWorker %d: All map tasks are done! Telling workers to switch to reduce tasks...\033[0m\n", reply.WorkerId)
		return nil
	}

	// get a map task
	log.Printf("%v unissued map tasks, %v issued map tasks\n", c.unIssuedMapTasks.size(), c.issuedMapTasks.Size())

	// release the mutex to allow unissued map tasks to be issued
	c.issueMapMutex.Unlock()
	// get current time
	nowSecond := getNowSecond()

	// get a map task from the unissued map task queue
	res, err := c.unIssuedMapTasks.popBack()
	var fileId int
	if err != nil {
		log.Println("No more unissued map tasks for now, waiting...")
		fileId = -1

	} else {
		fileId = res.(int)
		// add this task to the issued map task queue
		c.issueMapMutex.Lock()
		reply.FileName = c.fileNames[fileId]
		c.mapTasks[fileId] = MapTaskState{
			beginSecond: nowSecond,
			workerId:    reply.WorkerId,
		}
		c.issuedMapTasks.Insert(fileId)
		// this operation is done, release the mutex
		c.issueMapMutex.Unlock()

		log.Printf("\033[1;32;40mgiving map task %v on file %v at second %v\033[0m\n", fileId, reply.FileName, nowSecond)
	}
	reply.FileId = fileId
	reply.NReduce = c.nReduce
	reply.DoneFlag = false

	return nil
}

func getNowSecond() int64 {
	return time.Now().UnixNano() / int64(time.Second)
}

func (c *Coordinator) joinMapTask(args *MapTaskJoinArgs, reply *MapTaskJoinReply) error {
	// check the current time for whether the worker is taking too long
	nowSecond := getNowSecond()
	log.Printf("got a join request from worker %v on file %v %v \n", args.WorkerId, args.FilleId, c.fileNames[args.FilleId])

	c.issueMapMutex.Lock()
	defer c.issueMapMutex.Unlock()

}

type ReduceTaskState struct {
	beginSecond int64
	workerId    int
	fileId      int
}

type ReduceTaskArgs struct {
	WorkerId int
}

type ReduceTaskReply struct {
	// RIndex: reduce index
	RIndex int
	// NReduce: number of reduce tasks
	NReduce int
	// FileCount: number of files to reduce
	FileCount int
	// DoneFlag: whether all reduce tasks are done
	DoneFlag bool
}

type ReduceTaskJoinArgs struct {
	// args that a worker sends to join a reduce task
	WorkerId int
	// RIndex: reduce index
	RIndex int
}

type ReduceTaskJoinReply struct {
	// reply that a worker gets if it joins a reduce task
	Accepted bool
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
	ret := true

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.server()
	return &c
}
