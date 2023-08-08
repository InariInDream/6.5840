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
	FileId   int
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
	log.Printf("got a join request from worker %v on file %v %v \n", args.WorkerId, args.FileId, c.fileNames[args.FileId])

	c.issueMapMutex.Lock()
	defer c.issueMapMutex.Unlock()

	taskTime := c.mapTasks[args.FileId].beginSecond

	// check if the task is still valid
	if !c.issuedMapTasks.Contains(args.FileId) {
		log.Println("task does not exist or already done, ignoring...")
		c.issueMapMutex.Unlock()
		reply.Accepted = false
		return nil
	}

	// check if the workerid matches
	if c.mapTasks[args.FileId].workerId != args.WorkerId {
		log.Printf("map task belongs to worker %v, not worker %v, ignoring...\n", c.mapTasks[args.FileId].workerId, args.WorkerId)
		c.issueMapMutex.Unlock()
		reply.Accepted = false
		return nil
	}

	if nowSecond-taskTime > maxTaxTime {
		log.Printf("worker %v is taking too long, ignoring...\n", args.WorkerId)
		reply.Accepted = false
		c.unIssuedMapTasks.appendFront(args.FileId)
	} else {
		log.Println("\033[1;32;40mjoin request accepted\033[0m")
		reply.Accepted = true
		c.issuedMapTasks.Delete(args.FileId)
	}
	c.issueMapMutex.Unlock()
	return nil
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

func (c *Coordinator) prepareAllReduceTasks() {
	// prepare all reduce tasks
	for i := 0; i < c.nReduce; i++ {
		log.Printf("putting %vth reduce task into channel\n", i)
		c.unIssuedReduceTasks.appendBack(i)
	}
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

func (c *Coordinator) GiveReduceTask(args *ReduceTaskArgs, reply *ReduceTaskReply) error {
	log.Printf("Worker %v asks for a reduce task...\n", args.WorkerId)
	c.issueReduceMutex.Lock()
	defer c.issueReduceMutex.Unlock()

	// all done
	if c.unIssuedReduceTasks.size() == 0 && c.issuedReduceTasks.Size() == 0 {
		// info in green color
		log.Println("\033[1;32;40mAll reduce tasks are done! Telling workers to switch to shut down..\033[0m")
		c.issueReduceMutex.Unlock()
		c.doneFlag = true
		reply.DoneFlag = true
		reply.RIndex = -1
		return nil
	}

	log.Printf("%v unissued reduce tasks, %v issued reduce tasks\n", c.unIssuedReduceTasks.size(), c.issuedReduceTasks.Size())
	// release the mutex to allow unissued reduce tasks to be issued
	c.issueReduceMutex.Unlock()

	nowSecond := getNowSecond()
	res, err := c.unIssuedReduceTasks.popBack()
	var rIndex int
	if err != nil {
		log.Println("No more unissued reduce tasks for now, waiting...")
		rIndex = -1
	} else {
		rIndex = res.(int)
		// add this task to the issued reduce task queue
		c.issueReduceMutex.Lock()
		c.reduceTasks[rIndex] = ReduceTaskState{
			beginSecond: nowSecond,
			workerId:    args.WorkerId,
		}
		c.issuedReduceTasks.Insert(rIndex)
		// this operation is done, release the mutex
		c.issueReduceMutex.Unlock()
		log.Printf("\033[1;32;40mgiving reduce task %v at second %v\033[0m\n", rIndex, nowSecond)
	}
	reply.RIndex = rIndex
	reply.NReduce = c.nReduce
	reply.FileCount = len(c.fileNames)
	reply.DoneFlag = false

	return nil
}

func (c *Coordinator) JoinReduceTask(args *ReduceTaskJoinArgs, reply *ReduceTaskJoinReply) error {
	// check the current time for whether the worker is taking too long
	nowSecond := getNowSecond()
	log.Printf("got a join request from worker %v on reduce task %v\n", args.WorkerId, args.RIndex)

	c.issueReduceMutex.Lock()
	defer c.issueReduceMutex.Unlock()

	taskTime := c.reduceTasks[args.RIndex].beginSecond

	if !c.issuedReduceTasks.Contains(args.RIndex) {
		log.Println("task does not exist or already done, ignoring...")
		c.issueReduceMutex.Unlock()
		reply.Accepted = false
		return nil
	}

	// check if the workerid matches
	if c.reduceTasks[args.RIndex].workerId != args.WorkerId {
		log.Printf("reduce task belongs to worker %v, not worker %v, ignoring...\n", c.reduceTasks[args.RIndex].workerId, args.WorkerId)
		c.issueReduceMutex.Unlock()
		reply.Accepted = false
		return nil
	}

	if nowSecond-taskTime > maxTaxTime {
		log.Printf("worker %v is taking too long, ignoring...\n", args.WorkerId)
		reply.Accepted = false
		c.unIssuedReduceTasks.appendFront(args.RIndex)
	} else {
		log.Println("\033[1;32;40mjoin request accepted\033[0m")
		reply.Accepted = true
		c.issuedReduceTasks.Delete(args.RIndex)
	}
	c.issueReduceMutex.Unlock()
	return nil
}

func (m *MapSet) removeTimeoutMapTasks(mapTasks []MapTaskState, unIssuedTasks *BlockQueue) {
	for fileId, issued := range m.mapbool {
		nowSecond := getNowSecond()
		if issued && nowSecond-mapTasks[fileId.(int)].beginSecond > maxTaxTime {
			log.Printf("worker %v is taking too long, putting %vth map task back to unissued queue...\n", mapTasks[fileId.(int)].workerId, fileId.(int))
			unIssuedTasks.appendFront(fileId.(int))
			m.Delete(fileId)
		}
	}
}

func (m *MapSet) removeTimeoutReduceTasks(reduceTasks []ReduceTaskState, unIssuedTasks *BlockQueue) {
	for rIndex, issued := range m.mapbool {
		nowSecond := getNowSecond()
		if issued && nowSecond-reduceTasks[rIndex.(int)].beginSecond > maxTaxTime {
			log.Printf("worker %v is taking too long, putting %vth reduce task back to unissued queue...\n", reduceTasks[rIndex.(int)].workerId, rIndex.(int))
			unIssuedTasks.appendFront(rIndex.(int))
			m.Delete(rIndex)
		}
	}
}

func (c *Coordinator) removeTimeoutTasks() {
	log.Println("removing timeout tasks...")
	c.issueMapMutex.Lock()
	c.issuedMapTasks.removeTimeoutMapTasks(c.mapTasks, c.unIssuedMapTasks)
	c.issueMapMutex.Unlock()
	c.issueReduceMutex.Lock()
	c.issuedReduceTasks.removeTimeoutReduceTasks(c.reduceTasks, c.unIssuedReduceTasks)
	c.issueReduceMutex.Unlock()
}

func (c *Coordinator) loopRmTimeoutTasks() {
	for !c.doneFlag {
		c.removeTimeoutTasks()
		time.Sleep(2 * time.Second)
	}
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
	//ret := true

	// Your code here.
	if c.doneFlag {
		log.Println("asked whether the job is done, replying true...")
	} else {
		log.Println("asked whether the job is done, replying false...")
	}
	return c.doneFlag
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// main/mrcoordinator.go calls this function.

	log.SetPrefix("Coordinator: ")
	log.Println("initializing a coordinator...")

	c = Coordinator{
		fileNames:           files,
		nReduce:             nReduce,
		nowWorkerId:         0,
		mapTasks:            make([]MapTaskState, len(files)),
		reduceTasks:         make([]ReduceTaskState, nReduce),
		unIssuedMapTasks:    NewBlockQueue(),
		issuedMapTasks:      NewMapSet(),
		unIssuedReduceTasks: NewBlockQueue(),
		issuedReduceTasks:   NewMapSet(),
		mapDone:             false,
		doneFlag:            false,
	}

	// start a thread that listens for RPCs from worker.go
	c.server()
	log.Println("listening for RPCs from worker.go...")

	// start a new goroutine to remove timeout tasks
	go c.loopRmTimeoutTasks()

	log.Printf("file count: %v\n", len(files))
	for i := 0; i < len(files); i++ {
		log.Printf("putting %vth map task into channel\n", i)
		c.unIssuedMapTasks.appendBack(i)
	}

	return &c
}
