package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

var nfile int
var wokerNumber int

type Coordinator struct {
	// Your definitions here.
	// nReduce
	// worker
	// files
	Files            []string
	NReduce          int
	MMapOutFileNames map[int][]string
	MapOutFileNames  []string
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestInitRPC(args *RequestInitArgs, reply *RequestInitReply) error {
	reply.NReduce = c.NReduce
	reply.WorkerId = wokerNumber
	wokerNumber++
	return nil
}

// 请求一个MapTask
func (c *Coordinator) RequestMapTaskRPC(args *RequestMapTaskArgs, reply *RequestMapTaskReply) error {

	if nfile < len(c.Files) {
		reply.FileName = c.Files[nfile]
		nfile++
	}
	return nil
}

func (c *Coordinator) MapTasksDone(args *MapTaskEndArgs, reply *struct{}) error {
	// 追加模式，不会覆盖，兼容初始为空
	c.MMapOutFileNames[args.WorkerId] = append(c.MMapOutFileNames[args.WorkerId], args.FileNames...)
	c.MapOutFileNames = append(c.MapOutFileNames, args.FileNames...)
	return nil
}

func (c *Coordinator) RequestReduceTask(arg *RequestReduceTaskArgs, reply *RequestReduceReply) error {
	if len(c.MapOutFileNames) > 0 {
		reply.FileName = c.MapOutFileNames[0]
		c.MapOutFileNames = c.MapOutFileNames[1:]
	} else {
		reply.FileName = ""
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Files:            files,
		NReduce:          nReduce,
		MMapOutFileNames: make(map[int][]string),
		MapOutFileNames:  make([]string, 0),
	}
	c.server(sockname)
	return &c
}
