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
	Files   []string
	NReduce int
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
	c := Coordinator{}

	// Your code here.
	c.Files = append([]string{}, files...)
	c.NReduce = nReduce
	c.server(sockname)
	return &c
}
