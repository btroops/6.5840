package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Init
type RequestInitArgs struct{}
type RequestInitReply struct {
	NReduce  int
	WorkerId int
}

// 定义Worker状态枚举类型
type WorkerStatus int

// 枚举常量：使用 iota 自动自增赋值
const (
	IDLE          WorkerStatus = iota // 0：空闲
	MAP_WORKER                        // 1：执行Map任务
	REDUCE_WORKER                     // 2：执行Reduce任务
)

type RequestMapTaskArgs struct {
	WorkerStatus WorkerStatus
}

type RequestMapTaskReply struct {
	FileName string
}

type MapTaskEndArgs struct {
	FileNames []string
	WorkerId  int
}

type RequestReduceTaskArgs struct {
	WorkerId int
}

type RequestReduceReply struct {
	FileName string
}
