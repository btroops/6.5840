package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"strings"
)

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

var coordSockName string // socket for coordinator
var nReduce int
var workerId int

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	// 初始化
	Init()
	// 处理 map 任务
	DoMapTask(mapf)
	// 处理reduce 任务
	DoReduceTask(reducef)

}
func Init() {
	args := RequestInitArgs{}
	reply := RequestInitReply{}
	ok := call("Coordinator.RequestInitRPC", &args, &reply)
	if ok {
		nReduce = reply.NReduce
		workerId = reply.WorkerId
	} else {

	}

}
func DoMapTask(mapf func(string, string) []KeyValue) {
	data := make([]map[string][]string, nReduce)
	for i := range data {
		data[i] = make(map[string][]string)
	}
	for {
		args := RequestMapTaskArgs{}
		reply := RequestMapTaskReply{}
		ok := call("Coordinator.RequestMapTaskRPC", &args, &reply)
		// fmt.Printf()
		if ok && reply.FileName != "" {
			file, err := os.Open(reply.FileName)
			if err != nil {
				log.Fatalf("cannot open %v", reply.FileName)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.FileName)
			}
			file.Close()
			// fmt.Println(string(content))
			kva := mapf(reply.FileName, string(content))

			for _, kv := range kva {
				key := kv.Key
				value := kv.Value
				data_index := ihash(key) % nReduce
				values, exists := data[data_index][key]
				if !exists {
					values = make([]string, 0)
				}
				values = append(values, value)
				data[data_index][key] = values
			}
		} else {
			break
		}
	}
	mapOutPutFileNames := []string{}
	for i := 0; i < len(data); i++ {
		mapOutputFileName := fmt.Sprintf("map_worker_%d_%d.txt", workerId, i)
		file, err := os.Create(mapOutputFileName)
		if err != nil {
			log.Fatalf("failed to create %s: %v", mapOutputFileName, err)
		}
		for key, values := range data[i] {
			fmt.Fprintf(file, "%s %d\n", key, len(values))
		}
		file.Close()
		mapOutPutFileNames = append(mapOutPutFileNames, mapOutputFileName)
	}
	// 告诉coordinator 我这边的任务已经处理好了
	MapTasksDone(mapOutPutFileNames)
}

func MapTasksDone(files []string) {
	args := MapTaskEndArgs{
		FileNames: files,
		WorkerId:  workerId,
	}
	ok := call("Coordinator.MapTasksDone", &args, &struct{}{})
	if ok {

	} else {

	}
}

func DoReduceTask(reducef func(string, []string) string) error {
	// 向coordinator请求reduce任务
	data := make(map[string]int)

	for {
		args := RequestReduceTaskArgs{
			WorkerId: workerId,
		}
		reply := RequestReduceReply{}
		ok := call("Coordinator.RequestReduceTask", &args, &reply)
		if ok && reply.FileName != "" {
			file, err := os.Open(reply.FileName)
			if err != nil {
				log.Fatalf("cannot open %v", reply.FileName)
			}
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				line := strings.TrimSpace(scanner.Text())
				if line == "" {
					continue
				}
				k, v := strings.Fields(line)[0], strings.Fields(line)[1]
				num, _ := strconv.Atoi(v)
				data[k] += num
			}
		} else {
			break
		}
	}
	fileName := "mr-out-0"
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	for k, v := range data {
		fmt.Fprintf(file, "%s %d\n", k, v)
	}

	return nil

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
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
