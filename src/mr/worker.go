package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type woker struct {
	mapf 	func(string, string) ([]KeyValue)
	reducef func(string, []string) string
	taskType TaskType
	filename string		// 文件名
	numberOfReduce	int			// reduce 任务数量
	mapTaskId	int			// map 任务id
	reduceTaskId int		// reduce 任务id
	stage Stage				// MapReduce stage
}

func (w *woker) callApplyTask() {
	args := ApplyTaskArgs{}
	reply := ApplyTaskReply{}
	call("Coordinator.ApplyTask", &args, &reply)
	if !reply.Result {
		//log.Println("There is no task needed to be executed...")
		return
	}
	w.taskType = reply.TaskType
	w.filename = reply.Filename
	w.mapTaskId  = reply.MapTaskId
	w.reduceTaskId = reply.ReduceTaskId
	w.numberOfReduce = reply.NumberOfReduce

	//fmt.Printf("Receive task reply, taskType: %v, mapTaskId: %v, reduceTaskId: %v\n",
	//	w.taskType, w.mapTaskId, w.reduceTaskId)

	var err error
	switch reply.TaskType {
	case Map:
		err = w.handleMapTask(reply.Filename, reply.MapTaskId, reply.NumberOfReduce)
	case Reduce:
		err = w.handleReduceTask(reply.ReduceTaskId)
	}

	if err != nil {
		//log.Println(err)
		w.callTaskResult(false)
	} else {
		w.callTaskResult(true)
	}

}

// Execute mapf task
func (w *woker) handleMapTask(filename string, taskId int, nReduce int) (err error) {
	intermediate := []KeyValue{}
	file, err := os.Open(filename)
	if err != nil {
		//log.Println("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		//log.Println("cannot read %v", filename)
	}
	file.Close()
	kva := w.mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	var openFiles = make(map[string]*os.File)
	defer func() {
		for filename, f := range openFiles {
			os.Rename(f.Name(), filename)
			f.Close()
		}
	}()

	for _, kv := range intermediate {
		y := ihash(kv.Key) % nReduce
		ofilename := fmt.Sprintf("mr-%d-%d", taskId, y)
		if _, ok := openFiles[ofilename]; !ok {
			tmpFile, err := ioutil.TempFile("./", "intermediate")
			//f, err := os.OpenFile(ofilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				//log.Println(err)
				continue
			}
			openFiles[ofilename] = tmpFile
		}
		// If the file doesn't exist, create it, or append to the file f,
		f := openFiles[ofilename]
		enc := json.NewEncoder(f)
		err = enc.Encode(&kv)
		if err != nil {
			//log.Println(err)
		}
	}

	return
}

// Execute reducef task
func (w *woker) handleReduceTask(reduceTaskId int) (err error) {
	suffix := fmt.Sprintf("-%d", reduceTaskId)
	files, _ :=ioutil.ReadDir("./")

	intermediate := []KeyValue{}
	for _, fileinfo := range files {
		if !fileinfo.IsDir() && strings.HasSuffix(fileinfo.Name(), suffix) {
			f, err := os.Open(fileinfo.Name())
			if err != nil {
				//log.Println(err)
				continue
			}
			dec := json.NewDecoder(f)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				intermediate = append(intermediate, kv)
			}
			f.Close()
		}
	}

	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%d", reduceTaskId)
	tmpFile, err := ioutil.TempFile("./", "reducing")

	defer func() {
		os.Rename(tmpFile.Name(), oname)
		tmpFile.Close()
	}()

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
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
		output := w.reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	return
}

//  向Coordinator返回任务执行结果
func (w *woker) callTaskResult(success bool) {
	args := TaskResultArgs{}
	args.Success = success
	args.MapTaskId = w.mapTaskId
	args.ReduceTaskId = w.reduceTaskId
	args.TaskType = w.taskType

	reply := TaskResultReply{}

	call("Coordinator.ReportTaskResult", &args, &reply)
	// update current mr CurrentStage
	w.stage = reply.CurrentStage
}

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
	w := woker{
		mapf: mapf,
		reducef: reducef,
	}

	for {
		w.callApplyTask()
		if w.stage == STAGE_END {
			break
		}
		time.Sleep(time.Second)
	}

	//log.Println("Worker shut down...")
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
