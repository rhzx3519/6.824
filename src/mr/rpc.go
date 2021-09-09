package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

//---------------------------------------------------------

// MR CurrentStage
type Stage int

const (
	STAGE_MAP =	1 << iota
	STAGE_REDUCE
	STAGE_END
)


type TaskStatus int

const (
	Todo =	1 << iota
	Doing
	Done
)


type TaskType int

const (
	Map = 1 << iota
	Reduce
)

type MapTask struct {
	id 			int
	filename 	string
	status 		TaskStatus
}

type ReduceTask struct {
	id 		int
	status 	TaskStatus
}

type ApplyTaskArgs struct {

}

//
type ApplyTaskReply struct {
	Result bool
	Message string
	TaskType TaskType	// 任务类型
	Filename string		// 文件名
	NumberOfReduce	int			// reduce 任务数量
	MapTaskId	int			// map 任务id
	ReduceTaskId int		// reduce 任务id
}

//---------------------------------------------------------
// 任务执行结果
type TaskResultArgs struct {
	Success bool
	Message string
	TaskType TaskType
	MapTaskId	int			// map 任务id
	ReduceTaskId int		// reduce 任务id
}

type TaskResultReply struct {
	CurrentStage Stage
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
