package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

type Placeholder struct{}

type FinishArgs struct {
	IsMap bool
	Id    int
}

type TaskState int

const (
	Pending TaskState = iota
	Executing
	Finished
)

type TaskMeta struct {
	State     TaskState
	StartTime time.Time
	Id        int
}

type MapTask struct {
	TaskMeta
	Filename string
}

type ReduceTask struct {
	TaskMeta
	IntermediateFilenames []string
}

type TaskOperation int

const (
	ToWait TaskOperation = iota
	ToRun
)

type Task struct {
	Operation TaskOperation
	IsMap     bool
	NReduce   int
	Map       MapTask
	Reduce    ReduceTask
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
