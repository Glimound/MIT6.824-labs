package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// RPC definitions.

type GetTaskArgs struct {
}

type GetTaskReply struct {
	Wait      bool
	FileName  string
	NReduce   int
	MMap      int
	IsMapTask bool
	TaskNum   int
}

type FinishTaskArgs struct {
	TaskNum   int
	IsMapTask bool
}

type FinishTaskReply struct {
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
