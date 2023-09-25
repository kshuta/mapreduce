package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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

type JobType int

const (
	mapJob JobType = iota
	reduceJob
	doneJob
)

// Add your RPC definitions here.

type Args struct {
	IntermediateFiles []string
	Source            string
	Job               JobType
	ReducedFiles      []string
	ReduceTask        int
}

type Reply struct {
	FileName    string
	FileContent []byte
	Job         JobType
	WorkerID    int
	NReduce     int
	MapJobDone  bool
	ReduceFiles []string
	ReduceTask  int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
