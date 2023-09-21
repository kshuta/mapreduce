package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	mu                sync.Mutex
	mapTaskNum        int
	next              int
	mappedFiles       map[string]bool
	reduceTasks       []string
	workerID          int
	nReduce           int
	intermediateFiles []string
	mapJobChan        chan string
	mapJobSignal      chan bool
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) mapJobDone() bool {
	return m.mapTaskNum == len(m.mappedFiles)
}

func (m *Master) GetWork(args *Args, reply *Reply) error {

	source, ok := <-m.mapJobChan
	if ok {
		body, err := os.ReadFile(source)
		if err != nil {
			return err
		}

		reply.FileContent = body
		reply.FileName = source
		reply.Job = mapJob
		reply.WorkerID = m.workerID
		reply.NReduce = m.nReduce

		m.workerID += 1

		go func(source string) {
			timer := time.NewTimer(10 * time.Second)
			<-timer.C
			m.mu.Lock()
			_, ok := m.mappedFiles[source]
			m.mu.Unlock()
			if !ok {
				m.mapJobChan <- source
			}
		}(source)
		return nil
	}

	// reduce phase

	return nil
}

func (m *Master) PutWork(args *Args, reply *Reply) error {
	// retrieve reduce task number from worker, and store it for later use.
	m.intermediateFiles = append(m.intermediateFiles, args.IntermediateFiles...)
	m.mappedFiles[args.Source] = true
	m.mapJobSignal <- true
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	// ret := false

	// Your code here.
	for i := 0; i < m.mapTaskNum; i++ {
		<-m.mapJobSignal
	}
	close(m.mapJobChan)
	log.Println("finished map job!")

	return true
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.mapTaskNum = len(files)
	m.next = 0
	m.nReduce = nReduce
	m.workerID = 1
	m.intermediateFiles = make([]string, 0)
	m.mappedFiles = make(map[string]bool)
	m.mapJobSignal = make(chan bool, len(files))
	m.mapJobChan = make(chan string, len(files))
	for _, file := range files {
		m.mapJobChan <- file
	}

	// Your code here.
	m.server()
	return &m
}
