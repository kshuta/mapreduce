package mr

import (
	"fmt"
	"log"
	"log/slog"
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
	reducedFiles      []string
	workerID          int
	nReduce           int
	intermediateFiles map[int][]string
	mapJobChan        chan string
	mapJobSignal      map[string]chan bool
	reduceJobSignal   map[int]chan bool
	reducedTask       []int
	reduceJobs        chan int
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
	// if it is done, feed intermediate files into channels.
	return m.mapTaskNum == len(m.mappedFiles)
}

func (m *Master) reduceJobDone() bool {
	slog.Info("length of reducedTask", "len", len(m.reducedTask))
	return m.nReduce == len(m.reducedTask)
}

func (m *Master) GetMapJobStatus(args *Args, reply *GetMapJobStatusReply) error {
	reply.MapJobDone = m.mapJobDone()
	return nil
}

func (m *Master) GetWork(args *Args, reply *Reply) error {

	reply.WorkerID = m.workerID
	m.workerID += 1
	source, ok := <-m.mapJobChan
	if ok {
		reply.Job = mapJob
		mapReply := new(MapJobReply)
		slog.Info("getting work", "filename", source, "jobtype", mapJob)
		body, err := os.ReadFile(source)
		if err != nil {
			return err
		}

		mapReply.FileContent = body
		mapReply.FileName = source
		mapReply.NReduce = m.nReduce

		go func(source string) {
			timer := time.NewTimer(10 * time.Second)
			select {
			case <-timer.C:
				// spin up new process.
				slog.Info("worker timedout", "filename", source)
				m.mapJobChan <- source
			case <-m.mapJobSignal[source]:
				// done with job.
				slog.Info("finished work", "filename", source)
				m.mappedFiles[source] = true
			}
		}(source)
		reply.MapJobReply = *mapReply
		return nil
	}

	// reduce phase
	reduceTask, ok := <-m.reduceJobs
	if ok {
		reduceReply := new(ReduceJobReply)
		slog.Info("getting work", "reduceTask", reduceTask, "jobtype", reduceJob)
		reduceReply.ReduceFiles = m.intermediateFiles[reduceTask]
		reduceReply.ReduceTask = reduceTask

		go func(reduceTask int) {
			timer := time.NewTimer(10 * time.Second)
			select {
			case <-timer.C:
				slog.Info("worker timeout", "reducetask", reduceTask)
				m.reduceJobs <- reduceTask
			case <-m.reduceJobSignal[reduceTask]:
				slog.Info("finished reduce work", "reducetask", reduceTask)
				m.reducedTask = append(m.reducedTask, reduceTask)
			}
		}(reduceTask)

		reply.ReduceJobReply = *reduceReply
		return nil
	}

	reply.Job = doneJob

	return nil
}

func (m *Master) PutWork(args *Args, reply *Reply) error {
	// retrieve reduce task number from worker, and store it for later use.
	if args.Job == mapJob {
		slog.Info("putting work", "intermediateFiles", args.IntermediateFiles)
		for _, file := range args.IntermediateFiles {
			var taskNum int
			var tmp int
			fmt.Sscanf(file, "mr-%d-%d.json", &tmp, &taskNum)

			m.mu.Lock()
			slog.Info("writing to intermediate files", "tasknum", taskNum)
			m.intermediateFiles[taskNum] = append(m.intermediateFiles[taskNum], file)
			m.mu.Unlock()
		}
		m.mapJobSignal[args.Source] <- true
	} else {
		slog.Info("putting work", "reducedFiles", args.ReducedFiles, "reduceTask", args.ReduceTask)
		m.mu.Lock()
		m.reducedFiles = append(m.reducedFiles, args.ReducedFiles...)
		m.mu.Unlock()

		m.reduceJobSignal[args.ReduceTask] <- true

	}
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

	for {
		if m.mapJobDone() {
			break
		}
		time.Sleep(time.Second)
	}
	close(m.mapJobChan)
	slog.Info("finished map job")

	// wait for reduce job.
	for {
		if m.reduceJobDone() {
			break
		}
		time.Sleep(time.Second)
	}
	close(m.reduceJobs)
	slog.Info("finished reduce job")

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
	m.intermediateFiles = make(map[int][]string)
	for i := 0; i < nReduce; i++ {
		m.intermediateFiles[i] = make([]string, 0)
	}
	m.reduceJobs = make(chan int, nReduce)
	m.reduceJobSignal = make(map[int]chan bool)
	for i := 0; i < nReduce; i++ {
		m.reduceJobs <- i
		m.reduceJobSignal[i] = make(chan bool, 1)
	}

	m.mappedFiles = make(map[string]bool)
	m.mapJobSignal = make(map[string]chan bool)
	m.reducedFiles = make([]string, 0)

	m.mapJobChan = make(chan string, len(files))
	for _, file := range files {
		m.mapJobChan <- file
		m.mapJobSignal[file] = make(chan bool, 1)
	}

	// Your code here.
	m.server()
	return &m
}
