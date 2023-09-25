package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net/rpc"
	"os"
	"time"
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

// main/mrworker.go calls this function.
func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
	log *slog.Logger,
) {
	// Your worker implementation here.
	// I need a worker ID.
	workerID := 0

	for {
		reply := CallGetWork()
		if workerID == 0 {
			workerID = reply.WorkerID
		}
		log.Info("starting worker process", "workerID", workerID, "work", reply)
		if reply.Job == mapJob {
			intermediate := mapf(reply.FileName, string(reply.FileContent))
			fileNames := make([]string, 0)
			// encode intermediate values to json keypair
			for _, kv := range intermediate {
				reduceID := ihash(kv.Key) % reply.NReduce
				fileName := fmt.Sprintf("mr-%d-%d.json", workerID, reduceID)
				fileNames = append(fileNames, fileName)
				file, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
				if err != nil {
					log.Error("failed to open intermediate file", "err", err)
					break
				}
				enc := json.NewEncoder(file)
				if err := enc.Encode(&kv); err != nil {
					log.Error("failed to encode kv pair", "err", err)
					break
				}
			}

			CallPutWork(Args{
				IntermediateFiles: fileNames,
				Source:            reply.FileName,
			})
		} else if reply.Job == reduceJob {
			// wait until map job is done.
			for !CallCheckMapJobs() {
				// sleep
				<-time.NewTimer(3 * time.Second).C
			}

			// do the reduce job.
			// receive the specified files from master.
			// read the contents of the files.
			// sort
			// perform reduce.
			log.Info("running reduce", "reduceTask", reply.ReduceFiles)
			args := Args{
				ReduceTask:   reply.ReduceTask,
				ReducedFiles: reply.ReduceFiles,
				Job:          reduceJob,
			}
			CallPutWork(args)

		} else {
			break
		}

		time.Sleep(time.Second)
	}

}

func CallCheckMapJobs() bool {
	args := Args{}
	reply := Reply{}
	call("Master.GetMapJobStatus", &args, &reply)
	return reply.MapJobDone
}

func CallPutWork(args Args) *Reply {
	reply := new(Reply)
	call("Master.PutWork", args, reply)
	return reply
}

// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func CallGetWork() *Reply {
	args := Args{}
	reply := new(Reply)
	call("Master.GetWork", &args, reply)

	return reply
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		slog.Error("error dialing socket", "err", err)
		os.Exit(1)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	slog.Error("error calling rpc", "err", err.Error())
	return false
}
