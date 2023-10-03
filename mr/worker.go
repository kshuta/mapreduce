package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"net/rpc"
	"os"
	"sort"
	"time"

	"golang.org/x/exp/maps"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	workerID := 0

	for {
		reply := CallGetWork()
		if workerID == 0 {
			workerID = reply.WorkerID
		}
		log.Info("starting worker process", "workerID", workerID, "work", reply)
		if reply.Job == mapJob {
			intermediate := mapf(reply.FileName, string(reply.FileContent))
			fileNames := make([]string, 0) // make this into a set, so there are no duplicate files passed.
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
			kvs := make([]KeyValue, 0)
			reducedFiles := make(map[string]bool)
			for _, file := range reply.ReduceFiles {
				if _, ok := reducedFiles[file]; ok {
					continue
				}
				f, err := os.Open(file)
				if err != nil {
					log.Error("failed opening file to reduce", "err", err.Error())
					break
				}
				dec := json.NewDecoder(f)
				for {
					log.Info("decoding json values", "filename", file)
					var kv KeyValue
					if err := dec.Decode(&kv); err == io.EOF {
						break
					} else if err != nil {
						slog.Error("failed to decode json content", "err", err)
						return
					}
					kvs = append(kvs, kv)
				}
				log.Info("finished decoding json values", "kvs", kvs)
				f.Close()
				reducedFiles[file] = true
			}

			// sort
			sort.Sort(ByKey(kvs))

			ofile, err := os.CreateTemp("", "tmp")
			if err != nil {
				slog.Error("err opening temp file", "err", err)
			}
			// perform reduce.
			i := 0
			for i < len(kvs) {
				j := i + 1
				for j < len(kvs) && kvs[j].Key == kvs[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kvs[k].Value)
				}
				slog.Info("about to run reduce", "values", values)
				output := reducef(kvs[i].Key, values)

				slog.Info("writing to file", "key", kvs[i].Key, "output", output)
				// write to file
				fmt.Fprintf(ofile, "%v %v\n", kvs[i].Key, output)
				i = j
			}
			ofile.Close()
			os.Rename(ofile.Name(), fmt.Sprintf("mr-out-%d", reply.ReduceTask))

			log.Info("running reduce", "reduceTask", reply.ReduceFiles)
			args := Args{
				ReduceTask:   reply.ReduceTask,
				ReducedFiles: maps.Keys(reducedFiles),
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
