package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func doMap(mapf func(string, string) []KeyValue, job *Job, nReduce int) (files []string) {
	outFiles := make([]*os.File, nReduce)
	for idx := range outFiles {
		outFile, err := ioutil.TempFile("./", "mr-tmp-*")
		if err != nil {
			log.Fatalf("create tmp file failed: %v", err)
		}
		defer outFile.Close()
		outFiles[idx] = outFile
	}
	for _, filename := range job.Files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		for _, kv := range kva {
			hash := ihash(kv.Key) % nReduce
			js, _ := json.Marshal(kv)
			outFiles[hash].Write(js)
			outFiles[hash].WriteString("\n")
		}
	}
	for idx := range outFiles {
		filename := fmt.Sprintf("mr-%d-%d", job.Index, idx)
		os.Rename(outFiles[idx].Name(), filename)
		files = append(files, filename)
	}
	return
}

func doReduce(reducef func(string, []string) string, job *Job, nMap int) {
	log.Printf("Start reduce %d", job.Index)
	outFile, err := ioutil.TempFile("./", "mr-out-tmp-*")
	defer outFile.Close()
	if err != nil {
		log.Fatalf("create tmp file failed: %v", err)
	}
	m := make(map[string][]string)
	for _, filename := range job.Files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			kv := KeyValue{}
			if err := json.Unmarshal(scanner.Bytes(), &kv); err != nil {
				log.Fatalf("read kv failed: %v", err)
			}
			m[kv.Key] = append(m[kv.Key], kv.Value)
		}
		if err := scanner.Err(); err != nil {
			log.Fatal(err)
		}
		file.Close()
	}
	for key, value := range m {
		output := reducef(key, value)
		fmt.Fprintf(outFile, "%v %v\n", key, output)
	}
	os.Rename(outFile.Name(), fmt.Sprintf("mr-out-%d", job.Index))
	log.Printf("End reduce %d", job.Index)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	CallExample()
	var status int = MAP
	args := AcquireJobArgs{Job{Index: -1}, MAP}
	for {
		args.Status = status
		reply := AcquireJobReply{}
		call("Coordinator.AcquireJob", &args, &reply)
		fmt.Printf("AcReply: %+v\n", reply)
		if reply.Status == FINISH {
			break
		}
		status = reply.Status
		if reply.Job.Index >= 0 {
			// get a job, do it
			commitJob := reply.Job
			if status == MAP {
				commitJob.Files = doMap(mapf, &reply.Job, reply.NOther)
			} else {
				doReduce(reducef, &reply.Job, reply.NOther)
				commitJob.Files = make([]string, 0)
			}
			// job finished
			args = AcquireJobArgs{commitJob, status}
		} else {
			// no job, sleep to wait
			time.Sleep(time.Second)
			args = AcquireJobArgs{Job{Index: -1}, status}
		}
	}
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
