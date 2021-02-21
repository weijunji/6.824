package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Job status
const (
	WAITTING = iota
	RUNNING  = iota
	FINISHED = iota
)

// Job need to map
type Job struct {
	Files  []string
	Status int
	Index  int
}

type Coordinator struct {
	mapJobs      []Job
	reduceJobs   []Job
	status       int
	nMap         int
	remainMap    int
	nReduce      int
	remainReduce int
	lock         sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	return c.status == FINISH
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.status = MAP
	c.nMap = len(files)
	c.remainMap = c.nMap
	c.nReduce = nReduce
	c.remainReduce = c.nReduce
	c.mapJobs = make([]Job, len(files))
	c.reduceJobs = make([]Job, nReduce)
	for idx, file := range files {
		c.mapJobs[idx] = Job{[]string{file}, WAITTING, idx}
	}
	for idx := range c.reduceJobs {
		c.reduceJobs[idx] = Job{[]string{}, WAITTING, idx}
	}
	c.server()
	return &c
}

func (c *Coordinator) timer(status *int) {
	time.Sleep(time.Second * 10)

	c.lock.Lock()
	if *status == RUNNING {
		log.Printf("timeout\n")
		*status = WAITTING
	}
	c.lock.Unlock()
}

func (c *Coordinator) AcquireJob(args *AcquireJobArgs, reply *AcquireJobReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	fmt.Printf("Acquire: %+v\n", args)
	if args.CommitJob.Index >= 0 {
		if args.Status == MAP {
			if c.mapJobs[args.CommitJob.Index].Status == RUNNING {
				c.mapJobs[args.CommitJob.Index].Status = FINISHED
				for idx, file := range args.CommitJob.Files {
					c.reduceJobs[idx].Files = append(c.reduceJobs[idx].Files, file)
				}
				c.remainMap--
			}
			if c.remainMap == 0 {
				c.status = REDUCE
			}
		} else {
			if c.reduceJobs[args.CommitJob.Index].Status == RUNNING {
				c.reduceJobs[args.CommitJob.Index].Status = FINISHED
				c.remainReduce--
			}
			if c.remainReduce == 0 {
				c.status = FINISH
			}
		}
	}
	if c.status == MAP {
		for idx := range c.mapJobs {
			if c.mapJobs[idx].Status == WAITTING {
				reply.NOther = c.nReduce
				reply.Status = MAP
				reply.Job = c.mapJobs[idx]
				c.mapJobs[idx].Status = RUNNING
				go c.timer(&c.mapJobs[idx].Status)
				return nil
			}
		}
		reply.NOther = c.nReduce
		reply.Status = MAP
		reply.Job = Job{Files: make([]string, 0), Index: -1}
	} else if c.status == REDUCE {
		for idx := range c.reduceJobs {
			if c.reduceJobs[idx].Status == WAITTING {
				reply.NOther = c.nMap
				reply.Status = REDUCE
				reply.Job = c.reduceJobs[idx]
				c.reduceJobs[idx].Status = RUNNING
				go c.timer(&c.reduceJobs[idx].Status)
				return nil
			}
		}
		reply.NOther = c.nMap
		reply.Status = REDUCE
		reply.Job = Job{Files: make([]string, 0), Index: -1}
	} else {
		reply.Status = FINISH
	}
	return nil
}
