package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

var mutex sync.Mutex

type Coordinator struct {
	nReduce        int
	files          []string
	filesInProcess []string
	// 任务编号（递增）
	mapTaskCounter    int
	reduceTaskCounter int
	mapTasks          []MapTaskInfo
	reduceTasks       []ReduceTaskInfo
}

// 已分配的Map任务信息
type MapTaskInfo struct {
	fileName  string
	taskNum   int
	startTime time.Time
	done      bool
	crash     bool
}

// 已分配的Map任务信息
type ReduceTaskInfo struct {
	taskNum   int
	startTime time.Time
	done      bool
	crash     bool
}

// RPC handler
// 在worker请求后返回reduce任务的数量以及需要读入的文件名
func (c *Coordinator) HandleGetTaskRequest(args *GetTaskArgs, reply *GetTaskReply) error {
	mutex.Lock()
	defer mutex.Unlock()

	// 若仍有文件待处理
	if len(c.files) != 0 {
		// 分配map任务
		c.filesInProcess = append(c.filesInProcess, c.files[0])
		taskInfo := MapTaskInfo{
			fileName:  c.files[0],
			taskNum:   c.mapTaskCounter,
			startTime: time.Now(),
			done:      false,
			crash:     false,
		}
		c.mapTasks = append(c.mapTasks, taskInfo)

		reply.FileName = c.files[0]
		reply.NReduce = c.nReduce
		reply.IsMapTask = true
		reply.TaskNum = c.mapTaskCounter

		c.files = c.files[1:]
		c.mapTaskCounter++
	} else {
		allDone := true
		// 遍历任务列表，检查是否所有的map任务都已经完成
		// 同时检查是否存在崩溃任务，若有则重新分配崩溃任务
		for _, v := range c.mapTasks {
			if !v.done {
				allDone = false
			}
			if v.crash {
				// 重新设置任务状态及时间
				v.crash = false
				v.startTime = time.Now()

				reply.FileName = v.fileName
				reply.NReduce = c.nReduce
				reply.IsMapTask = true
				reply.TaskNum = v.taskNum
				break
			}
		}
		// 若所有map任务都已完成
		if allDone {
			// 判断reduce任务是否分配完
			if c.reduceTaskCounter < c.nReduce {
				// 若无，则分配reduce任务
				taskInfo := ReduceTaskInfo{
					taskNum:   c.reduceTaskCounter,
					startTime: time.Now(),
					done:      false,
					crash:     false,
				}
				c.reduceTasks = append(c.reduceTasks, taskInfo)

				reply.MMap = c.mapTaskCounter
				reply.IsMapTask = false
				reply.TaskNum = c.reduceTaskCounter

				c.reduceTaskCounter++
			} else {
				// 遍历任务列表，检查是否存在崩溃任务，若有则重新分配崩溃任务
				for _, v := range c.reduceTasks {
					if v.crash {
						// 重新设置任务状态及时间
						v.crash = false
						v.startTime = time.Now()

						reply.MMap = c.mapTaskCounter
						reply.IsMapTask = false
						reply.TaskNum = v.taskNum
						return nil
					}
				}
				// 若已分配完且不存在崩溃，则返回等待指令
				reply.Wait = true
			}
		}
	}
	return nil
}

// RPC handler
// 接收到任务完成通知时修改相应状态
func (c *Coordinator) HandleFinishTaskRequest(args *FinishTaskArgs, reply *FinishTaskReply) error {
	mutex.Lock()
	defer mutex.Unlock()

	if args.IsMapTask {
		c.mapTasks[args.taskNum].done = true
	} else {
		c.reduceTasks[args.taskNum].done = true
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := true
	mutex.Lock()
	defer mutex.Unlock()

	if c.reduceTaskCounter < c.nReduce {
		ret = false
	} else {
		// 判断是否所有的reduce任务都完成
		for _, v := range c.reduceTasks {
			if !v.done {
				ret = false
				break
			}
		}
	}

	return ret
}

// 定期检查任务是否超时
// 超时的任务会被设置为crash
func (c *Coordinator) timeoutChecker() {
	for {
		mutex.Lock()
		for _, task := range c.mapTasks {
			if !task.done && time.Now().Sub(task.startTime) > time.Second*10 {
				task.crash = true
			}
		}
		for _, task := range c.reduceTasks {
			if !task.done && time.Now().Sub(task.startTime) > time.Second*10 {
				task.crash = true
			}
		}
		mutex.Unlock()
		time.Sleep(time.Second)
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:           nReduce,
		files:             files,
		filesInProcess:    make([]string, 0, len(files)),
		mapTaskCounter:    0,
		reduceTaskCounter: 0,
		mapTasks:          make([]MapTaskInfo, 0),
		reduceTasks:       make([]ReduceTaskInfo, 0),
	}

	// Your code here.

	c.server()
	return &c
}
