package mr

import (
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type TaskType int

const TIMEOUT float64 = 10.0

// 任务类型
const (
	MapTask    TaskType = 0
	ReduceTask TaskType = 1
)

type Task struct {
	filename  string
	id        int
	timestamp time.Time
	taskType  TaskType
}

// 正在工作的任务
type WorkingTask struct {
}
type Coordinator struct {
	// Your definitions here.
	mapChannel         chan Task
	reduceChannel      chan Task
	mapWorkingTasks    map[int]Task
	reduceWorkingTasks map[int]Task
	// reduce任务的数量
	nReduce int
	// map任务数量
	nMap int
	// map任务是否完成
	mapDone bool
	mu      sync.Mutex
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

// worker获取task
// 首先分配map任务，如果mapCh为空，则分配reduce任务
func (c *Coordinator) GetMapTask(args *MapReduceArgs, reply *MapReply) error {
	// c.mu.Lock()
	_, ok := c.mapWorkingTasks[args.TaskId]
	if ok {
		log.Printf("coordinator know map task %d finished\n", args.TaskId)
	}
	c.mu.Lock()
	// 删除已经完成的任务
	delete(c.mapWorkingTasks, args.TaskId)
	// 遍历正在工作的任务队列，判断是否超时
	for _, value := range c.mapWorkingTasks {
		t := time.Now()
		// 回收超时的任务
		if t.Sub(value.timestamp).Seconds() > TIMEOUT {
			log.Printf("coordinator get map task back: %d\n", value.id)
			c.mapChannel <- value
		}
	}
	c.mu.Unlock()
	// 给worker分配一个map任务
	if len(c.mapChannel) > 0 {
		task := <-c.mapChannel
		reply.FileName = task.filename
		log.Println("coordinator assigns map task filename: " + task.filename)
		reply.NReduce = c.nReduce
		reply.TaskId = task.id
		reply.State = Ready
		// 更新时间戳并将任务添加到workingTasks中
		task.timestamp = time.Now()
		c.mu.Lock()
		// 添加工作任务
		c.mapWorkingTasks[task.id] = task
		c.mu.Unlock()
	} else if len(c.mapWorkingTasks) > 0 {
		reply.State = Working
	} else { // map任务全部完成，合并中间结果
		c.mu.Lock()
		if c.mapDone == false {
			log.Println("coordinator start merge!")
			// map任务完成之后需要合并中间结果
			MergeFile(c.nMap, c.nReduce)
			// 生成reduce任务队列
			for i := 0; i < c.nReduce; i++ {
				t := Task{"mr-" + strconv.Itoa(i), i, time.Now(), ReduceTask}
				c.reduceChannel <- t
			}
			c.mapDone = true
			log.Println("coordinator: map task finished")
		}
		// 通知worker map任务已经完成
		reply.State = Finished
		c.mu.Unlock()
	}
	// c.mu.Unlock()
	return nil
}

func (c *Coordinator) GetReduceTask(args *MapReduceArgs, reply *ReduceReply) error {
	// c.mu.Lock()
	_, ok := c.reduceWorkingTasks[args.TaskId]
	if ok {
		log.Printf("coordinator know reduce task %d finished\n", args.TaskId)
	}
	c.mu.Lock()
	// 删除已经完成的任务
	delete(c.reduceWorkingTasks, args.TaskId)
	// 遍历正在工作的任务队列，判断是否超时
	for _, value := range c.reduceWorkingTasks {
		t := time.Now()
		// 回收超时的任务
		if t.Sub(value.timestamp).Seconds() > TIMEOUT {
			log.Printf("coordinator get reduce task back: %d\n", value.id)
			c.reduceChannel <- value
		}
	}
	c.mu.Unlock()
	// 设计返回值
	// 给worker分配一个reduce任务
	if len(c.reduceChannel) > 0 {
		task := <-c.reduceChannel
		reply.FileName = task.filename
		// log.Println("reduce task filename: " + task.filename)
		// reply.NReduce = c.nReduce
		reply.TaskId = task.id
		reply.State = Ready
		// 更新时间戳并将任务添加到workingTasks中
		task.timestamp = time.Now()
		c.mu.Lock()
		c.reduceWorkingTasks[task.id] = task
		c.mu.Unlock()
	} else if len(c.mapChannel) == 0 && len(c.reduceWorkingTasks) > 0 {
		reply.State = Working
	} else { // reduce任务全部完成
		reply.State = Finished
	}
	// c.mu.Unlock()
	return nil
}

// 将map任务的结果进行合并
func MergeFile(nMap int, nReduce int) {
	for i := 0; i < nReduce; i++ {
		dstName := "mr-" + strconv.Itoa(i)
		for j := 0; j < nMap; j++ {
			srcName := "mr-" + strconv.Itoa(j) + "-" + strconv.Itoa(i)
			CopyFile(dstName, srcName)
			os.Remove(srcName)
		}
	}
}

func CopyFile(dstName string, srcName string) (written int64, err error) {
	src, err := os.Open(srcName)
	if err != nil {
		return
	}
	defer src.Close()

	// 追加到dst的末尾
	dst, err := os.OpenFile(dstName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return
	}
	defer dst.Close()
	return io.Copy(dst, src)
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
	// 监听
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
	ret := false
	// Your code here.
	if len(c.mapWorkingTasks) == 0 && len(c.mapChannel) == 0 &&
		len(c.reduceChannel) == 0 && len(c.reduceWorkingTasks) == 0 {
		ret = true
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.nReduce = nReduce
	c.nMap = len(files)
	c.mapChannel = make(chan Task, len(files))
	for i, file := range files {
		t := Task{file, i, time.Now(), MapTask}
		c.mapChannel <- t
	}
	c.reduceChannel = make(chan Task, nReduce)
	c.mapWorkingTasks = make(map[int]Task)
	c.reduceWorkingTasks = make(map[int]Task)
	c.mapDone = false

	c.server()
	return &c
}
