package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	// 不断向coordinator请求map任务并执行map任务
	CallMapTask(mapf)
	log.Printf("worker pid: %d, call reduce task", os.Getpid())
	CallReduceTask(reducef)
}

func CallMapTask(mapf func(string, string) []KeyValue) {
	// declare an argument structure.
	args := MapReduceArgs{-1}
	// declare a reply structure.
	//reply := MapReply{}
	var reply MapReply
	for {
		reply = MapReply{}
		// 获取任务
		call("Coordinator.GetMapTask", &args, &reply)
		// 所有map任务已经完成，跳出循环
		if reply.State == Finished {
			log.Printf("worker pid: %d, map task finished!\n", os.Getpid())
			break
		}
		if reply.State == Working {
			log.Printf("worker pid: %d, map task working!\n", os.Getpid())
			args.TaskId = -1
			time.Sleep(time.Second)
			continue
		}
		log.Printf("worker pid: %d, get map task: %d, state: %d, filename: %s\n", os.Getpid(), reply.TaskId, reply.State, reply.FileName)
		// 执行map任务
		filename := reply.FileName
		// log.Println("file name: " + filename)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		// 执行map任务
		kva := mapf(filename, string(content))
		nReduce := reply.NReduce
		// 使用随机数生成随机的临时文件名，需要保证worker之间的种子不一样，否则文件名会重复
		// rand.Seed(time.Now().Unix())
		// 创建nReduce个文件用于写结果
		encoders := make([]*json.Encoder, nReduce)
		files := make([]string, nReduce)
		openfiles := make([]*os.File, nReduce)
		for i := 0; i < nReduce; i++ {
			// 随机生成一个临时文件
			openfiles[i], err = ioutil.TempFile("./", "*")
			files[i] = openfiles[i].Name()
			// files[i] = "mr" + strconv.Itoa(i) + strconv.Itoa(rand.Int())
			// openfiles[i], err = os.OpenFile(files[i], os.O_CREATE|os.O_WRONLY, 0666)
			if err != nil {
				log.Fatalf("cannot open %v", files[i])
			}
			encoders[i] = json.NewEncoder(openfiles[i])
		}
		if len(kva) == 0 {
			log.Printf("result is empty\n")
		}
		for _, kv := range kva {
			id := ihash(kv.Key) % nReduce
			// 写入文件
			err := encoders[id].Encode(&kv)
			if err != nil {
				log.Println("Error in encoding json")
			}
		}
		// 关闭文件
		for _, f := range openfiles {
			f.Close()
		}
		// @TODO 是否需要coordinator知道已经完成该任务再将其写入文件中？
		// 通知coordinator已经完成map任务，如果coordinator接收到了则对结果进行重命名
		mapId := reply.TaskId
		// 对临时文件重命名
		for i := 0; i < nReduce; i++ {
			name := "mr-" + strconv.Itoa(mapId) + "-" + strconv.Itoa(i)
			os.Rename(files[i], name)
		}
		log.Printf("worker pid: %d,  finish map task: %d, filename: %v\n", os.Getpid(), reply.TaskId, "mr-"+strconv.Itoa(mapId)+"-"+strconv.Itoa(0))
		// 已经完成的任务id
		args.TaskId = reply.TaskId
	}

}

// reduce任务
func CallReduceTask(reducef func(string, []string) string) {
	// declare an argument structure.
	args := MapReduceArgs{-1}
	// declare a reply structure.
	var reply ReduceReply
	for {
		reply = ReduceReply{}
		// 获取任务
		call("Coordinator.GetReduceTask", &args, &reply)
		// 所有reduce任务已经完成，跳出循环
		if reply.State == Finished {
			log.Printf("worker pid: %d, reduce task finished!", os.Getpid())
			break
		}
		if reply.State == Working {
			log.Printf("worker pid: %d, reduce task working!", os.Getpid())
			time.Sleep(time.Second)
			continue
		}
		// 执行reduce任务
		filename := reply.FileName
		log.Printf("worker pid: %d, get reduce task: %d, state: %d, filename: %s\n", os.Getpid(), reply.TaskId, reply.State, reply.FileName)
		// 读取中间结果
		openfile, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(openfile)
		kva := []KeyValue{}
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}

		intermediate := kva
		sort.Sort(ByKey(intermediate))
		oname := "mr-out-" + strconv.Itoa(reply.TaskId)
		ofile, _ := os.Create(oname)

		//
		// call Reduce on each distinct key in intermediate[],
		// and print the result to mr-out-0.
		//
		i := 0
		for i < len(intermediate) {
			j := i + 1
			// 遍历key相同的结果
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			output := reducef(intermediate[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

			i = j
		}
		openfile.Close()
		ofile.Close()
		os.Remove(filename)
		// 已经完成的任务id
		args.TaskId = reply.TaskId
		log.Printf("worker pid: %d,  finish reduce task: %d, filename: %v\n", os.Getpid(), reply.TaskId, oname)
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
