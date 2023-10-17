package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "os"
import "encoding/json"
import "io"
import "strconv"
import "sort"

// Map functions return a slice of KeyValue.
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		reply := getTaskCall()
		// 若无可分配任务，则等待
		if reply.Wait {
			time.Sleep(time.Second)
		} else {
			// 判断任务类型
			if reply.IsMapTask {
				// 创建reduce任务编号与输出文件的映射
				fileMapping := make(map[int]*os.File, reply.NReduce)
				// 创建reduce任务编号与json编码器的映射
				encoderMapping := make(map[int]*json.Encoder, reply.NReduce)
				// 创建nReduce个临时文件并加入至映射中
				for i := 0; i < reply.NReduce; i++ {
					//fileMapping[i], _ = os.Create("mr-" + strconv.Itoa(reply.TaskNum) + "-" + strconv.Itoa(i))
					fileMapping[i], _ = os.CreateTemp("./", "mapTmp")
					encoderMapping[i] = json.NewEncoder(fileMapping[i])
				}

				intermediate := []KeyValue{}
				// 从输入文件中读取内容
				file, err := os.Open(reply.FileName)
				if err != nil {
					log.Fatalf("cannot open %v", reply.FileName)
				}
				content, err := io.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", reply.FileName)
				}
				file.Close()
				// 对文件的content调用map
				kva := mapf(reply.FileName, string(content))
				// 中间值打散存储至kv数组intermediate中
				intermediate = append(intermediate, kva...)

				// 以json格式写入至临时文件中
				for _, kv := range intermediate {
					encoderMapping[ihash(kv.Key)].Encode(&kv)
				}
				for i, tmpFile := range fileMapping {
					// 修改临时文件名为正式文件名，关闭file资源
					os.Rename(tmpFile.Name(), "mr-"+strconv.Itoa(reply.TaskNum)+"-"+strconv.Itoa(i))
					tmpFile.Close()
				}

				// 发送任务完成给Coordinator
				finishTaskCall(reply.TaskNum, true)
			} else {
				// 任务为reduce
				intermediate := []KeyValue{}

				// 读取M个json文件中的中间值
				for i := 0; i < reply.MMap; i++ {
					// 为第i个文件创建json解码器
					file, err := os.Open("mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.TaskNum))
					if err != nil {
						log.Fatalf("cannot open %v", reply.FileName)
					}
					dec := json.NewDecoder(file)

					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						intermediate = append(intermediate, kv)
					}

					file.Close()
				}

				// 对读入的中间值进行排序
				sort.Sort(ByKey(intermediate))

				// 创建输出文件
				oname := "mr-out-" + strconv.Itoa(reply.TaskNum)
				ofile, _ := os.Create(oname)

				// call Reduce on each distinct key in intermediate[],
				// and print the result to output file.
				i := 0
				for i < len(intermediate) {
					j := i + 1
					// 找到与当前key不同的下一个key
					for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
						j++
					}
					values := []string{}
					// 把当前key的所有value值保存在values数组中
					for k := i; k < j; k++ {
						values = append(values, intermediate[k].Value)
					}
					// 对values调用reduce
					output := reducef(intermediate[i].Key, values)

					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

					// 设置为下一个key
					i = j
				}
				ofile.Close()

				// 发送任务完成给Coordinator
				finishTaskCall(reply.TaskNum, false)
			}
		}
	}
}

// 向远程Coordinate发送RPC请求，获取任务
func getTaskCall() GetTaskReply {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	call("Coordinator.HandleGetTaskRequest", &args, &reply)
	return reply
}

// 向远程Coordinate发送请求，通知任务已完成
func finishTaskCall(taskNum int, isMapTask bool) {
	args := FinishTaskArgs{}
	args.taskNum = taskNum
	args.IsMapTask = isMapTask
	reply := FinishTaskReply{}
	call("Coordinator.HandleFinishTaskRequest", &args, &reply)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
