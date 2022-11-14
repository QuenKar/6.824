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

type TaskType int

type Phase int

type State int

const (
	MapTask TaskType = iota
	ReduceTask
	WaittingTask
	ExitTask
)

const (
	MapPhase Phase = iota
	ReducePhase
	AllDone
)

const (
	Working State = iota
	Waitting
	Done
)

type Task struct {
	TaskType  TaskType
	TaskId    int
	ReduceNum int
	FileSlice []string
}

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

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	flag := true
	for flag {
		task := GetTask()

		switch task.TaskType {
		case MapTask:
			{
				DoMapTask(mapf, &task)
				CallTaskDone(&task)
			}
		case ReduceTask:
			{
				DoReduceTask(reducef, &task)
				CallTaskDone(&task)
			}
		case WaittingTask:
			{
				time.Sleep(5 * time.Second)
			}
		case ExitTask:
			{
				time.Sleep(time.Second)
				fmt.Println("all task done, the worker will exit...")
				flag = false
			}
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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

func GetTask() Task {

	args := TaskArgs{}
	reply := Task{}

	ok := call("Coordinator.AssignTask", &args, &reply)

	if !ok {
		fmt.Printf("get task error\n")
		return Task{}
	}

	return reply
}

func CallTaskDone(t *Task) error {

	args := t
	reply := Task{}

	ok := call("Coordinator.MarkTaskFinished", &args, &reply)
	if !ok {
		return fmt.Errorf("mark task finished error")
	}

	return nil
}

func DoMapTask(mapf func(string, string) []KeyValue, resp *Task) {
	//
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//

	intermediate := []KeyValue{}

	file, err := os.Open(resp.FileSlice[0])
	if err != nil {
		log.Fatalf("cannot open %v", resp.FileSlice[0])
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", resp.FileSlice[0])
	}
	file.Close()
	kva := mapf(resp.FileSlice[0], string(content))
	intermediate = append(intermediate, kva...)

	//
	// a big difference from real MapReduce is that all the
	// intermediate data is in one place, intermediate[],
	// rather than being partitioned into NxM buckets.
	//

	// partition into NxM buckets.
	rn := resp.ReduceNum

	HashKV := make([][]KeyValue, rn)

	for _, kv := range intermediate {
		idx := ihash(kv.Key) % rn
		HashKV[idx] = append(HashKV[idx], kv)
	}

	for i := 0; i < rn; i++ {
		oname := "mr-tmp-" + strconv.Itoa(resp.TaskId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range HashKV[i] {
			enc.Encode(kv)
		}
		ofile.Close()
	}

}

func DoReduceTask(reducef func(string, []string) string, resp *Task) {

	reduceId := resp.TaskId

	intermediate := shuffle(resp.FileSlice)

	dir, _ := os.Getwd()
	tmpf, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		fmt.Errorf("fail to create temp file")
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmpf, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	tmpf.Close()

	fname := "mr-out-" + strconv.Itoa(reduceId)

	os.Rename(tmpf.Name(), fname)
}

func shuffle(files []string) []KeyValue {
	kvs := ByKey{}

	for _, fn := range files {
		f, _ := os.Open(fn)

		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
	}

	sort.Sort(kvs)

	return kvs
}
