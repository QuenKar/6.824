package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// global mutex
var mtx sync.Mutex

type taskMetaInfo struct {
	StartTime time.Time
	TaskAddr  *Task
	State     State
}

type taskMetaDict struct {
	Meta map[int]*taskMetaInfo
}

func (t *taskMetaDict) addTaskInfo(taskinfo *taskMetaInfo) bool {
	taskId := taskinfo.TaskAddr.TaskId
	if _, ok := t.Meta[taskId]; !ok {
		// not exist
		t.Meta[taskId] = taskinfo
		return true
	} else {
		fmt.Errorf("already contain this task!")
		return false
	}
}

func (t *taskMetaDict) judgeState(id int) bool {
	print("task judge\n")
	tinfo, ok := t.Meta[id]
	if !ok || tinfo.State != Waitting {
		return false
	}
	print("task working\n")
	tinfo.State = Working
	tinfo.StartTime = time.Now()
	return true
}

type Coordinator struct {
	// Your definitions here.

	ReducerNum  int
	TaskId      int
	GlobalPhase Phase
	files       []string

	TaskMapChan    chan *Task
	TaskReduceChan chan *Task

	taskMetaTable taskMetaDict //保存所有task信息

}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AssignTask(args *TaskArgs, reply *Task) error {
	fmt.Print("Tick Clock\n")
	mtx.Lock()
	defer mtx.Unlock()

	switch c.GlobalPhase {

	case MapPhase:
		{
			if len(c.TaskMapChan) > 0 {
				*reply = *<-c.TaskMapChan
				fmt.Println("assign a map task")
				if !c.taskMetaTable.judgeState(reply.TaskId) {
					fmt.Printf("map task[%d] is running!", reply.TaskId)
				}
			} else {
				//wait for task done
				reply.TaskType = WaittingTask
				//check Map Tasks all done
				if c.taskMetaTable.isMapTasksAllDone() {
					fmt.Printf("Map phase over\n")
					c.toNextPhase()
				}
				return nil
			}
		}
	case ReducePhase:
		{
			if len(c.TaskReduceChan) > 0 {
				*reply = *<-c.TaskReduceChan
				fmt.Println("assign a reduce task")
				if !c.taskMetaTable.judgeState(reply.TaskId) {
					fmt.Printf("reduce task[%d] is running!", reply.TaskId)
				}
			} else {
				//wait for task done
				reply.TaskType = WaittingTask
				//check Map Tasks all done
				if c.taskMetaTable.isReduceTasksAllDone() {
					fmt.Printf("Reduce phase over\n")
					c.toNextPhase()
				}
				return nil
			}
		}
	case AllDone:
		reply.TaskType = ExitTask
	default:
		panic("undefined phase")
	}

	return nil
}

func (t *taskMetaDict) isMapTasksAllDone() bool {

	var (
		MapDoneNum    = 0
		MapNotDoneNum = 0
	)

	for _, info := range t.Meta {
		if info.TaskAddr.TaskType == MapTask {
			if info.State == Done {
				MapDoneNum++
			} else {
				MapNotDoneNum++
			}
		}
	}

	if MapDoneNum > 0 && MapNotDoneNum == 0 {
		return true
	}

	return false
}

func (t *taskMetaDict) isReduceTasksAllDone() bool {

	var (
		ReduceDoneNum    = 0
		ReduceNotDoneNum = 0
	)

	for _, info := range t.Meta {
		if info.TaskAddr.TaskType == ReduceTask {
			if info.State == Done {
				ReduceDoneNum++
			} else {
				ReduceNotDoneNum++
			}
		}
	}
	//这个判断ReduceDoneNum要改进一下？
	if ReduceDoneNum > 0 && ReduceNotDoneNum == 0 {
		return true
	}

	return false
}

func (c *Coordinator) toNextPhase() {
	if c.GlobalPhase == MapPhase {
		fmt.Print("go to Reduce Phase\n")
		//prepare for reduce tasks
		c.makeReduceTasks()
		c.GlobalPhase = ReducePhase
	} else if c.GlobalPhase == ReducePhase {
		c.GlobalPhase = AllDone
	}
}

func (c *Coordinator) MarkTaskFinished(args *Task, reply *Task) error {
	mtx.Lock()
	defer mtx.Unlock()

	switch args.TaskType {
	case MapTask:
		{
			taskinfo, ok := c.taskMetaTable.Meta[args.TaskId]
			if ok {
				if taskinfo.State == Working {
					taskinfo.State = Done
					fmt.Printf("Map task:%d has done, mark finished.\n", args.TaskId)
				} else {
					fmt.Printf("Map task:%d mark finished already.\n", args.TaskId)
				}
			}
		}
	case ReduceTask:
		{
			taskinfo, ok := c.taskMetaTable.Meta[args.TaskId]
			if ok {
				if taskinfo.State == Working {
					taskinfo.State = Done
					fmt.Printf("Reduce task:%d has done, mark finished.\n", args.TaskId)
				} else {
					fmt.Printf("Reduce task:%d mark finished already.\n", args.TaskId)
				}
			}
		}
	default:
		return fmt.Errorf("task type error")
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
	mtx.Lock()
	defer mtx.Unlock()
	// Your code here.
	if c.GlobalPhase == AllDone {
		fmt.Printf("All tasks has been done, Phase done\n")
		return true
	} else {
		return false
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:          files,
		ReducerNum:     nReduce,
		TaskId:         0,
		GlobalPhase:    MapPhase,
		TaskMapChan:    make(chan *Task, len(files)),
		TaskReduceChan: make(chan *Task, nReduce),
		taskMetaTable: taskMetaDict{
			Meta: make(map[int]*taskMetaInfo, len(files)+nReduce),
		},
	}

	// Your code here.
	c.makeMapTasks(files)

	c.server()

	go c.CrashChecker()

	return &c
}

func (c *Coordinator) CrashChecker() {
	for {
		time.Sleep(time.Second)

		mtx.Lock()
		if c.GlobalPhase == AllDone {
			mtx.Unlock()
			break
		}

		for _, tinfo := range c.taskMetaTable.Meta {
			if tinfo.State == Working && time.Since(tinfo.StartTime) > 9*time.Second {
				fmt.Printf("task[%v] crash! take %v second\n", tinfo.TaskAddr.TaskId, time.Since(tinfo.StartTime))

				if tinfo.TaskAddr.TaskType == MapTask {
					c.TaskMapChan <- tinfo.TaskAddr
					tinfo.State = Waitting
				} else if tinfo.TaskAddr.TaskType == ReduceTask {
					c.TaskReduceChan <- tinfo.TaskAddr
					tinfo.State = Waitting
				}
			}
		}

		mtx.Unlock()
	}
}

func (c *Coordinator) makeMapTasks(files []string) {
	for _, filename := range files {
		id := c.generateTaskId()
		//make map Task
		task := Task{
			TaskType:  MapTask,
			TaskId:    id,
			ReduceNum: c.ReducerNum,
			FileSlice: []string{filename},
		}

		taskinfo := taskMetaInfo{
			TaskAddr: &task,
			State:    Waitting,
		}

		// record taskinfo
		c.taskMetaTable.addTaskInfo(&taskinfo)

		//debug
		fmt.Printf("make a map task, TaskId : %v\n", task.TaskId)

		c.TaskMapChan <- &task
	}
}

func (c *Coordinator) makeReduceTasks() {

	for i := 0; i < c.ReducerNum; i++ {
		id := c.generateTaskId()
		//make reduce Task
		task := Task{
			TaskType:  ReduceTask,
			TaskId:    id,
			ReduceNum: c.ReducerNum,
			FileSlice: getTmpFiles(i),
		}

		taskinfo := taskMetaInfo{
			TaskAddr: &task,
			State:    Waitting,
		}

		// record taskinfo
		c.taskMetaTable.addTaskInfo(&taskinfo)

		//debug
		fmt.Printf("make a reduce task, TaskId : %v\n", task.TaskId)

		c.TaskReduceChan <- &task

	}
}

func getTmpFiles(idx int) []string {
	res := []string{}

	path, _ := os.Getwd()
	fmt.Println("tmp files is in: " + path)
	fs, _ := ioutil.ReadDir(path)

	for _, f := range fs {
		if strings.HasPrefix(f.Name(), "mr-tmp") && strings.HasSuffix(f.Name(), strconv.Itoa(idx)) {
			res = append(res, f.Name())
		}
	}
	return res
}

func (c *Coordinator) generateTaskId() int {
	c.TaskId++
	return c.TaskId
}
