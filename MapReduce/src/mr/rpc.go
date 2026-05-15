package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type AssignTaskArgs struct{}

type AssignTaskReply struct {
	TaskType string
	Phase    Phase
	TaskId   int
	Filename string
	NReduce  int
	NMap     int
}

type ReportTaskArgs struct {
	Phase  Phase
	TaskId int
}

type ReportTaskReply struct{}
