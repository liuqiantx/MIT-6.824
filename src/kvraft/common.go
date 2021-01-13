package kvraft

import "time"

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeOut"

	ClerkRequestTimeout  = 400 * time.Millisecond
	WaitTimeout        = 400 * time.Millisecond
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Op    string
	Key   string
	Value string
	MsgId   int64
	ClerkId  int64
}

type PutAppendReply struct {
	Err Err
	IsLeader  bool
}

type GetArgs struct {
	Key string
	MsgId   int64
	ClerkId     int64

}

type GetReply struct {
	Err   Err
	Value string
	IsLeader  bool
}

type NotifyMsg struct {
	Err Err
	Value string
}
