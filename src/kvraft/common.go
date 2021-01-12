package kvraft

import "time"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeOut"

	ClerkRequestTimeout  = 400 * time.Millisecond
	WaitTimeout        = 400 * time.Millisecond
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	RequestSeq   int64
	ClientId  int64
}

type PutAppendReply struct {
	Err Err
	IsLeader  bool
}

type GetArgs struct {
	Key string
	RequestSeq   int64
	ClientId     int64

}

type GetReply struct {
	Err   Err
	Value string
	IsLeader  bool
}
