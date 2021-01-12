package kvraft

import (
	"../labrpc"
	"math/big"
	"crypto/rand"
	"fmt"
	"sync"
)


// clients 给随机 server 发送请求，当该 server 不是 leader,server 会将 client 请求重定向给它所知道的 leader
// 若 leader 掉线了，client 请求会超时，然后重新开始随机请求.

// client 请求失败的表现：1. 请求超时。2. 请求返回错误。
// 什么导致请求失败：1. server 挂了，或请求数据丢失。2. server 联系上了，但请求数据丢失。
// 虽然两者表现相同，但第二种会造成数据错误，因为 server 重复操作了.如何避免?
// 对于每个 request，给予独有 request id 以及 client id,状态机核查该 id 即可.

// Clerk: 给 leader 发送请求，返回结果，若失败，则重新随机发.
type Clerk struct {
	servers []*labrpc.ClientEnd
	mu      sync.Mutex
	clientId  int64
	leaderId  int
	requestSeq  int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = nrand()
	ck.leaderId = 0
	ck.requestSeq = 0
	fmt.Printf("Clerk %v initializing ...", ck.clientId)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// args := &GetArgs{}
	// You will have to modify this function.
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

// clerk 注意 server 返回的索引上是否存在另一个请求/raft schema 已经更改，此时，clerk 将发送请求到其他服务器
// 直到找到新的 leader