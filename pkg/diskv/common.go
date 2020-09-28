package diskv

import "shardmaster"

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrConfigSync = "ErrConfigSync"
	ErrNotInCache = "ErrNotInCache"
	NShards       = shardmaster.NShards
)

type Err string

type GetArgs struct {
	Xid       int64
	Key       string
	ConfigNum int
}

type GetReply struct {
	Value string
	Err   Err
}

type PutAppendArgs struct {
	Xid       int64
	Key       string
	Value     string
	Op        string
	ConfigNum int
}

type PutAppendReply struct {
	Err Err
}

type ShardId struct {
	ConfigNum int
	Shard     int
}

type ReceiveShardArgs struct {
	Sid    ShardId
	Data   map[string]string
	Reads  map[int64]GetReply
	Writes map[int64]PutAppendReply
}

type ReceiveShardReply struct {
	Err Err
}

type RequestLogArgs struct {
	Xid int64
	Me  int
}

type RequestLogReply struct {
	Ckpt Ckpt
	Log  map[int]Op
	Err  Err
}

// EOF