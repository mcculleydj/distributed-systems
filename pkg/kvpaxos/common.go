package kvpaxos

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Xid    int64
	Key    string
	Value  string
	Op     string   // "Put" or "Append"
	AckXid []int64
}

type PutAppendReply struct {
	Seq int
	Xid int64
	Err Err
}

type GetArgs struct {
	Xid    int64
	Key    string
	AckXid []int64
}

type GetReply struct {
	Seq   int
	Xid   int64
	Err   Err
	Value string
}
