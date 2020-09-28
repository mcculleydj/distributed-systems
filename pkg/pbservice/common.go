package pbservice

import "viewservice"

/* 
ErrWrongServer:
Could potentially be used as a trigger
for the client to ask for a new primary.

From the summary:
S1 can then return an error to the client indicating 
that S1 might no longer be the primary 
(reasoning that, since S2 rejected the operation, 
a new view must have been formed);
the client can then ask the view service for the correct 
primary (S2) and send it the operation.
*/

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

type PutAppendArgs struct {
	Id	  int64
	Key   string
	Value string
	Op    string
}

type PutAppendReply struct {
	Err Err
}

type PutAppendToBackupArgs struct {
	Id	  int64
	Key   string
	Value string
	Op    string
	View  viewservice.View
}

type GetArgs struct {
	Id  int64
	Key string
}

type GetFromBackupArgs struct {
	Id    int64
	Key   string
	View  viewservice.View
}

type GetReply struct {
	Err   Err
	Value string
}

type InitializeBackupArgs struct {
	Kvs map[string]string
}

type InitializeBackupReply struct {
	Err Err
}