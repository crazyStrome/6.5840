package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	// Your definitions here.
	data       sync.Map
	requestIDs sync.Map
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	v, ok := kv.data.Load(args.Key)
	if !ok {
		return
	}
	reply.Value = v.(string)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if args.Mode == Mode_Report {
		kv.requestIDs.Delete(args.RequestID)
		return
	}

	v, ok := kv.requestIDs.Load(args.RequestID)
	if ok {
		reply.Value = v.(string)
		return
	}

	v, ok = kv.data.Load(args.Key)
	if ok {
		reply.Value = v.(string)
	}
	if reply.Value != args.Value {
		kv.data.Store(args.Key, args.Value)
	}

	kv.requestIDs.Store(args.RequestID, reply.Value)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if args.Mode == Mode_Report {
		kv.requestIDs.Delete(args.RequestID)
		return
	}

	v, ok := kv.requestIDs.Load(args.RequestID)
	if ok {
		reply.Value = v.(string)
		return
	}

	v, ok = kv.data.Load(args.Key)
	if ok {
		reply.Value = v.(string)
		kv.data.Store(args.Key, v.(string)+args.Value)
	} else {
		kv.data.Store(args.Key, args.Value)
	}

	kv.requestIDs.Store(args.RequestID, reply.Value)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.

	return kv
}
