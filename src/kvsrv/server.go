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
	mu sync.Mutex

	// Your definitions here.
	data       map[string]string
	requestIDs sync.Map
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.data[args.Key]
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

	kv.mu.Lock()
	old := kv.data[args.Key]
	if old != args.Value {
		kv.data[args.Key] = args.Value
	}
	kv.mu.Unlock()

	reply.Value = old

	kv.requestIDs.Store(args.RequestID, old)
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

	kv.mu.Lock()
	old := kv.data[args.Key]
	if old == "" {
		kv.data[args.Key] = args.Value
	} else {
		kv.data[args.Key] = old + args.Value
	}
	kv.mu.Unlock()

	reply.Value = old

	kv.requestIDs.Store(args.RequestID, old)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.data = make(map[string]string, 20)

	return kv
}
