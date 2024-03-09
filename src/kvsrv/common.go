package kvsrv

type Mode int64

const (
	Mode_Modify Mode = 1
	Mode_Report Mode = 2
)

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	RequestID uint32 // 请求 id，去重用
	Mode      Mode   // 模式，是更新数据还是报告已经收到响应
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Value string
}
