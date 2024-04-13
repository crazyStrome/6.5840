package raft

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Index int
	Term  int64
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers
	currentTerm int64 // 遇到的最后一个 term
	voteInfo    struct {
		votedFor  int64 // 投票给的人，-1 表示未投票
		votedTerm int64 // 投票给 votedFor 的时候，votedFor 的任期
	}
	log []Entry // 日志条目

	// volatile state on all servers
	commitIndex int64 // 已知被提交的最高条目索引
	lastApplied int64 // 已经应用到状态机上的最高的条目索引

	currentState int32 // 当前的状态，follower：0，candidate：1，leader：2

	lastElectionTime int64 // 上一次选举超时的时刻，毫秒
	electionTimeout  int64 // 本次设置的选举超时，毫秒

	leaderID int // -1 表示没有 leaderID

	// volatile state on leaders
	nextIndex  []int // 每个 server 一个 index，标识要发给他们的下一个条目的索引。一开始是 leader 的最后一个日志条目索引 + 1
	matchIndex []int // 每个 server 一个，已复制到 server 上的最高索引，初始时是 0
}

func (rf *Raft) Logf(line string, args ...interface{}) {
	DPrintf("[Raft:%v, term:%v] "+line, append([]interface{}{rf.me, rf.getCurrentTerm()}, args...)...)
}

func (rf *Raft) getCurrentTerm() int64 {
	return atomic.LoadInt64(&rf.currentTerm)
}

func (rf *Raft) setTerm(term int64) {
	atomic.StoreInt64(&rf.currentTerm, term)
}

func (rf *Raft) getCurrentState() int32 {
	return atomic.LoadInt32(&rf.currentState)
}

// turnFollower 转为 follower
func (rf *Raft) turnFollower() {
	atomic.StoreInt32(&rf.currentState, StateFollower)
	rf.resetElectionTimer()
}

func (rf *Raft) turnCandidator() {
	if rf.getCurrentState() == StateCandidator {
		return
	}
	atomic.StoreInt32(&rf.currentState, StateCandidator)
}

func (rf *Raft) turnLeader() {
	if rf.getCurrentState() == StateLeader {
		return
	}
	atomic.StoreInt32(&rf.currentState, StateLeader)
}

func (rf *Raft) resetElectionTimer() {
	atomic.StoreInt64(&rf.lastElectionTime, time.Now().UnixMilli())
	atomic.StoreInt64(&rf.electionTimeout, getElectionTimeout())
}

const (
	StateFollower   = 0
	StateCandidator = 1
	StateLeader     = 2
)
