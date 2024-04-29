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
	Index   int64
	Term    int64
	Command interface{}
}

func (e Entry) clone() Entry {
	return Entry{
		Index:   e.Index,
		Term:    e.Term,
		Command: e.Command,
	}
}

type PersistState struct {
	CurrentTerm int64
	VoteInfo    struct {
		VotedFor  int64
		VotedTerm int64
	}
	Log                   []Entry
	SnapLastIncludedTerm  int64
	SnapLastIncludedIndex int64
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

	snapshot             []byte           // 快照
	snapLastIncludeIndex int64            // 快照包含的最后一条日志索引
	snapLastIncludeTerm  int64            // 快照包含的最后一条日志的任期
	snapshotCache        map[int64][]byte // cache

	// volatile state on all servers
	commitIndex int64 // 已知被提交的最高条目索引
	lastApplied int64 // 已经应用到状态机上的最高的条目索引

	currentState int32 // 当前的状态，follower：0，candidate：1，leader：2

	lastElectionTime int64 // 上一次选举超时的时刻，毫秒
	electionTimeout  int64 // 本次设置的选举超时，毫秒

	leaderID int // -1 表示没有 leaderID

	// volatile state on leaders
	nextIndex  []int64 // 每个 server 一个 index，标识要发给他们的下一个条目的索引。一开始是 leader 的最后一个日志条目索引 + 1
	matchIndex []int64 // 每个 server 一个，已复制到 server 上的最高索引，初始时是 0

	applyCh chan ApplyMsg // 应用日志的 channel
}

func (rf *Raft) getSnapLastIncludeIndex() int64 {
	return atomic.LoadInt64(&rf.snapLastIncludeIndex)
}

func (rf *Raft) getSnapLastIncludeTerm() int64 {
	return atomic.LoadInt64(&rf.snapLastIncludeTerm)
}

func (rf *Raft) setSnapLastIncludeIndex(idx int64) {
	atomic.StoreInt64(&rf.snapLastIncludeIndex, idx)
}

func (rf *Raft) setSnapLastIncludeTerm(term int64) {
	atomic.StoreInt64(&rf.snapLastIncludeTerm, term)
}

func (rf *Raft) getNextIndex(idx int) int64 {
	return atomic.LoadInt64(&rf.nextIndex[idx])
}

func (rf *Raft) getMatchIndex(idx int) int64 {
	return atomic.LoadInt64(&rf.matchIndex[idx])
}

func (rf *Raft) setNextIndex(idx int, next int64) {
	atomic.StoreInt64(&rf.nextIndex[idx], next)
}

func (rf *Raft) setMatchIndex(idx int, match int64) {
	atomic.StoreInt64(&rf.matchIndex[idx], match)
}

func (rf *Raft) Logf(line string, args ...interface{}) {
	DPrintf("[Raft:%v, term:%v] "+line, append([]interface{}{rf.me, rf.getCurrentTerm()}, args...)...)
}

func (rf *Raft) cloneLog() []Entry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	entries := make([]Entry, 0, len(rf.log))
	for _, entry := range rf.log {
		entries = append(entries, entry.clone())
	}
	return entries
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

// turnFollower 转为新 term 的 follower
func (rf *Raft) turnFollower(term int64) {
	atomic.StoreInt32(&rf.currentState, StateFollower)
	rf.resetElectionTimer()
	atomic.StoreInt64(&rf.currentTerm, term)
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

	// 初始化 nextIndex 和 matchIndex
	lastIdx, _ := rf.getLastLogIndexAndTerm()
	rf.nextIndex = make([]int64, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = lastIdx + 1
	}
	rf.matchIndex = make([]int64, len(rf.peers))
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
