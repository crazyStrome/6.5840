package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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

	leaderID int

	// volatile state on leaders
	nextIndex  []int // 每个 server 一个 index，标识要发给他们的下一个条目的索引。一开始是 leader 的最后一个日志条目索引 + 1
	matchIndex []int // 每个 server 一个，已复制到 server 上的最高索引，初始时是 0
}

func (rf *Raft) getCurrentTerm() int64 {
	return atomic.LoadInt64(&rf.currentTerm)
}

func (rf *Raft) incrTerm() {
	atomic.AddInt64(&rf.currentTerm, 1)
}

func (rf *Raft) setTerm(term int64) {
	atomic.StoreInt64(&rf.currentTerm, term)
}

func (rf *Raft) getCurrentState() int32 {
	return atomic.LoadInt32(&rf.currentState)
}

// turnFollower 转为 follower
func (rf *Raft) turnFollower() {
	if atomic.LoadInt32(&rf.currentState) == StateFollower {
		return
	}
	atomic.StoreInt32(&rf.currentState, StateFollower)
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

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = int(rf.getCurrentTerm())
	isleader = rf.getCurrentState() == StateLeader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int64 // candidate 的任期
	CandidateID  int   // candidate 的编号
	LastLogIndex int   // 最后一个日志的索引
	LastLogTerm  int64 // 最后一条日志的任期
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int64 // server 看到的任期
	VoteGranted bool  // 是否投票
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	curTerm := rf.getCurrentTerm()
	reply.Term = curTerm
	if curTerm > args.Term {
		DPrintf("[RequestVote] Raft:%v see low term:%v of:%v\n",
			rf.me, args.Term, args.CandidateID)
		return
	}
	votedFor, votedTerm := rf.getVoteInfo()
	if votedTerm == args.Term && votedFor != -1 && votedFor != int64(args.CandidateID) {
		DPrintf("[RequestVote] Raft:%v has vote for:%v\n", rf.me, votedFor)
		return
	}
	lastIdx, lastTerm := rf.getLastLogIndexAndTerm()
	if lastTerm > args.LastLogTerm {
		DPrintf("[RequestVote] Raft:%v LastLogTerm:%v bigger than Raft:%v:%v\n",
			rf.me, lastTerm, args.CandidateID, args.LastLogTerm)
		return
	}
	if lastTerm < args.LastLogTerm {
		DPrintf("[RequestVote] Raft:%v LastLogTerm:%v lower than Raft:%v:%v\n",
			rf.me, lastTerm, args.CandidateID, args.LastLogTerm)
		reply.VoteGranted = true
		return
	}
	if lastIdx > args.LastLogIndex {
		DPrintf("[RequestVote] Raft:%v LastLogIndex:%v bigger than Raft:%v:%v\n",
			rf.me, lastIdx, args.CandidateID, args.LastLogIndex)
		return
	}
	DPrintf("[RequestVote] Raft:%v LastLogIndex:%v and term:%v lower than Raft:%v:%v term:%v, voted for Raft:%v\n",
		rf.me, lastIdx, lastTerm, args.CandidateID, args.LastLogIndex,
		args.LastLogTerm, args.CandidateID)
	reply.VoteGranted = true

	rf.setTerm(args.Term)
	rf.setVoteInfo(args.CandidateID, args.Term)
}

func (rf *Raft) getLastLogIndexAndTerm() (int, int64) {
	if len(rf.log) == 0 {
		return 0, 0
	}
	entry := rf.log[len(rf.log)-1]
	return entry.Index, entry.Term
}

type AppendEntriesArgs struct {
	Term         int64   // leader 的任期
	LeaderID     int     // leader 的编号
	PrevLogIndex int     // 即将处理的记录的前一个索引
	PrevLogTerm  int64   // prevLogIndex 对应记录的任期
	Entries      []Entry // 日志记录，为空则为心跳消息
	LeaderCommit int64   // leader 提交的索引
}

type AppendEntriesReply struct {
	Term    int64 // 下游的任期
	Success bool  // 如果 follower 包含符合 prevLogIndex 和 prevLogTerm 的日志
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = atomic.LoadInt64(&rf.currentTerm)
	if reply.Term > args.Term {
		DPrintf("[AppendEntries] Raft:%v see lower term:%v of:%v\n",
			rf.me, args.Term, args.LeaderID)
		return
	}
	if !rf.matchLog(args.PrevLogIndex, args.PrevLogTerm) {
		DPrintf("[AppendEntries] Raft:%v's log don't match with:%v\n",
			rf.me, args.LeaderID)
		return
	}
	if len(args.Entries) == 0 {
		// 表明是心跳消息
		rf.turnFollower()
		rf.leaderID = args.LeaderID
		rf.setTerm(args.Term)
		rf.resetElectionTimer()
		reply.Success = true
		return
	}
	// 更新 commitIndex
}

// matchLog 判断是否匹配
func (rf *Raft) matchLog(prevIdx int, prevTerm int64) bool {
	if prevIdx == 0 {
		return true
	}
	if prevIdx >= len(rf.log) {
		return false
	}
	return rf.log[prevIdx].Term == prevTerm
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		if rf.getCurrentState() != StateLeader &&
			rf.isElectionTimeout() {
			rf.election()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) isElectionTimeout() bool {
	return time.Now().UnixMilli()-atomic.LoadInt64(&rf.lastElectionTime) >
		atomic.LoadInt64(&rf.electionTimeout)
}

// election 选举
func (rf *Raft) election() {
	// 转为 candidator
	rf.turnCandidator()

	// 增加任期
	rf.incrTerm()

	DPrintf("[election] Raft:%v start election, currentTerm:%v\n",
		rf.me, rf.getCurrentTerm())

	// 给自己投票
	rf.setVoteInfo(rf.me, rf.getCurrentTerm())
	DPrintf("[election] Raft:%v vote for self, term:%v\n",
		rf.me, rf.getCurrentTerm())

	// 重设选举计时器
	rf.resetElectionTimer()

	var cnt int64 = 1

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		idx := i
		go func() {
			req := &RequestVoteArgs{
				Term:        rf.getCurrentTerm(),
				CandidateID: rf.me,
			}
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(idx, req, reply)
			DPrintf("[election] Raft:%v request vote for:%v, req:%+v, rsp:%+v\n",
				rf.me, idx, req, reply)
			if !ok {
				DPrintf("[election] Raft:%v of term:%v request vote of:%v net fail\n",
					rf.me, rf.currentTerm, idx)
				return
			}
			if reply.Term > rf.getCurrentTerm() {
				// 遇到更高的任期，就转为 follower
				DPrintf("[election] Raft:%v of term:%v request vote of:%v term:%v, turn to follower\n",
					rf.me, rf.getCurrentTerm(), idx, reply.Term)
				rf.turnFollower()
				rf.setTerm(reply.Term)
				rf.resetElectionTimer()
				return
			}
			if reply.VoteGranted {
				DPrintf("[election] Raft:%v of term:%v granted vote of:%v, term:%v\n",
					rf.me, rf.getCurrentTerm(), idx, reply.Term)
				atomic.AddInt64(&cnt, 1)
			} else {
				DPrintf("[election] Raft:%v of term:%v didn't granted vote of:%v, term:%v\n",
					rf.me, rf.getCurrentTerm(), idx, reply.Term)
			}
			if rf.getCurrentState() != StateCandidator {
				DPrintf("[election] Raft:%v of term:%v after election, not be candidate, currentState:%v\n",
					rf.me, rf.getCurrentTerm(), atomic.LoadInt32(&rf.currentState))
				return
			}
			if int(atomic.LoadInt64(&cnt)*2) > len(rf.peers) {
				// 竞选成功
				DPrintf("[election] Raft:%v become leader of term:%v\n",
					rf.me, rf.getCurrentTerm())
				rf.turnLeader()
				go rf.heartBeat()
			}
		}()
	}
}

func (rf *Raft) setVoteInfo(server int, term int64) {
	atomic.StoreInt64(&rf.voteInfo.votedFor, int64(server))
	atomic.StoreInt64(&rf.voteInfo.votedTerm, term)
}

// getVoteInfo 返回 votedFor 和 votedTerm
func (rf *Raft) getVoteInfo() (int64, int64) {
	return atomic.LoadInt64(&rf.voteInfo.votedFor),
		atomic.LoadInt64(&rf.voteInfo.votedTerm)
}

// heartBeat 只有 leader 状态才发心跳，其他状态会退出
// 心跳间隔为 100ms
func (rf *Raft) heartBeat() {
	DPrintf("[heartBeat] Raft:%v start heartBeat for term:%v\n",
		rf.me, rf.getCurrentTerm())
	for rf.getCurrentState() == StateLeader {
		rf.sendHeartBeats()
		time.Sleep(100 * time.Millisecond)
	}
	DPrintf("[heartBeat] Raft:%v stop heartBeat for term:%v\n",
		rf.me, atomic.LoadInt64(&rf.currentTerm))
}

func (rf *Raft) sendHeartBeats() {
	prevIdx, prevTerm := rf.getLastLogIndexAndTerm()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		idx := i
		req := &AppendEntriesArgs{
			Term:         atomic.LoadInt64(&rf.currentTerm),
			LeaderID:     rf.me,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      []Entry{},
			LeaderCommit: atomic.LoadInt64(&rf.commitIndex),
		}
		rsp := &AppendEntriesReply{}
		go func() {
			ok := rf.sendHeartBeat(idx, req, rsp)
			if !ok {
				DPrintf("[heartBeat] Leader:%v term:%v heart beat with Raft:%v net fail\n",
					rf.me, rf.getCurrentTerm(), idx)
				return
			}
			if rsp.Term > rf.getCurrentTerm() {
				DPrintf("[heartBeat] Leader:%v see high term:%v of:%v, turn to follower\n",
					rf.me, rsp.Term, idx)
				rf.turnFollower()
				rf.resetElectionTimer()
			}
		}()
	}
}

func (rf *Raft) sendHeartBeat(server int,
	args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func getElectionTimeout() int64 {
	return 500 + (rand.Int63() % 500)
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.turnFollower()
	rf.resetElectionTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
