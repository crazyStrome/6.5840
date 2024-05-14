package raft

import (
	"sync/atomic"
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int64 // candidate 的任期
	CandidateID  int   // candidate 的编号
	LastLogIndex int64 // 最后一个日志的索引
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
	if curTerm >= args.Term {
		rf.Logf("[RequestVote] see low term:%v of Raft:%v, don't vote\n",
			args.Term, args.CandidateID)
		return
	}
	rf.turnFollower(args.Term)
	rf.persist()
	votedFor, votedTerm := rf.getVoteInfo()
	if votedTerm == args.Term && votedFor != -1 && votedFor != int64(args.CandidateID) {
		rf.Logf("[RequestVote] has vote for Raft:%v for term:%v\n",
			votedFor, votedTerm)
		return
	}
	lastIdx, lastTerm := rf.getLastLogIndexAndTerm()
	if rf.compareLog(args.LastLogIndex, args.LastLogTerm) > 0 {
		rf.Logf("[RequestVote] log lastIdx:%v, lastTerm:%v is new than Raft:%v, don't granted\n",
			lastIdx, lastTerm, args.CandidateID)
		return
	}
	rf.Logf("[RequestVote] candidator:%v'log lastIdx:%v and lastTerm:%v is update-to-date with my lastIdx:%v and lastTerm:%v, got granted for term:%v\n",
		args.CandidateID, lastIdx, lastTerm, args.LastLogIndex,
		args.LastLogTerm, args.Term)
	reply.VoteGranted = true

	rf.setVoteInfo(args.CandidateID, args.Term)
	rf.persist()
	rf.leaderID = -1
}

// compareLog 返回 0，相等；大于 0，rf 的日志更新；小于 0，target 的日志更新
func (rf *Raft) compareLog(targetLastIdx int64, targetLastTerm int64) int {
	lastIdx, lastTerm := rf.getLastLogIndexAndTerm()
	if lastIdx == targetLastIdx && lastTerm == targetLastTerm {
		return 0
	}
	if lastTerm > targetLastTerm {
		return 1
	}
	if lastTerm < targetLastTerm {
		return -1
	}
	if lastIdx > targetLastIdx {
		return 1
	}
	return -1
}

type AppendEntriesArgs struct {
	Term         int64   // leader 的任期
	LeaderID     int     // leader 的编号
	PrevLogIndex int64   // 即将处理的记录的前一个索引
	PrevLogTerm  int64   // prevLogIndex 对应记录的任期
	Entries      []Entry // 日志记录，为空则为心跳消息
	LeaderCommit int64   // leader 提交的索引
}

type XData struct {
	XTerm  int64 // 冲突条目对应的任期
	XIndex int64 // 冲突任期对应的第一个条目索引
	XLen   int64 // 日志长度
}

type AppendEntriesReply struct {
	Term    int64 // 下游的任期
	Success bool  // 如果 follower 包含符合 prevLogIndex 和 prevLogTerm 的日志

	XData XData
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = rf.getCurrentTerm()
	if reply.Term > args.Term {
		rf.Logf("[AppendEntries] see lower term:%v of Raft:%v\n",
			args.Term, args.LeaderID)
		return
	}
	// 收到 leader 的信息，就更新数据
	rf.turnFollower(args.Term)
	rf.persist()
	rf.leaderID = args.LeaderID

	if !rf.matchLog(args.PrevLogIndex, args.PrevLogTerm) {
		reply.XData = rf.getXData(args.PrevLogIndex)
		//rf.Logf("[AppendEntries] log don't match with leader:%v, args.PrevLogIndex:%v, args.PrevLogIndex:%v, is heartBeat:%v, XData:%+v\n",
		//	args.LeaderID, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries) == 0, reply.XData)
		return
	}

	rf.mu.Lock()
	entries := make([]Entry, 0, len(rf.log)+len(args.Entries))
	for _, entry := range rf.log {
		if entry.Index <= args.PrevLogIndex {
			entries = append(entries, entry.clone())
		}
	}
	for _, entry := range args.Entries {
		if rf.getSnapLastIncludeIndex() >= entry.Index {
			continue
		}
		entries = append(entries, entry.clone())
	}
	rf.log = entries
	rf.mu.Unlock()

	rf.persist()

	reply.Success = true
	// 只有日志匹配之后，才更新 commitIndex
	rf.updateCommitIndex(args.LeaderCommit)

	if len(args.Entries) == 0 {
		return
	}
	rf.Logf("[AppendEntries] after entries:%+v", rf.cloneLog())
	rf.Logf("[AppendEntries] add entry to log at args.PrevLogIndex:%v, args.PrevLogTerm:%v, leader:%v, entry len:%v\n",
		args.PrevLogIndex, args.PrevLogTerm, args.LeaderID, len(args.Entries))
}

func (rf *Raft) getXData(leaderIdx int64) XData {
	x := XData{}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	x.XLen = int64(len(rf.log)) + rf.getSnapLastIncludeIndex()
	var term int64
	for _, entry := range rf.log {
		if entry.Index == leaderIdx {
			term = entry.Term
			break
		}
	}
	x.XTerm = term

	for _, entry := range rf.log {
		if entry.Term == term {
			x.XIndex = entry.Index
			break
		}
	}
	return x
}

func (rf *Raft) updateCommitIndex(commitIndex int64) {
	curIdx := atomic.LoadInt64(&rf.commitIndex)
	if commitIndex <= curIdx {
		return
	}
	lastIdx, _ := rf.getLastLogIndexAndTerm()
	afterIdx := min(commitIndex, int64(lastIdx))
	atomic.StoreInt64(&rf.commitIndex, afterIdx)
	rf.Logf("[updateCommitIndex] from:%v to %v\n", curIdx, afterIdx)
}

func (rf *Raft) getLastLogIndexAndTerm() (int64, int64) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(rf.log) == 0 {
		return rf.getSnapLastIncludeIndex(), rf.getSnapLastIncludeTerm()
	}
	entry := rf.log[len(rf.log)-1]
	return entry.Index, entry.Term
}

// matchLog 判断是否匹配
func (rf *Raft) matchLog(prevIdx int64, prevTerm int64) bool {
	if prevIdx == 0 {
		return true
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for _, entry := range rf.log {
		if entry.Index != prevIdx {
			continue
		}
		return entry.Term == prevTerm
	}
	return rf.getSnapLastIncludeIndex() >= prevIdx
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

func min(a, b int64) int64 {
	if a > b {
		return b
	}
	return a
}

type InstallSnapshotArgs struct {
	Term              int64  // leader 的任期
	LeaderID          int    // leader 编号
	LastIncludedIndex int64  // 快照包含的最后一个索引
	LastIncludedTerm  int64  // 快照包含的最后一条日志的任期
	Offset            int64  // 该块数据所在快照的位置
	Data              []byte // 数据块
	Done              bool   // 是否是最后一块
}

type InstallSnapshotReply struct {
	Term int64 // 当前任期，leader 需要用这个来判断
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	curTerm := rf.getCurrentTerm()
	reply.Term = curTerm
	if curTerm > args.Term {
		rf.Logf("[InstallSnapshot] see low term:%v of leader:%v\n",
			args.Term, args.LeaderID)
		return
	}
	rf.turnFollower(args.Term)
	rf.persist()

	if rf.getSnapLastIncludeIndex() >= args.LastIncludedIndex {
		rf.Logf("[InstallSnapshot] already has idx:%v, bigger than:%v\n",
			rf.getSnapLastIncludeIndex(), args.LastIncludedIndex)
		return
	}

	rf.updateEntryForSnapshot(args.LastIncludedIndex,
		args.LastIncludedTerm, args.Data)

	rf.updateCommitIndex(int64(args.LastIncludedIndex))

	rf.persist()

	rf.Logf("[InstallSnapshot] done, args:%+v\n", args)
}
