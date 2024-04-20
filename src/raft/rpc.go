package raft

import "sync/atomic"

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
	rf.leaderID = -1
}

// compareLog 返回 0，相等；大于 0，rf 的日志更新；小于 0，target 的日志更新
func (rf *Raft) compareLog(targetLastIdx int, targetLastTerm int64) int {
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
	PrevLogIndex int     // 即将处理的记录的前一个索引
	PrevLogTerm  int64   // prevLogIndex 对应记录的任期
	Entries      []Entry // 日志记录，为空则为心跳消息
	LeaderCommit int64   // leader 提交的索引
}

type XData struct {
	XTerm  int64 // 冲突条目对应的任期
	XIndex int   // 冲突任期对应的第一个条目索引
	XLen   int   // 日志长度
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
		rf.Logf("[AppendEntries] log don't match with leader:%v, args.PrevLogIndex:%v, args.PrevLogIndex:%v, is heartBeat:%v, XData:%+v\n",
			args.LeaderID, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries) == 0, reply.XData)
		return
	}

	rf.mu.Lock()
	rf.log = rf.log[:args.PrevLogIndex]
	rf.mu.Unlock()
	rf.persist()

	if len(args.Entries) == 0 {
		// 表明是心跳消息
		// 只有日志匹配之后，才更新 commitIndex
		rf.updateCommitIndex(args.LeaderCommit)
		reply.Success = true
		return
	}

	rf.mu.Lock()
	rf.log = append(rf.log, args.Entries...)
	rf.mu.Unlock()
	rf.persist()
	reply.Success = true
	rf.Logf("[AppendEntries] entries:%+v", rf.cloneLog())
	// 只有日志匹配之后，才更新 commitIndex
	rf.updateCommitIndex(args.LeaderCommit)
	rf.Logf("[AppendEntries] add entry to log at args.PrevLogIndex:%v, args.PrevLogTerm:%v, leader:%v, entry len:%v\n",
		args.PrevLogIndex, args.PrevLogTerm, args.LeaderID, len(args.Entries))
}

func (rf *Raft) getXData(leaderIdx int) XData {
	x := XData{}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	x.XLen = len(rf.log)
	if leaderIdx > len(rf.log) {
		return x
	}
	term := rf.log[leaderIdx-1].Term
	x.XTerm = term
	for i := leaderIdx - 1; i >= 0; i-- {
		if rf.log[i].Term != term {
			break
		}
		x.XIndex = rf.log[i].Index
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

func (rf *Raft) getLastLogIndexAndTerm() (int, int64) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(rf.log) == 0 {
		return 0, 0
	}
	entry := rf.log[len(rf.log)-1]
	return entry.Index, entry.Term
}

// matchLog 判断是否匹配
func (rf *Raft) matchLog(prevIdx int, prevTerm int64) bool {
	if prevIdx == 0 {
		return true
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if prevIdx > len(rf.log) {
		return false
	}
	return rf.log[prevIdx-1].Term == prevTerm
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
