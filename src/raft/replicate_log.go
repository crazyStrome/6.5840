package raft

import (
	"sync/atomic"
	"time"
)

// commitLogs 每个 follower 一个线程，进行日志提交
func (rf *Raft) commitLogs() {
	// 初始化 nextIndex 和 matchIndex
	lastIdx, _ := rf.getLastLogIndexAndTerm()
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = lastIdx + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.commitLog(i)
	}
}

func (rf *Raft) commitLog(idx int) {
	rf.Logf("[commitLog] start to Raft:%v\n", idx)
	for !rf.killed() && rf.getCurrentState() == StateLeader {
		lastIdx, _ := rf.getLastLogIndexAndTerm()
		nextIdx := rf.nextIndex[idx]
		if nextIdx > lastIdx {
			// 还没有新日志，等待久一点
			time.Sleep(100 * time.Millisecond)
			continue
		}
		var prevIdx int
		var prevTerm int64
		rf.mu.Lock()
		entries := rf.log[nextIdx:lastIdx]
		if nextIdx > 0 && len(rf.log) >= nextIdx {
			prevEntry := rf.log[nextIdx-1]
			prevIdx = prevEntry.Index
			prevTerm = prevEntry.Term
		}
		rf.mu.Unlock()
		args := &AppendEntriesArgs{
			Term:         rf.getCurrentTerm(),
			LeaderID:     rf.me,
			LeaderCommit: atomic.LoadInt64(&rf.commitIndex),
			Entries:      entries,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
		}
		reply := &AppendEntriesReply{}
		ok := rf.peers[idx].Call("Raft.AppendEntries", args, reply)
		if !ok {
			rf.Logf("[commitLog] AppendEntries to Raft:%v fail\n", idx)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if reply.Term > rf.getCurrentTerm() {
			rf.Logf("[commitLog] see high term:%v of Raft:%v\n", reply.Term, idx)
			rf.turnFollower(reply.Term)
			break
		}
		if reply.Success {
			rf.nextIndex[idx]++
			rf.matchIndex[idx] = nextIdx
			rf.updateLeaderCommitIndex()
		} else {
			rf.nextIndex[idx]--
		}
		time.Sleep(10 * time.Millisecond)
	}
	rf.Logf("[commitLog] stop to Raft:%v\n", idx)
}

func (rf *Raft) applyLog() {
	for !rf.killed() {
		commitIndex := atomic.LoadInt64(&rf.commitIndex)
		lastApplied := atomic.LoadInt64(&rf.lastApplied)
		if lastApplied < commitIndex {
			rf.mu.Lock()
			entry := rf.log[lastApplied]
			rf.mu.Unlock()
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: int(lastApplied + 1),
			}
			atomic.AddInt64(&rf.lastApplied, 1)
			rf.Logf("[applyLog] done with idx:%v, entry:%+v\n",
				lastApplied+1, entry)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// getEntries 返回 [start: end] 索引的数据，索引是条目索引，从 1 开始
func (rf *Raft) getEntries(start, end int) []Entry {
	if end <= start {
		return nil
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.log[start-1 : end-1]
}

func (rf *Raft) updateLeaderCommitIndex() {
	if rf.getCurrentState() != StateLeader {
		return
	}
	matchIndexs := make(map[int]int)
	for _, idx := range rf.matchIndex {
		matchIndexs[idx]++
	}
	var commitIndex int
	for idx, cnt := range matchIndexs {
		if cnt*2 > len(rf.peers) && commitIndex < idx {
			commitIndex = idx
		}
	}
	atomic.StoreInt64(&rf.commitIndex, int64(commitIndex))
}
