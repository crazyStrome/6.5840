package raft

import (
	"sort"
	"sync/atomic"
	"time"
)

// commitLogs 每个 follower 一个线程，进行日志提交
func (rf *Raft) commitLogs() {
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
		prevEntries := rf.getEntries(nextIdx-1, nextIdx)
		if len(prevEntries) != 0 {
			prevIdx = prevEntries[0].Index
			prevTerm = prevEntries[0].Term
		}
		entries := rf.getEntries(nextIdx, lastIdx+1)
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
			rf.persist()
			break
		}
		if reply.Success {
			rf.nextIndex[idx] += len(entries)
			rf.matchIndex[idx] = prevIdx + len(entries)
			rf.updateLeaderCommitIndex()
			rf.Logf("[commitLog] append to Raft:%v success, idx:%v, len:%v\n",
				idx, nextIdx, len(entries))
			time.Sleep(10 * time.Millisecond)
			continue
		}
		nIndex := nextIdx - 1
		x := reply.XData
		xTermEntries := rf.getEntriesForTerm(x.XTerm)
		if len(xTermEntries) == 0 {
			nIndex = x.XIndex
		} else {
			nIndex = xTermEntries[len(xTermEntries)-1].Index
		}
		if prevIdx > x.XLen {
			nIndex = x.XLen
		}
		if nIndex == 0 {
			nIndex = 1
		}
		rf.nextIndex[idx] = nIndex
		rf.Logf("[commitLog] append to Raft:%v fail, idx:%v, len:%v, xData:%+v, nextIdx:%v\n",
			idx, nextIdx, len(entries), x, nIndex)
		time.Sleep(5 * time.Millisecond)
	}
	rf.Logf("[commitLog] stop to Raft:%v\n", idx)
}

func (rf *Raft) getEntriesForTerm(term int64) []Entry {
	entries := []Entry{}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for _, entry := range rf.log {
		if entry.Term != term {
			continue
		}
		entries = append(entries, entry.clone())
	}
	return entries
}

func (rf *Raft) applyLog() {
	for !rf.killed() {
		commitIndex := atomic.LoadInt64(&rf.commitIndex)
		lastApplied := atomic.LoadInt64(&rf.lastApplied)
		if lastApplied < commitIndex {
			entries := rf.getEntries(int(lastApplied+1), int(commitIndex+1))
			for _, entry := range entries {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: entry.Index,
				}
			}
			atomic.AddInt64(&rf.lastApplied, int64(len(entries)))
			rf.Logf("[applyLog] done with idx:%v, entry:%+v\n",
				lastApplied+1, entries)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// getEntries 返回 [start: end] 索引的数据，索引是条目索引，从 1 开始
func (rf *Raft) getEntries(start, end int) []Entry {
	if start <= 0 {
		return nil
	}
	if end <= start {
		return nil
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if start > len(rf.log) {
		return nil
	}
	entries := []Entry{}
	for i := start - 1; i < end-1 && i < len(rf.log); i++ {
		entries = append(entries, rf.log[i].clone())
	}
	return entries
}

func (rf *Raft) updateLeaderCommitIndex() {
	if rf.getCurrentState() != StateLeader {
		return
	}
	matchIndexs := make([]int64, 0, len(rf.peers))
	for _, idx := range rf.matchIndex {
		matchIndexs = append(matchIndexs, int64(idx))
	}
	commitIndex := getMaxMajority(matchIndexs)
	rf.Logf("[updateLeaderCommitIndex] from:%v to:%v\n", atomic.LoadInt64(&rf.commitIndex), commitIndex)

	atomic.StoreInt64(&rf.commitIndex, int64(commitIndex))
}

func getMaxMajority(matchIndexs []int64) int64 {
	if len(matchIndexs) == 0 {
		return 0
	}
	sort.Slice(matchIndexs, func(i, j int) bool {
		return matchIndexs[i] < matchIndexs[j]
	})
	return matchIndexs[len(matchIndexs)/2]
}
