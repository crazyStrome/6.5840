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
		nextIdx := rf.getNextIndex(idx)
		if nextIdx > lastIdx {
			// 还没有新日志，等待久一点
			//rf.Logf("[commitLog] sleep idx:%v, lastIndex:%v, nextIndex:%v\n",
			//	idx, lastIdx, nextIdx)
			//time.Sleep(10 * time.Millisecond)
			continue
		}
		//rf.Logf("[commitLog] start idx:%v, lastIndex:%v, nextIndex:%v\n",
		//	idx, lastIdx, nextIdx)

		// 如果 nextIndex 在快照里，就 InstallSnapshot
		var ok bool
		if rf.getSnapLastIncludeIndex() >= nextIdx {
			ok = rf.sendSnapshot(idx)
		} else {
			ok = rf.sendAppendEntries(idx)
		}
		if !ok {
			time.Sleep(5 * time.Millisecond)
		} else {
			time.Sleep(1 * time.Millisecond)
		}
	}
	rf.Logf("[commitLog] stop to Raft:%v\n", idx)
}

func (rf *Raft) sendSnapshot(idx int) bool {
	args := &InstallSnapshotArgs{
		Term:              rf.getCurrentTerm(),
		LeaderID:          rf.me,
		LastIncludedIndex: rf.getSnapLastIncludeIndex(),
		LastIncludedTerm:  rf.getSnapLastIncludeTerm(),
		Offset:            0,
		Data:              rf.getSnapshot(),
		Done:              true,
	}
	reply := &InstallSnapshotReply{}
	ok := rf.peers[idx].Call("Raft.InstallSnapshot", args, reply)
	if !ok {
		rf.Logf("[sendSnapshot] InstallSnapshot to Raft:%v fail, args:%+v, reply:%+v, ok:%v\n",
			idx, args, reply, ok)
		return false
	}

	if reply.Term > rf.getCurrentTerm() {
		rf.Logf("[commitLog] InstallSnapshot see high term:%v of Raft:%v\n", reply.Term, idx)
		rf.turnFollower(reply.Term)
		rf.persist()
		return true
	}

	rf.setNextIndex(idx, args.LastIncludedIndex+1)
	rf.setMatchIndex(idx, args.LastIncludedIndex)

	rf.updateLeaderCommitIndex()

	rf.Logf("[commitLog] InstallSnapshot to Raft:%v suc, idx:%v, offset:%v, next:%+v\n",
		idx, args.LastIncludedIndex, args.Offset, rf.nextIndex)
	return true
}

func (rf *Raft) sendAppendEntries(idx int) bool {
	nextIdx := rf.getNextIndex(idx)
	lastIdx, _ := rf.getLastLogIndexAndTerm()

	var prevIdx int64
	var prevTerm int64
	prevEntries := rf.getEntries(nextIdx-1, nextIdx)
	if len(prevEntries) != 0 {
		prevIdx = prevEntries[0].Index
		prevTerm = prevEntries[0].Term
	}
	if rf.getSnapLastIncludeIndex() == nextIdx-1 {
		prevIdx = nextIdx - 1
		prevTerm = rf.getSnapLastIncludeTerm()
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
		rf.Logf("[commitLog] AppendEntries to Raft:%v fail, args:%+v\n", idx, args)
		return false
	}
	if reply.Term > rf.getCurrentTerm() {
		rf.Logf("[commitLog] see high term:%v of Raft:%v\n", reply.Term, idx)
		rf.turnFollower(reply.Term)
		rf.persist()
		return true
	}
	if reply.Success {
		rf.setNextIndex(idx, nextIdx+int64(len(entries)))
		rf.setMatchIndex(idx, prevIdx+int64(len(entries)))
		rf.updateLeaderCommitIndex()
		rf.Logf("[commitLog] append to Raft:%v success, idx:%v, len:%v\n",
			idx, nextIdx, len(entries))
		return true
	}
	x := reply.XData
	nIndex := rf.getXNextIndex(prevIdx, x)
	rf.setNextIndex(idx, nIndex)
	rf.Logf("[commitLog] append to Raft:%v fail, idx:%v, len:%v, xData:%+v, nextIdx:%v\n",
		idx, nextIdx, len(entries), x, nIndex)
	return true
}

func (rf *Raft) getXNextIndex(prevIdx int64, x XData) int64 {
	xTermEntries := rf.getEntriesForTerm(x.XTerm)
	var nIndex int64
	if len(xTermEntries) == 0 {
		nIndex = x.XIndex
	} else {
		nIndex = xTermEntries[len(xTermEntries)-1].Index
	}
	if prevIdx > x.XLen {
		nIndex = x.XLen + 1
	}
	if nIndex == 0 {
		nIndex = 1
	}
	return nIndex
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
			rf.Logf("[applyLog] current logs:%+v\n", rf.cloneLog())
			// 如果 lastApplied + 1 在快照里，就用应用快照
			if rf.getSnapLastIncludeIndex() > lastApplied {
				rf.applyCh <- ApplyMsg{
					SnapshotValid: true,
					Snapshot:      rf.getSnapshot(),
					SnapshotTerm:  int(rf.getSnapLastIncludeTerm()),
					SnapshotIndex: int(rf.getSnapLastIncludeIndex()),
				}
				atomic.StoreInt64(&rf.lastApplied, rf.getSnapLastIncludeIndex())
				rf.Logf("[applyLog] apply snapshot idx:%v\n", rf.getSnapLastIncludeIndex())
				continue
			}
			entries := rf.getEntries(lastApplied+1, commitIndex+1)
			for _, entry := range entries {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: int(entry.Index),
				}
			}
			atomic.AddInt64(&rf.lastApplied, int64(len(entries)))
			rf.Logf("[applyLog] done with idx:%v, entry:%+v\n",
				lastApplied+1, entries)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// getEntries 返回 [start: end) 索引的数据，索引是条目索引，从 1 开始
func (rf *Raft) getEntries(start, end int64) []Entry {
	if start <= 0 {
		return nil
	}
	if end <= start {
		return nil
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	entries := make([]Entry, 0, end-start)
	for _, entry := range rf.log {
		if entry.Index < start || entry.Index >= end {
			continue
		}
		entries = append(entries, entry.clone())
	}
	return entries
}

func (rf *Raft) updateLeaderCommitIndex() {
	if rf.getCurrentState() != StateLeader {
		return
	}
	matchIndexs := make([]int64, 0, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		matchIndexs = append(matchIndexs, rf.getMatchIndex(i))
	}
	commitIndex := getMaxMajority(matchIndexs)
	rf.Logf("[updateLeaderCommitIndex] from:%v to:%v, matchIndex:%+v\n",
		atomic.LoadInt64(&rf.commitIndex), commitIndex, rf.matchIndex)

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
