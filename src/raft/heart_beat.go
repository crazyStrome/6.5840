package raft

import (
	"sync/atomic"
	"time"
)

// heartBeat 只有 leader 状态才发心跳，其他状态会退出
// 心跳间隔为 100ms
func (rf *Raft) heartBeats() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.heartBeat(i)
	}
}

func (rf *Raft) heartBeat(idx int) {
	rf.Logf("[heartBeat] to Raft:%v\n", idx)
	for !rf.killed() && rf.getCurrentState() == StateLeader {
		rf.sendHeartBeat(idx)
		time.Sleep(50 * time.Millisecond)
	}
	rf.Logf("[heartBeat] done to Raft:%v\n", idx)
}

func (rf *Raft) sendHeartBeat(idx int) {
	curTerm := rf.getCurrentTerm()
	commitIndex := atomic.LoadInt64(&rf.commitIndex)
	prevIdx, prevTerm := rf.getLastLogIndexAndTerm()
	req := &AppendEntriesArgs{
		Term:         curTerm,
		LeaderID:     rf.me,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      []Entry{},
		LeaderCommit: commitIndex,
	}
	rsp := &AppendEntriesReply{}
	ok := rf.peers[idx].Call("Raft.AppendEntries", req, rsp)
	if !ok {
		//rf.Logf("[heartBeat] send heart beat to Raft:%v net fail\n", idx)
		return
	}
	if rsp.Term > rf.getCurrentTerm() {
		rf.Logf("[heartBeat] see high term:%v of Raft:%v, turn to follower\n", rsp.Term, idx)
		rf.turnFollower(rsp.Term)
		rf.persist()
		return
	}
	if rsp.Success {
		return
	}
	x := rsp.XData
	nIndex := rf.getNextIndex(prevIdx, x)
	rf.mu.Lock()
	rf.nextIndex[idx] = nIndex
	rf.mu.Unlock()
	rf.Logf("[sendHeartBeat] to Raft:%v log diff, idx:%v, xData:%+v, nextIndex:%v, all:%+v\n",
		idx, prevIdx, x, nIndex, rf.nextIndex)
}
