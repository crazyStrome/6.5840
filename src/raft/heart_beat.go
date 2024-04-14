package raft

import (
	"sync/atomic"
	"time"
)

// heartBeat 只有 leader 状态才发心跳，其他状态会退出
// 心跳间隔为 100ms
func (rf *Raft) heartBeat() {
	rf.Logf("[heartBeat] start heartBeat\n")
	for !rf.killed() && rf.getCurrentState() == StateLeader {
		rf.sendHeartBeats()
		time.Sleep(200 * time.Millisecond)
	}
	rf.Logf("[heartBeat] stop heartBeat\n")
}

func (rf *Raft) sendHeartBeats() {
	curTerm := rf.getCurrentTerm()
	commitIndex := atomic.LoadInt64(&rf.commitIndex)
	prevIdx, prevTerm := rf.getLastLogIndexAndTerm()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		idx := i
		req := &AppendEntriesArgs{
			Term:         curTerm,
			LeaderID:     rf.me,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      []Entry{},
			LeaderCommit: commitIndex,
		}
		rsp := &AppendEntriesReply{}
		go func() {
			ok := rf.sendHeartBeat(idx, req, rsp)
			if !ok {
				//rf.Logf("[heartBeat] send heart beat to Raft:%v net fail\n", idx)
				return
			}
			if rsp.Term > rf.getCurrentTerm() {
				rf.Logf("[heartBeat] see high term:%v of Raft:%v, turn to follower\n", rsp.Term, idx)
				rf.turnFollower(rsp.Term)
			}
		}()
	}
}

func (rf *Raft) sendHeartBeat(server int,
	args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
