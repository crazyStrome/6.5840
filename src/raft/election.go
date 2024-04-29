package raft

import (
	"fmt"
	"sync/atomic"
)

// election 选举
func (rf *Raft) election() {
	// 转为 candidator
	rf.turnCandidator()
	// 增加任期
	rf.incrTerm()
	// 给自己投票
	rf.setVoteInfo(rf.me, rf.getCurrentTerm())
	rf.persist()
	rf.Logf("[election] start election and vote for self\n")

	// 重设选举计时器
	rf.resetElectionTimer()

	msgCh := make(chan *RequestVoteReply, len(rf.peers)-1)
	defer close(msgCh)

	me := rf.me
	curTerm := rf.getCurrentTerm()

	lastIdx, lastTerm := rf.getLastLogIndexAndTerm()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		idx := i
		name := fmt.Sprintf("Raft:%v sendRequestVote to Raft:%v in term:%v",
			me, idx, curTerm)
		asyncDo(name, func() {
			req := &RequestVoteArgs{
				Term:         curTerm,
				CandidateID:  me,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			}
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(idx, req, reply)
			if reply.VoteGranted {
				rf.Logf("[election] granted vote of Raft:%v, term:%v\n", idx, reply.Term)
			} else {
				rf.Logf("[election] didn't granted vote of Raft:%v, term:%v, network:%v\n", idx, reply.Term, ok)
			}
			msgCh <- reply
		})
	}
	grantedCnt := 1
	for i := 0; i < len(rf.peers)-1; i++ {
		reply := <-msgCh
		if rf.getCurrentState() != StateCandidator {
			return
		}
		if reply.Term > rf.getCurrentTerm() {
			rf.Logf("[election] see hight term:%v of other, turn follower\n", reply.Term)
			rf.turnFollower(reply.Term)
			rf.persist()
			return
		}
		if reply.VoteGranted {
			grantedCnt++
		}
		if grantedCnt*2 > len(rf.peers) {
			rf.Logf("[election] become leader")
			rf.turnLeader()
			go rf.heartBeats()
			go rf.commitLogs()
			return
		}
	}
	rf.resetElectionTimer()
	rf.Logf("[election] resetElectionTimer\n")
}

func (rf *Raft) incrTerm() {
	atomic.AddInt64(&rf.currentTerm, 1)
}
