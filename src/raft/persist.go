package raft

func (rf *Raft) getPersistState() PersistState {
	state := PersistState{}
	state.CurrentTerm = rf.getCurrentTerm()
	state.VoteInfo.VotedFor, state.VoteInfo.VotedTerm = rf.getVoteInfo()
	state.Log = rf.cloneLog()
	return state
}

func (rf *Raft) getSnapshot() *Snapshot {
	rf.mu.Lock()
	rf.mu.Unlock()
	return rf.snapshot
}

func (rf *Raft) recoverFromState(state PersistState) {
	rf.setTerm(state.CurrentTerm)
	rf.setVoteInfo(int(state.VoteInfo.VotedFor), state.VoteInfo.VotedTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.log = state.Log
}

func (rf *Raft) recoverFromSnapshot(snapshot *Snapshot) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.snapshot = snapshot
}
