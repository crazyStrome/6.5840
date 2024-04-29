package raft

func (rf *Raft) getPersistState() PersistState {
	state := PersistState{}
	state.CurrentTerm = rf.getCurrentTerm()
	state.VoteInfo.VotedFor, state.VoteInfo.VotedTerm = rf.getVoteInfo()
	state.Log = rf.cloneLog()
	snapshot := rf.getSnapshot()
	if snapshot != nil {
		state.Snapshot.LastIncludedIndex = snapshot.LastIncludedIndex
		state.Snapshot.LastIncludedTerm = snapshot.LastIncludedTerm
	}
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
	rf.snapshot = &Snapshot{
		LastIncludedIndex: state.Snapshot.LastIncludedIndex,
		LastIncludedTerm:  state.Snapshot.LastIncludedTerm,
	}
	rf.log = state.Log
}

func (rf *Raft) recoverFromSnapshot(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.snapshot == nil {
		rf.snapshot = &Snapshot{}
	}
	rf.snapshot.Snapshot = data
}
