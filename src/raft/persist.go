package raft

func (rf *Raft) getPersistState() PersistState {
	state := PersistState{}
	state.CurrentTerm = rf.getCurrentTerm()
	state.VoteInfo.VotedFor, state.VoteInfo.VotedTerm = rf.getVoteInfo()
	state.Log = rf.cloneLog()
	state.SnapLastIncludedIndex = rf.getSnapLastIncludeIndex()
	state.SnapLastIncludedTerm = rf.getSnapLastIncludeTerm()
	return state
}

func (rf *Raft) getSnapshot() []byte {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return clone(rf.snapshot)
}

func (rf *Raft) recoverFromState(state PersistState) {
	rf.setTerm(state.CurrentTerm)
	rf.setVoteInfo(int(state.VoteInfo.VotedFor), state.VoteInfo.VotedTerm)
	rf.setSnapLastIncludeIndex(state.SnapLastIncludedIndex)
	rf.setSnapLastIncludeTerm(state.SnapLastIncludedTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.log = state.Log
}

func (rf *Raft) setSnapshot(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.snapshot = clone(data)
}
