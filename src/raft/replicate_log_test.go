package raft

import "testing"

func Test_getMaxMajority(t *testing.T) {
	t.Log(getMaxMajority([]int64{1, 1, 2}))
	t.Log(getMaxMajority([]int64{1, 2, 3}))
}
