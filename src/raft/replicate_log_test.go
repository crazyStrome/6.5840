package raft

import "testing"

func Test_getMaxMajority(t *testing.T) {
	t.Log(getMaxMajority([]int64{1, 1, 2}))
	t.Log(getMaxMajority([]int64{1, 2, 3}))
}

func Test_getEntries(t *testing.T) {
	rf := &Raft{
		log: []Entry{
			{
				Index: 1,
			},
			{
				Index: 2,
			},
			{
				Index: 3,
			},
			{
				Index: 4,
			},
		},
	}
	t.Logf("%+v\n", rf.getEntries(1, 4))
}
