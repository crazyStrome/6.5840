package raft

import "log"

// Debugging
const Debug = false

func init() {
	log.SetFlags(log.Lmicroseconds)
}

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}
