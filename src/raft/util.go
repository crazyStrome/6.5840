package raft

import (
	"log"
)

// Debugging
const Debug = true

func init() {
	log.SetFlags(log.Lmicroseconds)
}

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func asyncDo(name string, f func()) {
	go func() {
		defer func() {
			if e := recover(); e != nil {
				DPrintf("[asyncDo] %v panic:%v", name, e)
			}
		}()

		f()
	}()
}
