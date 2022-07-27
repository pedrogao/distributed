package raft

import (
	"github.com/pedrogao/log"
)

// Debug Debugging
const Debug = false

func DPrintf(format string, a ...any) (n int, err error) {
	if Debug {
		log.SetOptions(log.WithLevel(log.DebugLevel))
	}
	log.Infof(format, a...)
	return
}
