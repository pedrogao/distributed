package common

import (
	"time"
)

// A Cond is used to wait for conditions.
type Cond struct {
	signal chan struct{}
}

// NewCond returns a Cond.
func NewCond() *Cond {
	return &Cond{
		signal: make(chan struct{}),
	}
}

// WaitWithTimeout wait for signal return remain wait time or timed out.
func (cond *Cond) WaitWithTimeout(timeout time.Duration) (time.Duration, bool) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	begin := Now()
	select {
	case <-cond.signal:
		elapsed := Since(begin)
		remainTimeout := timeout - elapsed
		return remainTimeout, true
	case <-timer.C:
		return 0, false
	}
}

// Wait waits for signals.
func (cond *Cond) Wait() {
	<-cond.signal
}

// Signal wakes one goroutine waiting on c, if there is any.
func (cond *Cond) Signal() {
	select {
	case cond.signal <- struct{}{}:
	default:
	}
}
