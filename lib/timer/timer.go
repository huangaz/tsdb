package timer

import (
	"time"
)

const (
	INVALID_TIME int64 = -1
)

type Timer struct {
	start_ int64
	sum_   int64
}

func NewTimer(autoStart bool) *Timer {
	res := &Timer{
		start_: INVALID_TIME,
		sum_:   0,
	}
	if autoStart {
		res.Start()
	}
	return res
}

// Checks if timer is running.
func (t *Timer) running() bool {
	return t.start_ != INVALID_TIME
}

// (Re)starts the timer, regardless if the timer is running.
func (t *Timer) Start() {
	t.start_ = t.getNow()
}

// Returns the total time elapsed.
func (t *Timer) Get() int64 {
	sum := t.sum_
	if t.running() {
		sum += t.getNow() - t.start_
	}
	return sum
}

// Stops the timer. Returns the total time elapsed.
func (t *Timer) Stop() int64 {
	if t.running() {
		t.record()
	}
	t.start_ = INVALID_TIME
	return t.sum_
}

// Returns total time and resets it. Restarts timer immediately.
func (t *Timer) Reset() int64 {
	if t.running() {
		t.record()
		t.Start()
	}
	sum := t.sum_
	t.sum_ = 0
	return sum
}

// Adds elapsed time to the total time in record.
func (t *Timer) record() {
	t.sum_ += t.getNow() - t.start_
}

func (t *Timer) getNow() int64 {
	return time.Now().Unix()
}
