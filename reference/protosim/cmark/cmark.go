//go:build cgo

// Package cmark builds device marks with Hopwake's C hook (pkg/hook/c), as
// firmware would, so the simulated devices exercise the real C code.
package cmark

/*
#cgo CFLAGS: -std=c99 -O2 -I${SRCDIR}/../../../pkg/hook/c
#include <stdlib.h>
#include "hopwake_mark.h"
#include "hopwake_mark.c"
*/
import "C"

import (
	"sync"
	"unsafe"
)

// Queue is one device's mark queue, kept in C memory.
type Queue struct {
	mu    sync.Mutex
	q     *C.hw_queue
	slots *C.hw_mark
}

// Available reports whether marks can be built in this binary.
const Available = true

// New makes a queue of n marks.
func New(n int) *Queue {
	q := &Queue{
		q:     (*C.hw_queue)(C.malloc(C.size_t(unsafe.Sizeof(C.hw_queue{})))),
		slots: (*C.hw_mark)(C.malloc(C.size_t(n) * C.size_t(unsafe.Sizeof(C.hw_mark{})))),
	}
	C.hw_queue_init(q.q, q.slots, C.size_t(n))
	return q
}

// MarkTime keeps a mark for a reading known by type and time (Unix ms).
// It reports whether the oldest mark had to go.
func (q *Queue) MarkTime(hop string, store bool, typ string, timeMs, atMs int64) bool {
	h, t := C.CString(hop), C.CString(typ)
	defer C.free(unsafe.Pointer(h))
	defer C.free(unsafe.Pointer(t))
	op := C.hw_op(C.HW_SEND)
	if store {
		op = C.HW_STORE
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return C.hw_mark_time(q.q, h, op, t, C.int64_t(timeMs), C.int64_t(atMs)) == C.HW_DROPPED_OLDEST
}

// Encode writes the waiting marks that fit in size bytes as one message,
// and how many it holds; nil when nothing is waiting.
func (q *Queue) Encode(size int) ([]byte, int) {
	buf := (*C.char)(C.malloc(C.size_t(size)))
	defer C.free(unsafe.Pointer(buf))
	var n C.size_t
	q.mu.Lock()
	l := C.hw_encode(q.q, buf, C.size_t(size), &n)
	q.mu.Unlock()
	if l == 0 {
		return nil, 0
	}
	return C.GoBytes(unsafe.Pointer(buf), C.int(l)), int(n)
}

// Commit forgets the first n marks, after they were published.
func (q *Queue) Commit(n int) {
	q.mu.Lock()
	C.hw_commit(q.q, C.size_t(n))
	q.mu.Unlock()
}

// Len is how many marks are waiting.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return int(C.hw_queue_len(q.q))
}
