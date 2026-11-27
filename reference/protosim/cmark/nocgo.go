//go:build !cgo

package cmark

// Available reports whether marks can be built in this binary: they need
// the C hook, so a binary built without cgo cannot.
const Available = false

// Queue is unavailable without cgo.
type Queue struct{}

func New(int) *Queue                                            { panic("device marks need cgo") }
func (*Queue) MarkTime(string, bool, string, int64, int64) bool { return false }
func (*Queue) Encode(int) ([]byte, int)                         { return nil, 0 }
func (*Queue) Commit(int)                                       {}
func (*Queue) Len() int                                         { return 0 }
