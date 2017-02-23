package memalpha

import (
	"errors"
	"fmt"
)

// ProtocolError describes a protocol violation.
type ProtocolError string

func (pe ProtocolError) Error() string {
	return fmt.Sprintf("memcache: protocol error: %s", string(pe))
}

// ClientError means some sort of client error in the input line, i.e. the input doesn't
// confirm to the protocol in some way.
type ClientError string

func (ce ClientError) Error() string {
	return fmt.Sprintf("memcache: client error: %s", string(ce))
}

// ServerError means some sort of server error prevents the server from carrying out the
// command.
type ServerError string

func (se ServerError) Error() string {
	return fmt.Sprintf("memcache: server error: %s", string(se))
}

var (
	// ErrCacheMiss means that a Get failed because the item wasn't present.
	ErrCacheMiss = errors.New("memcache: cache miss")

	// ErrNotFound indicates that the item wasn't present.
	ErrNotFound = errors.New("memcache: item not found")

	// ErrCasConflict indicates that the item you are trying to store with
	// a "cas" command has been modified since you last fetched it.
	ErrCasConflict = errors.New("memcache: compare-and-swap conflict")

	// ErrNotStored normally means that the condition for an "add" or a
	// "replace" command wasn't met.
	ErrNotStored = errors.New("memcache: item not stored")

	// ErrReplyError means the client sent a nonexistent command name.
	ErrReplyError = errors.New("memcache: nonexistent command name")
)
