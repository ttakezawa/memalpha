package memalpha

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func freePort() (int, error) {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}
	defer func() { _ = l.Close() }()

	return l.Addr().(*net.TCPAddr).Port, nil
}

type server struct {
	cmd  *exec.Cmd
	conn *TextConn
}

func newServer() *server {
	return &server{}
}

func (s *server) Start() error {
	port, err := freePort()
	if err != nil {
		return err
	}

	s.cmd = exec.Command("memcached", "-p", strconv.Itoa(port))
	if err = s.cmd.Start(); err != nil {
		return err
	}

	// Wait a bit for the socket to appear.
	for i := 0; i < 10; i++ {
		s.conn, err = Dial(fmt.Sprintf("localhost:%d", port))
		if err == nil {
			return nil
		}
		time.Sleep(time.Duration(25*i) * time.Millisecond)
	}

	return err
}

func (s *server) Shutdown() error {
	_ = s.cmd.Process.Kill()
	return s.cmd.Wait()
}

func TestDialContext(t *testing.T) {
	memd := newServer()
	err := memd.Start()
	if err != nil {
		t.Skipf("skipping test; couldn't start memcached: %s", err)
	}
	defer func() { _ = memd.Shutdown() }()

	ctx, cancel := context.WithCancel(context.Background())
	_, err = DialContext(ctx, memd.conn.Addr)
	assert.NoError(t, err)

	cancel()

	_, err = DialContext(ctx, memd.conn.Addr)
	assert.Error(t, err)
}

func TestLocalhost(t *testing.T) {
	memd := newServer()
	err := memd.Start()
	if err != nil {
		t.Skipf("skipping test; couldn't start memcached: %s", err)
	}
	defer func() { _ = memd.Shutdown() }()

	c := memd.conn

	mustSet := func(key string, value []byte) {
		err := c.Set(key, value, 0, 0, true)
		assert.NoError(t, err, fmt.Sprintf("must Set(%q, %q)", key, value))
	}

	assertItem := func(key string, expected []byte) {
		value, _, err := c.Get(key)
		assert.Nil(t, err)
		assert.NoError(t, err, fmt.Sprintf("must Get(%q)", key))
		assert.Equal(t, string(expected), string(value))
	}

	// Set
	err = c.Set("foo", []byte("fooval"), 0, 0, false)
	assert.NoError(t, err, "first set(foo)")

	err = c.Set("foo", []byte("fooval"), 0, 0, false)
	assert.NoError(t, err, "second set(foo)")

	// Get
	value, _, err := c.Get("foo")
	assert.NoError(t, err, "get(foo)")
	assert.Equal(t, []byte("fooval"), value, "get(fool)")

	// Set large item
	largeKey := string(bytes.Repeat([]byte("A"), 250))
	largeValue := bytes.Repeat([]byte("A"), 1023*1024)
	err = c.Set(largeKey, largeValue, 0, 0, false)
	assert.NoError(t, err, "set(largeKey)")

	// Get large item
	value, _, err = c.Get(largeKey)
	assert.NoError(t, err, "get(largeKey)")
	assert.Equal(t, largeValue, value, "get(largeKey)")

	// Set noreply
	err = c.Set("set_norep", []byte("val"), 0, 0, true)
	assert.NoError(t, err, "set(set_norep, val, noreply)")
	assertItem("set_norep", []byte("val"))

	// Set with flags
	err = c.Set("set_flags", []byte("val"), 42, 0, false)
	assert.NoError(t, err, "set(set_flags, val, flags = 42)")
	_, flags, err := c.Get("set_flags")
	assert.NoError(t, err, "get(set_flags)")
	assert.EqualValues(t, 42, flags, "get(set_flags)")

	// Set with exptime
	err = c.Set("set_exptime", []byte("val"), 0, 1, false)
	assert.NoError(t, err, "set(set_exptime, val, exptime = 1)")
	assertItem("set_exptime", []byte("val"))
	time.Sleep(time.Second)
	_, _, err = c.Get("set_exptime")
	assert.Equal(t, ErrCacheMiss, err, "get(set_exptime)")

	// Gets
	mustSet("bar", []byte("barval"))
	m, err := c.Gets([]string{"foo", "bar"})
	assert.NoError(t, err, "gets(foo, bar)")
	keyToValue := make(map[string]string)
	for key, response := range m {
		keyToValue[key] = string(response.Value)
	}
	expected := map[string]string{"foo": "fooval", "bar": "barval"}
	assert.Equal(t, expected, keyToValue, "gets(foo, bar)")

	// Add
	err = c.Add("baz", []byte("baz1"), 0, 0, false)
	assert.NoError(t, err, "first add(baz)")
	err = c.Add("baz", []byte("baz2"), 0, 0, false)
	assert.Equal(t, ErrNotStored, err, "second add(baz)")

	// Add noreply
	err = c.Add("add_norep", []byte("val"), 0, 0, true)
	assert.NoError(t, err, "add(add_norep, noreply)")
	assertItem("add_norep", []byte("val"))

	// Replace
	mustSet("foo", []byte("fooval"))
	err = c.Replace("foo", []byte("fooval2"), 0, 0, false)
	assert.NoError(t, err, "replace(foo, fooval2)")
	assertItem("foo", []byte("fooval2"))

	// Replace noreply
	err = c.Replace("foo", []byte("fooval3"), 0, 0, true)
	assert.NoError(t, err, "replace(foo, fooval3, noreply)")
	assertItem("foo", []byte("fooval3"))

	// Append
	err = c.Append("foo", []byte("suffix"), false)
	assert.NoError(t, err, "append(foo, suffix)")
	assertItem("foo", []byte("fooval3suffix"))

	// Append noreply
	mustSet("bar", []byte("fooval"))
	err = c.Append("bar", []byte("app"), true)
	assert.NoError(t, err, "replace(bar, app)")
	assertItem("bar", []byte("foovalapp"))

	// Prepend
	err = c.Prepend("foo", []byte("prefix"), false)
	assert.NoError(t, err, "prepend(foo, prefix)")
	assertItem("foo", []byte("prefixfooval3suffix"))

	// Prepend noreply
	err = c.Prepend("foo", []byte("pre"), true)
	assert.NoError(t, err, "prepend(foo, pre)")
	assertItem("foo", []byte("preprefixfooval3suffix"))

	// CompareAndSwap
	m, err = c.Gets([]string{"foo"})
	assert.NoError(t, err, "gets(foo)")
	err = c.CompareAndSwap("foo", []byte("swapped"), m["foo"].CasID, 0, 0, false)
	assert.NoError(t, err, "cas(foo, swapped, casid)")
	err = c.CompareAndSwap("foo", []byte("swapped_failed"), m["foo"].CasID, 0, 0, false)
	assert.Equal(t, ErrCasConflict, err, "cas(foo, swapped_faile, casid)")
	assertItem("foo", []byte("swapped"))

	// CompareAndSwap noreply
	m, err = c.Gets([]string{"foo"})
	assert.NoError(t, err, "gets(foo)")
	err = c.CompareAndSwap("foo", []byte("swapped_norep"), m["foo"].CasID, 0, 0, true)
	assert.NoError(t, err, "cas(foo, swapped_norep, casid)")
	assertItem("foo", []byte("swapped_norep"))

	// CompareAndSwap raises ErrNotFound
	err = c.CompareAndSwap("not_exists", []byte("ignored"), 42, 0, 0, false)
	assert.Equal(t, ErrNotFound, err, "cas(not_exists)")

	// Delete
	err = c.Delete("foo", false)
	assert.NoError(t, err, "delete(foo)")
	_, _, err = c.Get("foo")
	assert.Equal(t, ErrCacheMiss, err, "get(foo)")

	// Delete noreply
	mustSet("foo", []byte("exist"))
	err = c.Delete("foo", true)
	assert.NoError(t, err, "delete(foo, noreply)")
	_, _, err = c.Get("foo")
	assert.Equal(t, ErrCacheMiss, err, "get(foo)")

	// Delete raises ErrNotFound
	err = c.Delete("not_exists", false)
	assert.Equal(t, ErrNotFound, err, "delete(not_exists)")

	// Increment
	mustSet("foo", []byte("35"))
	num, err := c.Increment("foo", 7, false)
	assert.NoError(t, err, "incr(foo, 7)")
	assert.EqualValues(t, 42, num, "incr(foo, 7)")

	// Increment noreply
	_, err = c.Increment("foo", 2, true)
	assert.NoError(t, err, "incr(foo, 2, noreply)")
	assertItem("foo", []byte("44"))

	// Increment raises ErrNotFound
	_, err = c.Increment("not_exists", 10, false)
	assert.Equal(t, ErrNotFound, err, "incr(not_exists, 10)")

	// Decrement
	num, err = c.Decrement("foo", 2, false)
	assert.NoError(t, err, "decr(foo, 2)")
	assert.EqualValues(t, 42, num, "decr(foo, 2)")
	assertItem("foo", []byte("42"))

	// Touch
	err = c.Touch("foo", 2, false)
	assert.NoError(t, err, "touch(foo, 2)")
	assertItem("foo", []byte("42"))
	time.Sleep(2 * time.Second)
	_, _, err = c.Get("foo")
	assert.Equal(t, ErrCacheMiss, err, "get(foo)")

	// Touch noreply
	mustSet("foo", []byte("val"))
	err = c.Touch("foo", 2, true)
	assert.NoError(t, err, "touch(foo, 2, noreply)")

	// Touch raises ErrNotFound
	err = c.Touch("not_exists", 10, false)
	assert.Equal(t, ErrNotFound, err, "touch(not_exists)")

	// Stats
	stats, err := c.Stats()
	assert.NoError(t, err, "stats()")
	if len(stats) < 2 {
		t.Fatalf("stats(): len(Value) = %q, want len(value) > 2", stats)
	}

	// StatsArg
	stats, err = c.StatsArg("slabs")
	assert.NoError(t, err, "stats(slabs)")
	if len(stats) < 2 {
		t.Fatalf("stats(): len(Value) = %q, want len(value) > 2", stats)
	}

	// FlushAll
	mustSet("foo", []byte("bar"))
	err = c.FlushAll(0, false)
	assert.NoError(t, err, "flush_all(0)")
	_, _, err = c.Get("foo")
	assert.Equal(t, ErrCacheMiss, err, "get(foo)")

	// FlushAll delayed
	mustSet("foo", []byte("val"))
	err = c.FlushAll(1, false)
	assert.NoError(t, err, "flush_all(1)")
	time.Sleep(1 * time.Second)
	_, _, err = c.Get("foo")
	assert.Equal(t, ErrCacheMiss, err, "get(foo)")

	// FlushAll non optional delayed
	err = c.FlushAll(-1, false)
	assert.NoError(t, err, "flush_all(-1)")

	// FlushAll noreply
	err = c.FlushAll(0, true)
	assert.NoError(t, err, "flush_all(0, noreply)")

	// Version
	ver, err := c.Version()
	assert.NoError(t, err, "version()")
	assert.NotEmpty(t, ver, "version()")

	// Quit
	err = c.Quit()
	assert.NoError(t, err, "quit()")
	if c.netConn == nil {
		t.Fatalf("net.Conn = %q, want nil", c.netConn)
	}

	// Close
	err = c.Close()
	assert.NoError(t, err, "c.Close()")

	// Close again
	err = c.Close()
	assert.NoError(t, err, "retry c.Close()")
}
