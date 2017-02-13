package memalpha

import (
	"bytes"
	"fmt"
	"net"
	"os/exec"
	"reflect"
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
	defer l.Close()

	return l.Addr().(*net.TCPAddr).Port, nil
}

type server struct {
	cmd    *exec.Cmd
	client *Client
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

	s.client = NewClient(fmt.Sprintf("localhost:%d", port))

	// Wait a bit for the socket to appear.
	for i := 0; i < 10; i++ {
		s.client.ensureConnected()
		err = s.client.Err()
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

func TestLocalhost(t *testing.T) {
	memd := newServer()
	err := memd.Start()

	if err != nil {
		t.Skipf("skipping test; couldn't start memcached: %s", err)
	}
	defer memd.Shutdown()

	c := memd.client

	mustSet := func(key string, value []byte) {
		err := c.Set(key, value, true)
		assert.Nil(t, err)
	}

	assertItem := func(key string, expected []byte) {
		value, _, err := c.Get(key)
		assert.Nil(t, err)
		assert.Equal(t, expected, value)
	}

	// Set
	err = c.Set("foo", []byte("fooval"), false)
	if err != nil {
		t.Fatalf("first set(foo): %v", err)
	}
	err = c.Set("foo", []byte("fooval"), false)
	if err != nil {
		t.Fatalf("second set(foo): %v", err)
	}

	// Get
	value, _, err := c.Get("foo")
	if err != nil {
		t.Fatalf("get(foo): %v", err)
	}
	if !bytes.Equal(value, []byte("fooval")) {
		t.Fatalf("get(foo) Value = %q, want fooval", value)
	}

	// Set large item
	largeKey := string(bytes.Repeat([]byte("A"), 250))
	largeValue := bytes.Repeat([]byte("A"), 1023*1024)
	err = c.Set(largeKey, largeValue, false)
	if err != nil {
		t.Fatalf("set(largeKey): %v", err)
	}

	// Get large item
	value, _, err = c.Get(largeKey)
	if err != nil {
		t.Fatalf("get(largeKey): %v", err)
	}
	if !bytes.Equal(value, largeValue) {
		peekLen := len(value)
		if peekLen > 10 {
			peekLen = 10
		}
		t.Fatalf("get(largeKey) Value = %q, want fooval", value[:peekLen])
	}

	// TODO: Set noreply

	// Gets
	err = c.Set("bar", []byte("barval"), false)
	if err != nil {
		t.Fatalf("set(bar): %v", err)
	}
	m, err := c.Gets([]string{"foo", "bar"})
	if err != nil {
		t.Fatalf("gets(foo, bar): %v", err)
	}
	keyToValue := make(map[string]string)
	for key, response := range m {
		keyToValue[key] = string(response.Value)
	}
	expected := map[string]string{"foo": "fooval", "bar": "barval"}
	if !reflect.DeepEqual(keyToValue, expected) {
		t.Fatalf("gets(foo, bar) Value = %+v, want %+v", m, expected)
	}

	// Add
	err = c.Add("baz", []byte("baz1"))
	if err != nil {
		t.Fatalf("first add(baz): %v", err)
	}
	err = c.Add("baz", []byte("baz2"))
	if err != ErrNotStored {
		t.Fatalf("second add(baz) Error = ErrNotStored, want %+v", err)
	}
	// TODO: Add noreply

	// Replace
	mustSet("foo", []byte("fooval"))
	err = c.Replace("foo", []byte("fooval2"))
	if err != nil {
		t.Fatalf("replace(foo): %v", err)
	}
	value, _, err = c.Get("foo")
	if err != nil {
		t.Fatalf("get(foo): %v", err)
	}
	if !bytes.Equal(value, []byte("fooval2")) {
		t.Fatalf("replace(foo, fooval2) then, get(foo) Value = %q, want fooval2", value)
	}
	// TODO: Replace noreply

	// Append
	err = c.Append("foo", []byte("suffix"))
	if err != nil {
		t.Fatalf("append(foo, suffix): %v", err)
	}
	value, _, err = c.Get("foo")
	if err != nil {
		t.Fatalf("get(foo): %v", err)
	}
	if !bytes.Equal(value, []byte("fooval2suffix")) {
		t.Fatalf("append(foo, suffix) then, get(foo) Value = %q, want fooval2suffix", value)
	}
	// TODO: Replace noreply

	// Prepend
	err = c.Prepend("foo", []byte("prefix"))
	if err != nil {
		t.Fatalf("prepend(foo, prefix): %v", err)
	}
	value, _, err = c.Get("foo")
	if err != nil {
		t.Fatalf("get(foo): %v", err)
	}
	if !bytes.Equal(value, []byte("prefixfooval2suffix")) {
		t.Fatalf("prepend(foo, prefix) then, get(foo) Value = %q, want prefixfooval2suffix", value)
	}
	// TODO: Prepend noreply

	// CompareAndSwap
	m, err = c.Gets([]string{"foo"})
	if err != nil {
		t.Fatalf("gets(foo): %v", err)
	}
	err = c.CompareAndSwap("foo", []byte("swapped"), m["foo"].CasID)
	if err != nil {
		t.Fatalf("cas(foo, swapped, casid): %v", err)
	}
	err = c.CompareAndSwap("foo", []byte("swapped"), m["foo"].CasID)
	if err != ErrCasConflict {
		t.Fatalf("cas(foo, swapped, casid) Error = %v, want %v", err, ErrCasConflict)
	}
	// TODO: CompareAndSwap noreply

	// CompareAndSwap raises ErrNotFound
	err = c.CompareAndSwap("not_exists", []byte("ignored"), 42)
	if err != ErrNotFound {
		t.Fatalf("cas(not_exists) Error = %q, want ErrNotFound", err)
	}

	// Delete
	err = c.Delete("foo", false)
	if err != nil {
		t.Fatalf("delete(foo): %v", err)
	}
	_, _, err = c.Get("foo")
	if err != ErrCacheMiss {
		t.Fatalf("get(foo) Error = %q, want ErrCacheMiss", err)
	}

	// Delete noreply
	mustSet("foo", []byte("exist"))
	err = c.Delete("foo", true)
	assert.Nil(t, err)
	_, _, err = c.Get("foo")
	if err != ErrCacheMiss {
		t.Fatalf("get(foo) Error = %q, want ErrCacheMiss", err)
	}

	// Delete raises ErrNotFound
	err = c.Delete("not_exists", false)
	if err != ErrNotFound {
		t.Fatalf("delete(not_exists) Error = %q, want ErrNotFound", err)
	}

	// Increment
	mustSet("foo", []byte("35"))
	num, err := c.Increment("foo", 7, false)
	if err != nil {
		t.Fatalf("incr(foo, 7): %v", err)
	}
	if num != 42 {
		t.Fatalf("incr(foo, 7) Value = %q, want 42", num)
	}

	// Increment noreply
	num, err = c.Increment("foo", 2, true)
	assert.Nil(t, err)
	assertItem("foo", []byte("44"))

	// Increment raises ErrNotFound
	_, err = c.Increment("not_exists", 10, false)
	if err != ErrNotFound {
		t.Fatalf("incr(not_exists) Error = %q, want ErrNotFound", err)
	}

	// Decrement
	num, err = c.Decrement("foo", 2, false)
	if err != nil {
		t.Fatalf("decr(foo, 2): %v", err)
	}
	if num != 42 {
		t.Fatalf("decr(foo, 2) Value = %q, want 42", num)
	}
	value, _, err = c.Get("foo")
	if err != nil {
		t.Fatalf("get(foo): %v", err)
	}
	if !bytes.Equal(value, []byte("42")) {
		t.Fatalf("get(foo) Value = %q, want 42", value)
	}

	// Touch
	err = c.Touch("foo", 2, false)
	if err != nil {
		t.Fatalf("touch(foo, 2): %v", err)
	}
	value, _, err = c.Get("foo")
	if err != nil {
		t.Fatalf("get(foo): %v", err)
	}
	if !bytes.Equal(value, []byte("42")) {
		t.Fatalf("get(foo) Value = %q, want 42", value)
	}
	time.Sleep(2 * time.Second)
	_, _, err = c.Get("foo")
	if err != ErrCacheMiss {
		t.Fatalf("get(foo) Error = %q, want ErrCacheMiss", err)
	}

	// Touch noreply
	mustSet("foo", []byte("val"))
	err = c.Touch("foo", 2, true)
	assert.Nil(t, err)

	// Touch raises ErrNotFound
	err = c.Touch("not_exists", 10, false)
	if err != ErrNotFound {
		t.Fatalf("touch(not_exists) Error = %q, want ErrNotFound", err)
	}

	// Stats
	stats, err := c.Stats()
	if err != nil {
		t.Fatalf("stats(): %v", err)
	}
	if len(stats) < 2 {
		t.Fatalf("stats(): len(Value) = %q, want len(value) > 2", stats)
	}

	// StatsArg
	stats, err = c.StatsArg("slabs")
	if err != nil {
		t.Fatalf("stats(): %v", err)
	}
	if len(stats) < 2 {
		t.Fatalf("stats(): len(Value) = %q, want len(value) > 2", stats)
	}

	// FlushAll
	mustSet("foo", []byte("bar"))
	err = c.FlushAll(0, false)
	if err != nil {
		t.Fatalf("version(): %v", err)
	}
	_, _, err = c.Get("foo")
	if err != ErrCacheMiss {
		t.Fatalf("get(foo) Error = %q, want ErrCacheMiss", err)
	}

	// FlushAll delayed
	mustSet("foo", []byte("val"))
	err = c.FlushAll(1, false)
	assert.Nil(t, err)
	time.Sleep(1 * time.Second)
	_, _, err = c.Get("foo")
	if err != ErrCacheMiss {
		t.Fatalf("get(foo) Error = %q, want ErrCacheMiss", err)
	}

	// FlushAll non optional delayed
	err = c.FlushAll(-1, false)
	assert.Nil(t, err)

	// FlushAll noreply
	err = c.FlushAll(0, true)
	assert.Nil(t, err)

	// Version
	ver, err := c.Version()
	if err != nil {
		t.Fatalf("version(): %v", err)
	}
	if len(ver) == 0 {
		t.Fatalf("version() Value = %q, want len(value) > 0", ver)
	}

	// Quit
	err = c.Quit()
	if err != nil {
		t.Fatalf("quit(): %v", err)
	}
	if c.conn == nil {
		t.Fatalf("net.Conn = %q, want nil", c.conn)
	}
}
