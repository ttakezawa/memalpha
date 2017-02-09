package memalpha

import (
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"testing"
	"time"
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
		err = s.client.ensureConnected()
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

	// Set
	err = c.Set("foo", "fooval")
	if err != nil {
		t.Fatalf("first set(foo): %v", err)
	}
	err = c.Set("foo", "fooval")
	if err != nil {
		t.Fatalf("second set(foo): %v", err)
	}

	// Get
	val, err := c.Get("foo")
	if err != nil {
		t.Fatalf("get(foo): %v", err)
	}
	if val != "fooval" {
		t.Fatalf("get(foo) Value = %q, want fooval", val)
	}

	// Replace
	err = c.Set("foo", "fooval")
	if err != nil {
		t.Fatalf("set(foo): %v", err)
	}
	err = c.Replace("foo", "fooval2")
	if err != nil {
		t.Fatalf("replace(foo): %v", err)
	}
	val, err = c.Get("foo")
	if err != nil {
		t.Fatalf("get(foo): %v", err)
	}
	if val != "fooval2" {
		t.Fatalf("replace(foo, fooval2) then, get(foo) Value = %q, want fooval2", val)
	}

	// Append
	err = c.Append("foo", "suffix")
	if err != nil {
		t.Fatalf("append(foo, suffix): %v", err)
	}
	val, err = c.Get("foo")
	if err != nil {
		t.Fatalf("get(foo): %v", err)
	}
	if val != "fooval2suffix" {
		t.Fatalf("append(foo, suffix) then, get(foo) Value = %q, want fooval2suffix", val)
	}

	// Prepend
	err = c.Prepend("foo", "prefix")
	if err != nil {
		t.Fatalf("prepend(foo, prefix): %v", err)
	}
	val, err = c.Get("foo")
	if err != nil {
		t.Fatalf("get(foo): %v", err)
	}
	if val != "prefixfooval2suffix" {
		t.Fatalf("prepend(foo, prefix) then, get(foo) Value = %q, want prefixfooval2suffix", val)
	}

	// TODO: Test CompareAndSwap

	// Delete
	err = c.Delete("foo", false)
	if err != nil {
		t.Fatalf("delete(foo): %v", err)
	}
	val, err = c.Get("foo")
	if err != ErrCacheMiss {
		t.Fatalf("get(foo) Error = %q, want ErrCacheMiss", val)
	}

	// Increment
	err = c.Set("foo", "35")
	if err != nil {
		t.Fatalf("set(foo): %v", err)
	}
	num, err := c.Increment("foo", 7, false)
	if err != nil {
		t.Fatalf("incr(foo, 7): %v", err)
	}
	if num != 42 {
		t.Fatalf("incr(foo, 7) Value = %q, want 42", num)
	}

	// Decrement
	num, err = c.Decrement("foo", 10, false)
	if err != nil {
		t.Fatalf("decr(foo, 10): %v", err)
	}
	if num != 32 {
		t.Fatalf("decr(foo, 10) Value = %q, want 32", num)
	}
	val, err = c.Get("foo")
	if err != nil {
		t.Fatalf("get(foo): %v", err)
	}
	if val != "32" {
		t.Fatalf("get(foo) Value = %q, want 32", val)
	}

	// Touch
	err = c.Touch("foo", 2, false)
	if err != nil {
		t.Fatalf("touch(foo, 2): %v", err)
	}
	val, err = c.Get("foo")
	if err != nil {
		t.Fatalf("get(foo): %v", err)
	}
	if val != "32" {
		t.Fatalf("get(foo) Value = %q, want 32", val)
	}
	time.Sleep(2 * time.Second)
	val, err = c.Get("foo")
	if err != ErrCacheMiss {
		t.Fatalf("get(foo) Error = %q, want ErrCacheMiss", val)
	}

	// Stats
	m, err := c.Stats()
	if err != nil {
		t.Fatalf("stats(): %v", err)
	}
	if len(m) < 2 {
		t.Fatalf("stats(): len(Value) = %q, want len(value) > 2", m)
	}

	// FlushAll
	err = c.Set("foo", "bar")
	if err != nil {
		t.Fatalf("set(foo): %v", err)
	}
	err = c.FlushAll(0, false)
	if err != nil {
		t.Fatalf("version(): %v", err)
	}
	val, err = c.Get("foo")
	if err != ErrCacheMiss {
		t.Fatalf("get(foo) Error = %q, want ErrCacheMiss", val)
	}

	// Version
	val, err = c.Version()
	if err != nil {
		t.Fatalf("version(): %v", err)
	}
	if len(val) == 0 {
		t.Fatalf("version() Value = %q, want len(value) > 0", val)
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
