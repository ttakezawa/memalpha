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
		err = s.client.ensureConnect()
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
}
