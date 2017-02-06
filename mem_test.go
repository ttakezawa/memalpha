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

func startMemcached(cb func(*Client)) error {
	port, err := freePort()
	if err != nil {
		return err
	}

	cmd := exec.Command("memcached", "-p", strconv.Itoa(port))
	if err = cmd.Start(); err != nil {
		return err
	}
	defer cmd.Wait()
	defer cmd.Process.Kill()

	client := NewClient(fmt.Sprintf("localhost:%d", port))

	// Wait a bit for the socket to appear.
	for i := 0; i < 10; i++ {
		if err := client.connect(); err == nil {
			break
		}
		time.Sleep(time.Duration(25*i) * time.Millisecond)
	}

	cb(client)

	return nil
}

func TestLocalhost(t *testing.T) {
	err := startMemcached(func(c *Client) {
		testSuite(t, c)
	})

	if err != nil {
		t.Skipf("skipping test; couldn't start memcached: %s", err)
	}
}

func testSuite(t *testing.T, c *Client) {
	checkErr := func(err error, format string, args ...interface{}) {
		if err != nil {
			t.Fatalf(format, args...)
		}
	}

	// Set
	err := c.Set("foo", "fooval")
	checkErr(err, "first set(foo): %v", err)
	err = c.Set("foo", "fooval")
	checkErr(err, "second set(foo): %v", err)

	// Get
	val, err := c.Get("foo")
	checkErr(err, "get(foo): %v", err)
	if val != "fooval" {
		t.Errorf("get(foo) Value = %q, want fooval", val)
	}
}
