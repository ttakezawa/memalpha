package memdtest

import (
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"time"

	"github.com/ttakezawa/memalpha"
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
	Dial func(addr string) (memalpha.Conn, error)
	cmd  *exec.Cmd
	Addr string
	Conn memalpha.Conn
}

func NewServer(dial func(addr string) (memalpha.Conn, error)) *server {
	return &server{Dial: dial}
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
		addr := fmt.Sprintf("localhost:%d", port)
		s.Conn, err = s.Dial(addr)
		if err == nil {
			s.Addr = addr
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
