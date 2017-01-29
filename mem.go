package memalpha

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
)

// See
// - https://github.com/memcached/memcached/blob/master/doc/protocol.txt
// - https://github.com/youtube/vitess/blob/master/go/memcache/memcache.go

// Client is a memcached client
type Client struct {
	Addr string
	Conn net.Conn
}

// NewClient returns a new Client.
func NewClient(addr string) *Client {
	client := &Client{Addr: addr}
	return client

}

func (c *Client) connect() error {
	tcpAddr, err := net.ResolveTCPAddr("tcp4", c.Addr)
	if err != nil {
		return err
	}

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return err
	}

	c.Conn = conn
	return nil
}

func (c *Client) isConnected() bool {
	return c.Conn != nil
}

func (c *Client) ensureConnect() error {
	if c.isConnected() {
		return nil
	}

	return c.connect()
}

//// Retrieval commands

func (c *Client) sendRetrieveCommand(cmd string, key string) (string, error) {
	err := c.ensureConnect()
	if err != nil {
		return "", err
	}

	_, err = c.Conn.Write([]byte(fmt.Sprintf("%s %s \r\n", cmd, key)))
	if err != nil {
		return "", err
	}

	reader := bufio.NewReader(c.Conn)
	header, isPrefix, err := reader.ReadLine()
	if err != nil {
		return "", err
	}
	if isPrefix {
		return "", errors.New("buffer is not enough")
	}

	// VALUE <key> <flags> <bytes> [<cas unique>]\r\n
	headerChunks := strings.Split(string(header), " ")
	fmt.Printf("debug header: %+v\n", headerChunks) // output for debug
	if len(headerChunks) < 4 {
		return "", fmt.Errorf("Malformed response: %s", string(header))
	}

	if headerChunks[1] != key {
		return "", fmt.Errorf("Malformed response key: %s", string(header))
	}

	flags, err := strconv.ParseUint(headerChunks[2], 10, 16)
	fmt.Printf("debug flags: %+v\n", flags) // output for debug
	if err != nil {
		return "", err
	}

	size, err := strconv.ParseUint(headerChunks[3], 10, 64)
	fmt.Printf("debug size: %+v\n", size) // output for debug
	if err != nil {
		return "", err
	}

	if len(headerChunks) == 5 {
		cas, err2 := strconv.ParseUint(headerChunks[4], 10, 64)
		fmt.Printf("debug cas: %+v\n", cas) // output for debug
		if err2 != nil {
			return "", err2
		}
	}

	buffer := make([]byte, size)
	n, err := io.ReadFull(reader, buffer)
	fmt.Printf("debug n: %+v\n", n) // output for debug
	if err != nil {
		return "", err
	}

	return string(buffer), nil
}

// Get takes one or more keys and returns all found items.
func (c *Client) Get(key string) (string, error) {
	return c.sendRetrieveCommand("get", key)
}

// Gets is an alternative get command for using with CAS.
func (c *Client) Gets(key string) (string, error) {
	return c.sendRetrieveCommand("gets", key)
}

//// Storage commands

func (c *Client) sendCommand(command string, key string, value []byte, flags uint32, exptime int, casid uint64, noreply bool) error {
	err := c.ensureConnect()
	if err != nil {
		return err
	}

	option := ""
	if noreply {
		option = "noreply"
	}

	// Send command: <command> <key> <flags> <exptime> <bytes> [noreply]\r\n
	_, err = c.Conn.Write([]byte(fmt.Sprintf("%s %s %d %d %d %s\r\n", command, key, flags, exptime, len(value), option)))
	if err != nil {
		return err
	}

	// Send data block: <data block>\r\n
	_, err = c.Conn.Write(value)
	if err != nil {
		return err
	}
	_, err = c.Conn.Write([]byte("\r\n"))
	if err != nil {
		return err
	}

	if !noreply {
		// Receive reply
		err = c.receiveReply()
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) receiveReply() error {
	reader := bufio.NewReader(c.Conn)
	reply, isPrefix, err := reader.ReadLine()
	if err != nil {
		return err
	}
	if isPrefix {
		return errors.New("buffer is not enough")
	}

	fmt.Printf("Reply: %+v\n", string(reply)) // output for debug
	return nil
}

// Set key
func (c *Client) Set(key string, value string) error {
	var flags uint32
	exptime := 0
	noreply := false

	err := c.sendCommand("set", key, []byte(value), flags, exptime, 0, noreply)
	return err
}

// Add key
func (c *Client) Add(key string, value string) error {
	var flags uint32
	exptime := 0
	noreply := false

	err := c.sendCommand("add", key, []byte(value), flags, exptime, 0, noreply)
	return err
}

// Replace key
func (c *Client) Replace(key string, value string) error {
	var flags uint32
	exptime := 0
	noreply := false

	err := c.sendCommand("replace", key, []byte(value), flags, exptime, 0, noreply)
	return err
}

// Append key
func (c *Client) Append(key string, value string) error {
	var flags uint32
	exptime := 0
	noreply := false

	err := c.sendCommand("append", key, []byte(value), flags, exptime, 0, noreply)
	return err
}

// Prepend key
func (c *Client) Prepend(key string, value string) error {
	var flags uint32
	exptime := 0
	noreply := false

	err := c.sendCommand("prepend", key, []byte(value), flags, exptime, 0, noreply)
	return err
}

func Cas() {
	// TODO
}

//// Deletion

func Delete() {
	// TODO
}

//// Increment/Decrement

func Increment() {
	// TODO
}

func Decrement() {
	// TODO
}

//// Touch

func Touch() {
	// TODO
}

//// Slabs Reassign (Not Impl)
//// Slabs Automove (Not Impl)
//// LRU_Crawler (Not Impl)
//// Watchers (Not Impl)

//// Statistics

func Stats() {
	// TODO
}

//// General-purpose statistics (Not Impl)
// STAT <name> <value>\r\n

//// Other commands

func FlushAll() {
	// TODO
}

func Version() {
	// TODO
}

func Quit() {
	// TODO
}
