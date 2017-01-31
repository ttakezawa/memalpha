package memalpha

import (
	"bufio"
	"bytes"
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
)

var (
	replyStored    = []byte("STORED")
	replyNotStored = []byte("NOT_STORED")
	replyExists    = []byte("EXISTS")
	replyNotFound  = []byte("NOT_FOUND")
	replyDeleted   = []byte("DELETED")
)

var (
	responseEnd = []byte("END")
)

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

	if bytes.Equal(header, responseEnd) {
		return "", ErrCacheMiss
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

func (c *Client) sendStorageCommand(command string, key string, value []byte, flags uint32, exptime int, casid uint64, noreply bool) error {
	err := c.ensureConnect()
	if err != nil {
		return err
	}

	option := ""
	if noreply {
		option = "noreply"
	}

	if command == "cas" {
		// Send command: cas       <key> <flags> <exptime> <bytes> <cas unique> [noreply]\r\n
		_, err = c.Conn.Write([]byte(fmt.Sprintf("%s %s %d %d %d %d %s\r\n", command, key, flags, exptime, len(value), casid, option)))
	} else {
		// Send command: <command> <key> <flags> <exptime> <bytes> [noreply]\r\n
		_, err = c.Conn.Write([]byte(fmt.Sprintf("%s %s %d %d %d %s\r\n", command, key, flags, exptime, len(value), option)))
	}
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
		reply, err := c.receiveReply()
		if err != nil {
			return err
		}
		switch {
		case bytes.Equal(reply, replyStored):
			return nil
		case bytes.Equal(reply, replyExists):
			return ErrCasConflict
		case bytes.Equal(reply, replyNotStored):
			return ErrNotStored
		case bytes.Equal(reply, replyNotFound):
			return ErrNotFound
		}
		return errors.New(string(reply))
	}

	return nil
}

func (c *Client) receiveReply() ([]byte, error) {
	reader := bufio.NewReader(c.Conn)
	reply, isPrefix, err := reader.ReadLine()
	if err != nil {
		return nil, err
	}
	if isPrefix {
		return nil, errors.New("buffer is not enough")
	}

	fmt.Printf("Reply: %+v\n", string(reply)) // output for debug

	return reply, nil
}

// Set key
func (c *Client) Set(key string, value string) error {
	var flags uint32
	exptime := 0
	noreply := false

	err := c.sendStorageCommand("set", key, []byte(value), flags, exptime, 0, noreply)
	return err
}

// Add key
func (c *Client) Add(key string, value string) error {
	var flags uint32
	exptime := 0
	noreply := false

	err := c.sendStorageCommand("add", key, []byte(value), flags, exptime, 0, noreply)
	return err
}

// Replace key
func (c *Client) Replace(key string, value string) error {
	var flags uint32
	exptime := 0
	noreply := false

	err := c.sendStorageCommand("replace", key, []byte(value), flags, exptime, 0, noreply)
	return err
}

// Append key
func (c *Client) Append(key string, value string) error {
	var flags uint32
	exptime := 0
	noreply := false

	err := c.sendStorageCommand("append", key, []byte(value), flags, exptime, 0, noreply)
	return err
}

// Prepend key
func (c *Client) Prepend(key string, value string) error {
	var flags uint32
	exptime := 0
	noreply := false

	err := c.sendStorageCommand("prepend", key, []byte(value), flags, exptime, 0, noreply)
	return err
}

func (c *Client) CompareAndSwap(key string, value string, casid uint64) error {
	var flags uint32
	exptime := 0
	noreply := false

	err := c.sendStorageCommand("cas", key, []byte(value), flags, exptime, casid, noreply)
	return err
}

//// Deletion

// Delete deletes the item with the provided key
func (c *Client) Delete(key string, noreply bool) error {
	err := c.ensureConnect()
	if err != nil {
		return err
	}

	option := ""
	if noreply {
		option = "noreply"
	}

	// delete <key> [noreply]\r\n
	_, err = c.Conn.Write([]byte(fmt.Sprintf("delete %s %s\r\n", key, option)))
	if err != nil {
		return err
	}

	if !noreply {
		// Receive reply
		reply, err := c.receiveReply()
		if err != nil {
			return err
		}
		switch {
		case bytes.Equal(reply, replyDeleted):
			return nil
		case bytes.Equal(reply, replyNotFound):
			return ErrNotFound
		}
		return errors.New(string(reply))
	}

	return nil
}

//// Increment/Decrement

// Increment key by value. value is the amount by which the client wants to increase
// the item. It is a decimal representation of a 64-bit unsigned integer. The return
// value is the new value. If noreply is true, the return value is always 0.
func (c *Client) Increment(key string, value uint64, noreply bool) (uint64, error) {
	err := c.ensureConnect()
	if err != nil {
		return 0, err
	}

	option := ""
	if noreply {
		option = "noreply"
	}

	// incr <key> <value> [noreply]\r\n
	_, err = c.Conn.Write([]byte(fmt.Sprintf("incr %s %d %s\r\n", key, value, option)))
	if err != nil {
		return 0, err
	}

	if !noreply {
		// Receive reply
		reply, err1 := c.receiveReply()
		if err1 != nil {
			return 0, err1
		}
		switch {
		case bytes.Equal(reply, replyNotFound):
			return 0, ErrNotFound
			// TODO: case ERROR, CLIENT_ERROR, SERVER_ERROR
		}
		newValue, err1 := strconv.ParseUint(string(reply), 10, 64)
		if err1 != nil {
			return 0, err1
		}
		return newValue, nil
	}

	return 0, nil
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
