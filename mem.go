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
	replyTouched   = []byte("TOUCHED")
	replyOk        = []byte("OK")
)

var (
	responseEnd = []byte("END")
)

var (
	bytesCrlf     = []byte("\r\n")
	optionNoreply = "noreply"
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

func (c *Client) sendRetrieveCommand(cmd string, key string) error {
	err := c.ensureConnect()
	if err != nil {
		return err
	}

	_, err = c.Conn.Write([]byte(fmt.Sprintf("%s %s \r\n", cmd, key)))
	return err
}

// returns key, value, err
func (c *Client) receiveGetResponse(reader *bufio.Reader) (string, string, error) {
	header, isPrefix, err := reader.ReadLine()
	if err != nil {
		return "", "", err
	}
	if isPrefix {
		return "", "", errors.New("buffer is not enough")
	}
	if bytes.Equal(header, responseEnd) {
		return "", "", ErrCacheMiss
	}

	// VALUE <key> <flags> <bytes> [<cas unique>]\r\n
	headerChunks := strings.Split(string(header), " ")
	fmt.Printf("debug header: %+v\n", headerChunks) // output for debug
	if len(headerChunks) < 4 {
		return "", "", fmt.Errorf("Malformed response: %#v", string(header))
	}

	key := headerChunks[1]

	flags, err := strconv.ParseUint(headerChunks[2], 10, 16)
	fmt.Printf("debug flags: %+v\n", flags) // output for debug
	if err != nil {
		return "", "", err
	}

	size, err := strconv.ParseUint(headerChunks[3], 10, 64)
	fmt.Printf("debug size: %+v\n", size) // output for debug
	if err != nil {
		return "", "", err
	}

	if len(headerChunks) == 5 {
		cas, err2 := strconv.ParseUint(headerChunks[4], 10, 64)
		fmt.Printf("debug cas: %+v\n", cas) // output for debug
		if err2 != nil {
			return "", "", err2
		}
	}

	buffer, err := c.receiveGetPayload(reader, size)
	if err != nil {
		return "", "", err
	}

	return key, string(buffer[:size]), nil
}

func (c *Client) receiveGetPayload(reader io.Reader, size uint64) ([]byte, error) {
	buffer := make([]byte, size+2)
	n, err := io.ReadFull(reader, buffer)
	fmt.Printf("debug n: %+v\n", n) // output for debug
	if err != nil {
		return nil, err
	}

	// Check \r\n
	if !bytes.HasSuffix(buffer, bytesCrlf) {
		return nil, errors.New("Malformed response: currupt get result end")
	}

	return buffer, nil
}

// Get takes one or more keys and returns all found items.
func (c *Client) Get(key string) (string, error) {
	err := c.sendRetrieveCommand("get", key)
	if err != nil {
		return "", err
	}

	reader := bufio.NewReader(c.Conn)
	_, value, err := c.receiveGetResponse(reader)

	endLine, isPrefix, err := reader.ReadLine()
	if err != nil {
		return "", err
	}
	if isPrefix {
		return "", errors.New("buffer is not enough")
	}
	if !bytes.Equal(endLine, responseEnd) {
		return "", errors.New("Malformed response: currupt get result end")
	}

	return value, err
}

// Gets is an alternative get command for using with CAS.
func (c *Client) Gets(keys []string) (map[string]string, error) {
	err := c.sendRetrieveCommand("gets", strings.Join(keys, " "))
	if err != nil {
		return nil, err
	}

	m := make(map[string]string)
	reader := bufio.NewReader(c.Conn)
	for {
		key, value, err1 := c.receiveGetResponse(reader)
		if err1 != nil {
			if err1 == ErrCacheMiss {
				break
			}
			return nil, err1
		}
		m[key] = value
	}

	return m, nil
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
	_, err = c.Conn.Write(bytesCrlf)
	if err != nil {
		return err
	}

	if noreply {
		return nil
	}

	return c.receiveReplyToStorageCommand()
}

func (c *Client) receiveReplyToStorageCommand() error {
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

// CompareAndSwap is a check and set operation which means "store this data but only if no
// one else has updated since I last fetched it."
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
		option = optionNoreply
	}

	// delete <key> [noreply]\r\n
	_, err = c.Conn.Write([]byte(fmt.Sprintf("delete %s %s\r\n", key, option)))
	if err != nil {
		return err
	}

	if !noreply {
		// Receive reply
		reply, err1 := c.receiveReply()
		if err1 != nil {
			return err1
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
// Note that Overflow in the "incr" command will wrap around the 64 bit mark.
func (c *Client) Increment(key string, value uint64, noreply bool) (uint64, error) {
	return c.executeIncrDecrCommand("incr", key, value, noreply)
}

// Decrement key by value. value is the amount by which the client wants to decrease
// the item. It is a decimal representation of a 64-bit unsigned integer. The return
// value is the new value. If noreply is true, the return value is always 0.
// Note that underflow in the "decr" command is caught: if a client tries to decrease
// the value below 0, the new value will be 0.
func (c *Client) Decrement(key string, value uint64, noreply bool) (uint64, error) {
	return c.executeIncrDecrCommand("decr", key, value, noreply)
}

func (c *Client) executeIncrDecrCommand(command string, key string, value uint64, noreply bool) (uint64, error) {
	err := c.ensureConnect()
	if err != nil {
		return 0, err
	}

	option := ""
	if noreply {
		option = optionNoreply
	}

	// <incr|decr> <key> <value> [noreply]\r\n
	_, err = c.Conn.Write([]byte(fmt.Sprintf("%s %s %d %s\r\n", command, key, value, option)))
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

//// Touch

// Touch is used to update the expiration time of an existing item without fetching it.
func (c *Client) Touch(key string, exptime int32, noreply bool) error {
	err := c.ensureConnect()
	if err != nil {
		return err
	}

	option := ""
	if noreply {
		option = "noreply"
	}

	// touch <key> <exptime> [noreply]\r\n
	_, err = c.Conn.Write([]byte(fmt.Sprintf("touch %s %d %s\r\n", key, exptime, option)))
	if err != nil {
		return err
	}

	if !noreply {
		// Recieve reply
		reply, err1 := c.receiveReply()
		if err1 != nil {
			return err1
		}
		switch {
		case bytes.Equal(reply, replyTouched):
			return nil
		case bytes.Equal(reply, replyNotFound):
			return ErrNotFound
			// TODO: case ERROR, CLIENT_ERROR, SERVER_ERROR
		}
		return errors.New(string(reply))
	}

	return nil
}

//// Slabs Reassign (Not Impl)
//// Slabs Automove (Not Impl)
//// LRU_Crawler (Not Impl)
//// Watchers (Not Impl)

//// Statistics

// Stats returns a map of stats.
func (c *Client) Stats() (map[string]string, error) {
	err := c.ensureConnect()
	if err != nil {
		return nil, err
	}

	// Send command: stats\r\n
	_, err = c.Conn.Write([]byte("stats\r\n"))
	if err != nil {
		return nil, err
	}

	m := make(map[string]string)
	reader := bufio.NewReader(c.Conn)
	for {
		line, isPrefix, err1 := reader.ReadLine()
		if err1 != nil {
			return nil, err1
		}
		if isPrefix {
			return nil, errors.New("buffer is not enough")
		}
		if bytes.Equal(line, responseEnd) {
			return m, nil
		}
		if !bytes.HasPrefix(line, []byte("STAT ")) {
			return nil, errors.New("Malformed stats response")
		}

		data := bytes.SplitN(line[5:], []byte(" "), 3)
		m[string(data[0])] = string(data[1])
	}
}

//// Other commands

// FlushAll invalidates all existing items immediately (by default) or after the delay
// specified. If delay is < 0, it ignores the delay.
func (c *Client) FlushAll(delay int, noreply bool) error {
	err := c.ensureConnect()
	if err != nil {
		return err
	}

	option := ""
	if noreply {
		option = optionNoreply
	}

	// flush_all [delay] [noreply]\r\n
	if delay >= 0 {
		_, err = c.Conn.Write([]byte(fmt.Sprintf("flush_all %d %s\r\n", delay, option)))
	} else {
		_, err = c.Conn.Write([]byte(fmt.Sprintf("flush_all %s\r\n", option)))
	}
	if err != nil {
		return err
	}

	if !noreply {
		// Recieve reply
		reply, err1 := c.receiveReply()
		if err1 != nil {
			return err1
		}
		switch {
		case bytes.Equal(reply, replyOk):
			return nil
			// TODO: case ERROR, CLIENT_ERROR, SERVER_ERROR
		}
		return errors.New(string(reply))
	}

	return nil
}

// Version returns the version of memcached server
func (c *Client) Version() (string, error) {
	err := c.ensureConnect()
	if err != nil {
		return "", err
	}

	// version\r\n
	// NOTE: noreply option is not allowed.
	_, err = c.Conn.Write([]byte("version\r\n"))
	if err != nil {
		return "", err
	}

	// Receive reply
	reply, err1 := c.receiveReply()
	if err1 != nil {
		return "", err1
	}

	if bytes.HasPrefix(reply, []byte("VERSION ")) {
		// "VERSION " is 8 chars.
		return string(reply[8:]), nil
	}

	// TODO: error handling
	return "", errors.New(string(reply))
}

// Quit closes the connection to memcached server
func (c *Client) Quit() error {
	err := c.ensureConnect()
	if err != nil {
		return err
	}

	// quit\r\n
	// NOTE: noreply option is not allowed.
	_, err = c.Conn.Write([]byte("quit\r\n"))
	return err
}
