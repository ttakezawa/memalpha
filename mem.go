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

var (
	replyStored            = []byte("STORED")
	replyNotStored         = []byte("NOT_STORED")
	replyExists            = []byte("EXISTS")
	replyNotFound          = []byte("NOT_FOUND")
	replyDeleted           = []byte("DELETED")
	replyTouched           = []byte("TOUCHED")
	replyOk                = []byte("OK")
	replyError             = []byte("ERROR")
	replyClientErrorPrefix = []byte("CLIENT_ERROR ")
	replyServerErrorPrefix = []byte("SERVER_ERROR ")
)

var (
	responseEnd = []byte("END")
)

var (
	bytesCrlf     = []byte("\r\n")
	bytesVersion  = []byte("VERSION ")
	optionNoreply = "noreply"
)

// Client is a memcached client
type Client struct {
	Addr string
	conn net.Conn
	rw   *bufio.ReadWriter
	err  error
}

// Response is a response of get
type Response struct {
	Value []byte
	Flags uint32
	CasID uint64
}

// NewClient returns a new Client.
func NewClient(addr string) *Client {
	client := &Client{Addr: addr}
	return client
}

// Close a connection.
func (c *Client) Close() error {
	if c.conn == nil {
		return nil
	}

	err := c.conn.Close()
	c.rw = nil
	c.conn = nil
	return err
}

func (c *Client) ensureConnected() {
	if c.err != nil {
		return
	}

	if c.rw != nil {
		return
	}

	conn, err := net.Dial("tcp", c.Addr)
	if err != nil {
		c.err = err
		return
	}

	c.conn = conn
	c.rw = bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
}

func (c *Client) readLine() []byte {
	if c.err != nil {
		return nil
	}

	var line, next []byte
	var isPrefix = true

	for isPrefix && c.err == nil {
		next, isPrefix, c.err = c.rw.ReadLine()
		line = append(line, next...)
	}
	return line
}

func (c *Client) write(p []byte) {
	if c.err != nil {
		return
	}
	_, c.err = c.rw.Write(p)
}

func (c *Client) flush() {
	if c.err != nil {
		return
	}
	c.err = c.rw.Flush()
}

// Err results in clearing c.err
func (c *Client) Err() error {
	err := c.err
	c.err = nil
	return err
}

func (c *Client) receiveReply() []byte {
	if c.err != nil {
		return nil
	}
	return c.readLine()
}

func (c *Client) checkReply(reply []byte) (ok bool) {
	if c.err != nil {
		return false
	}

	switch {
	case bytes.Equal(reply, replyStored), bytes.Equal(reply, replyDeleted), bytes.Equal(reply, replyTouched), bytes.Equal(reply, replyOk):
		return true
	case bytes.Equal(reply, replyExists):
		c.err = ErrCasConflict
	case bytes.Equal(reply, replyNotStored):
		c.err = ErrNotStored
	case bytes.Equal(reply, replyNotFound):
		c.err = ErrNotFound
	case bytes.Equal(reply, replyError):
		c.err = ErrReplyError
	case bytes.HasPrefix(reply, replyClientErrorPrefix):
		c.err = ClientError(reply[len(replyClientErrorPrefix):])
	case bytes.HasPrefix(reply, replyServerErrorPrefix):
		c.err = ServerError(reply[len(replyServerErrorPrefix):])
	}
	return false
}

func (c *Client) receiveCheckReply() {
	reply := c.receiveReply()
	ok := c.checkReply(reply)

	if c.err == nil && !ok {
		c.err = ProtocolError(fmt.Sprintf("unknown reply type: %s", string(reply)))
	}
}

//// Retrieval commands

func (c *Client) sendRetrieveCommand(cmd string, key string) {
	c.ensureConnected()

	c.write([]byte(fmt.Sprintf("%s %s\r\n", cmd, key)))
	c.flush()
}

// returns key, value, casId, flags, err
func (c *Client) receiveGetResponse() (string, *Response) {
	header := c.readLine()
	if c.err != nil {
		return "", nil
	}
	if bytes.Equal(header, responseEnd) {
		c.err = ErrCacheMiss
		return "", nil
	}

	response := &Response{}
	key, size, err := c.parseGetResponseHeader(header, response)
	if err != nil {
		c.err = err
		return "", nil
	}

	body, err := c.receiveGetResponseBody(size)
	if err != nil {
		c.err = err
		return "", nil
	}
	response.Value = body[:size]

	return key, response
}

func (c *Client) parseGetResponseHeader(header []byte, response *Response) (key string, size uint64, err error) {
	// VALUE <key> <flags> <bytes> [<cas unique>]\r\n
	headerChunks := strings.Split(string(header), " ")
	debugf("debug header: %+v\n", headerChunks) // output for debug
	if len(headerChunks) < 4 {
		return "", 0, ProtocolError(fmt.Sprintf("malformed response: %#v", string(header)))
	}

	key = headerChunks[1]

	flags, err := strconv.ParseUint(headerChunks[2], 10, 32)
	debugf("debug flags: %+v\n", flags) // output for debug
	if err != nil {
		return "", 0, err
	}
	response.Flags = uint32(flags)

	size, err = strconv.ParseUint(headerChunks[3], 10, 64)
	debugf("debug size: %+v\n", size) // output for debug
	if err != nil {
		return "", 0, err
	}

	if len(headerChunks) == 5 {
		response.CasID, err = strconv.ParseUint(headerChunks[4], 10, 64)
		debugf("debug cas: %+v\n", response.CasID) // output for debug
		if err != nil {
			return "", 0, err
		}
	}

	return key, size, nil
}

func (c *Client) receiveGetResponseBody(size uint64) ([]byte, error) {
	buffer := make([]byte, size+2)
	n, err := io.ReadFull(c.rw, buffer)
	debugf("debug n: %+v\n", n) // output for debug
	if err != nil {
		return nil, err
	}

	// Check \r\n
	if !bytes.HasSuffix(buffer, bytesCrlf) {
		return nil, ProtocolError("malformed response: corrupt get result end")
	}

	return buffer, nil
}

// Get returns a value, flags and error.
func (c *Client) Get(key string) (value []byte, flags uint32, err error) {
	c.sendRetrieveCommand("get", key)

	_, response := c.receiveGetResponse()

	// Confirm END
	endLine := c.readLine()
	if err = c.Err(); err != nil {
		return nil, 0, err
	}
	if !bytes.Equal(endLine, responseEnd) {
		return nil, 0, ProtocolError("malformed response: corrupt get result end")
	}

	return response.Value, response.Flags, nil
}

// Gets is an alternative get command for using with CAS.
func (c *Client) Gets(keys []string) (map[string]*Response, error) {
	c.sendRetrieveCommand("gets", strings.Join(keys, " "))

	m := make(map[string]*Response)
	for {
		key, response := c.receiveGetResponse()
		if err := c.Err(); err != nil {
			if err == ErrCacheMiss {
				break
			}
			return nil, err
		}
		m[key] = response
	}

	return m, nil
}

//// Storage commands

func (c *Client) sendStorageCommand(command string, key string, value []byte, flags uint32, exptime int, casid uint64, noreply bool) error {
	c.ensureConnected()

	option := ""
	if noreply {
		option = "noreply"
	}

	if command == "cas" {
		// Send command: cas       <key> <flags> <exptime> <bytes> <cas unique> [noreply]\r\n
		c.write([]byte(fmt.Sprintf("%s %s %d %d %d %d %s\r\n", command, key, flags, exptime, len(value), casid, option)))
	} else {
		// Send command: <command> <key> <flags> <exptime> <bytes> [noreply]\r\n
		c.write([]byte(fmt.Sprintf("%s %s %d %d %d %s\r\n", command, key, flags, exptime, len(value), option)))
	}

	// Send data block: <data block>\r\n
	c.write(value)
	c.write(bytesCrlf)
	c.flush()

	if !noreply {
		c.receiveCheckReply()
	}

	return c.Err()
}

// Set key
func (c *Client) Set(key string, value []byte, noreply bool) error {
	var flags uint32
	exptime := 0
	return c.sendStorageCommand("set", key, value, flags, exptime, 0, noreply)
}

// Add key
func (c *Client) Add(key string, value []byte) error {
	var flags uint32
	exptime := 0
	noreply := false

	return c.sendStorageCommand("add", key, value, flags, exptime, 0, noreply)
}

// Replace key
func (c *Client) Replace(key string, value []byte) error {
	var flags uint32
	exptime := 0
	noreply := false

	return c.sendStorageCommand("replace", key, value, flags, exptime, 0, noreply)
}

// Append key
func (c *Client) Append(key string, value []byte) error {
	var flags uint32
	exptime := 0
	noreply := false

	return c.sendStorageCommand("append", key, value, flags, exptime, 0, noreply)
}

// Prepend key
func (c *Client) Prepend(key string, value []byte) error {
	var flags uint32
	exptime := 0
	noreply := false

	return c.sendStorageCommand("prepend", key, value, flags, exptime, 0, noreply)
}

// CompareAndSwap is a check and set operation which means "store this data but only if no
// one else has updated since I last fetched it."
func (c *Client) CompareAndSwap(key string, value []byte, casid uint64) error {
	var flags uint32
	exptime := 0
	noreply := false

	return c.sendStorageCommand("cas", key, value, flags, exptime, casid, noreply)
}

//// Deletion

// Delete deletes the item with the provided key
func (c *Client) Delete(key string, noreply bool) error {
	c.ensureConnected()

	option := ""
	if noreply {
		option = optionNoreply
	}

	// delete <key> [noreply]\r\n
	c.write([]byte(fmt.Sprintf("delete %s %s\r\n", key, option)))
	c.flush()

	if !noreply {
		// Receive reply
		c.receiveCheckReply()
	}

	return c.Err()
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
	c.ensureConnected()

	option := ""
	if noreply {
		option = optionNoreply
	}

	// <incr|decr> <key> <value> [noreply]\r\n
	c.write([]byte(fmt.Sprintf("%s %s %d %s\r\n", command, key, value, option)))
	c.flush()

	if noreply {
		return 0, c.Err()
	}

	// Receive reply
	reply := c.receiveReply()
	c.checkReply(reply)
	if err := c.Err(); err != nil {
		return 0, err
	}

	// Calculate a new value from reply.
	newValue, err := strconv.ParseUint(string(reply), 10, 64)
	if err != nil {
		return 0, err
	}
	return newValue, nil
}

//// Touch

// Touch is used to update the expiration time of an existing item without fetching it.
func (c *Client) Touch(key string, exptime int32, noreply bool) error {
	c.ensureConnected()

	option := ""
	if noreply {
		option = "noreply"
	}

	// touch <key> <exptime> [noreply]\r\n
	c.write([]byte(fmt.Sprintf("touch %s %d %s\r\n", key, exptime, option)))
	c.flush()

	if noreply {
		return c.Err()
	}

	// Recieve reply
	c.receiveCheckReply()
	return c.Err()
}

//// Slabs Reassign (Not Impl)
//// Slabs Automove (Not Impl)
//// LRU_Crawler (Not Impl)
//// Watchers (Not Impl)

//// Statistics

// Stats returns a map of stats.
func (c *Client) Stats() (map[string]string, error) {
	return c.stats([]byte("stats\r\n"))
}

// StatsArg returns a map of stats. Depending on <args>, various internal data is sent by
// the server.
func (c *Client) StatsArg(argument string) (map[string]string, error) {
	return c.stats([]byte(fmt.Sprintf("stats %s\r\n", argument)))
}

func (c *Client) stats(command []byte) (map[string]string, error) {
	c.ensureConnected()

	// Send command: stats\r\n
	c.write(command)
	c.flush()

	m := make(map[string]string)
	for {
		line := c.readLine()
		if err := c.Err(); err != nil {
			return nil, err
		}
		if bytes.Equal(line, responseEnd) {
			return m, nil
		}
		if !bytes.HasPrefix(line, []byte("STAT ")) {
			return nil, ProtocolError("malformed stats response")
		}

		data := bytes.SplitN(line[5:], []byte(" "), 3)
		m[string(data[0])] = string(data[1])
	}
}

//// Other commands

// FlushAll invalidates all existing items immediately (by default) or after the delay
// specified. If delay is < 0, it ignores the delay.
func (c *Client) FlushAll(delay int, noreply bool) error {
	c.ensureConnected()

	option := ""
	if noreply {
		option = optionNoreply
	}

	// flush_all [delay] [noreply]\r\n
	if delay >= 0 {
		c.write([]byte(fmt.Sprintf("flush_all %d %s\r\n", delay, option)))
	} else {
		c.write([]byte(fmt.Sprintf("flush_all %s\r\n", option)))
	}
	c.flush()

	if noreply {
		return c.Err()
	}

	// Recieve reply
	c.receiveCheckReply()
	return c.Err()
}

// Version returns the version of memcached server
func (c *Client) Version() (string, error) {
	c.ensureConnected()

	// version\r\n
	// NOTE: noreply option is not allowed.
	c.write([]byte("version\r\n"))
	c.flush()

	// Receive reply
	reply := c.receiveReply()
	c.checkReply(reply)
	if err := c.Err(); err != nil {
		return "", err
	}

	if bytes.HasPrefix(reply, bytesVersion) {
		// "VERSION " is 8 chars.
		return string(reply[len(bytesVersion):]), nil
	}
	return "", ProtocolError(fmt.Sprintf("unknown reply type: %s", string(reply)))
}

// Quit closes the connection to memcached server
func (c *Client) Quit() error {
	c.ensureConnected()

	// quit\r\n
	// NOTE: noreply option is not allowed.
	c.write([]byte("quit\r\n"))
	c.flush()
	return c.Err()
}
