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

//// Retrieval commands

func Get(key string) (string, error) {
	service := "127.0.0.1:11211"

	tcpAddr, err := net.ResolveTCPAddr("tcp4", service)
	if err != nil {
		return "", err
	}

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return "", err
	}

	_, err = conn.Write([]byte(fmt.Sprintf("get %s \r\n", key)))
	if err != nil {
		return "", err
	}

	reader := bufio.NewReader(conn)
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

func Gets() {
	// TODO
}

//// Storage commands

// Set key
func Set(key string, value string) error {
	// TODO
	return nil
}

func Add() {
	// TODO
}

func Replace() {
	// TODO
}

func Append() {
	// TODO
}

func Cas() {
	// TODO
}

func Prepend() {
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
