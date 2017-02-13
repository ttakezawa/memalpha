package memalpha

import (
	"bufio"
	"bytes"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestServerError(t *testing.T) {
	errorMessage := "test fake"
	response := bytes.NewReader([]byte("SERVER_ERROR " + errorMessage))
	request := bytes.NewBuffer([]byte{})

	serverReadWriter := bufio.NewReadWriter(bufio.NewReader(response), bufio.NewWriter(request))

	c := &Client{rw: serverReadWriter}

	err := c.Set("foo", []byte("bar"), false)
	e, ok := err.(ServerError)
	if ok && strings.Contains(e.Error(), "server error: "+errorMessage) {
		return
	}

	t.Fatalf("set(foo): Error = %v, want ServerError: test fake", err)
}

func TestClientError(t *testing.T) {
	errorMessage := "test fake"
	response := bytes.NewReader([]byte("CLIENT_ERROR " + errorMessage))
	request := bytes.NewBuffer([]byte{})

	serverReadWriter := bufio.NewReadWriter(bufio.NewReader(response), bufio.NewWriter(request))

	c := &Client{rw: serverReadWriter}

	err := c.Set("foo", []byte("bar"), false)
	e, ok := err.(ClientError)
	if ok && strings.Contains(e.Error(), "client error: "+errorMessage) {
		return
	}

	t.Fatalf("set(foo): Error = %v, want ClientError: test fake", err)
}

func TestReplyError(t *testing.T) {
	response := bytes.NewReader([]byte("ERROR"))
	request := bytes.NewBuffer([]byte{})

	serverReadWriter := bufio.NewReadWriter(bufio.NewReader(response), bufio.NewWriter(request))

	c := &Client{rw: serverReadWriter}

	err := c.Set("foo", []byte("bar"), false)
	assert.Equal(t, ErrReplyError, err)
}

func TestStatsProtocolError(t *testing.T) {
	response := bytes.NewReader([]byte("foobar"))
	request := bytes.NewBuffer([]byte{})

	serverReadWriter := bufio.NewReadWriter(bufio.NewReader(response), bufio.NewWriter(request))

	c := &Client{rw: serverReadWriter}

	_, err := c.Stats()
	assert.Equal(t, ProtocolError("malformed stats response"), err)
}

func TestIncrValueError(t *testing.T) {
	response := bytes.NewReader([]byte("foobar"))
	request := bytes.NewBuffer([]byte{})

	serverReadWriter := bufio.NewReadWriter(bufio.NewReader(response), bufio.NewWriter(request))

	c := &Client{rw: serverReadWriter}

	err := c.Set("foo", []byte("42"), true)
	_, err = c.Increment("foo", 1, false)
	assert.IsType(t, &strconv.NumError{}, err)
}
