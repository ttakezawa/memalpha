package memalpha

import (
	"bufio"
	"bytes"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func fakedClient(response []byte) *Client {
	request := bytes.NewBuffer([]byte{})

	serverReadWriter := bufio.NewReadWriter(
		bufio.NewReader(bytes.NewReader(response)),
		bufio.NewWriter(request),
	)

	return &Client{rw: serverReadWriter}
}

func TestServerError(t *testing.T) {
	errorMessage := "test fake"
	c := fakedClient([]byte("SERVER_ERROR " + errorMessage))

	err := c.Set("foo", []byte("bar"), false)
	e, ok := err.(ServerError)
	if ok && strings.Contains(e.Error(), "server error: "+errorMessage) {
		return
	}

	t.Fatalf("set(foo): Error = %v, want ServerError: test fake", err)
}

func TestClientError(t *testing.T) {
	errorMessage := "test fake"
	c := fakedClient([]byte("CLIENT_ERROR " + errorMessage))

	err := c.Set("foo", []byte("bar"), false)
	e, ok := err.(ClientError)
	if ok && strings.Contains(e.Error(), "client error: "+errorMessage) {
		return
	}

	t.Fatalf("set(foo): Error = %v, want ClientError: test fake", err)
}

func TestReplyError(t *testing.T) {
	c := fakedClient([]byte("ERROR"))

	err := c.Set("foo", []byte("bar"), false)
	assert.Equal(t, ErrReplyError, err)
}

func TestStatsProtocolError(t *testing.T) {
	c := fakedClient([]byte("foobar"))

	_, err := c.Stats()
	assert.Equal(t, ProtocolError("malformed stats response"), err)
}

func TestIncrValueError(t *testing.T) {
	c := fakedClient([]byte("foobar"))

	err := c.Set("foo", []byte("42"), true)
	_, err = c.Increment("foo", 1, false)
	assert.IsType(t, &strconv.NumError{}, err)
}

func TestIllegalVersionResponse(t *testing.T) {
	c := fakedClient([]byte("Illegal Ver 1"))

	_, err := c.Version()
	assert.IsType(t, ProtocolError(""), err)
}
