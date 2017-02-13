package memalpha

import (
	"bufio"
	"bytes"
	"io"
	"io/ioutil"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func newFakedClient(response string, requestWriter io.Writer) *Client {
	return &Client{rw: bufio.NewReadWriter(
		bufio.NewReader(bytes.NewReader([]byte(response))),
		bufio.NewWriter(requestWriter),
	)}
}

func TestServerError(t *testing.T) {
	errorMessage := "test fake"
	c := newFakedClient("SERVER_ERROR "+errorMessage, ioutil.Discard)

	err := c.Set("foo", []byte("bar"), false)
	e, ok := err.(ServerError)
	if ok && strings.Contains(e.Error(), "server error: "+errorMessage) {
		return
	}

	t.Fatalf("set(foo): Error = %v, want ServerError: test fake", err)
}

func TestClientError(t *testing.T) {
	errorMessage := "test fake"
	c := newFakedClient("CLIENT_ERROR "+errorMessage, ioutil.Discard)

	err := c.Set("foo", []byte("bar"), false)
	e, ok := err.(ClientError)
	if ok && strings.Contains(e.Error(), "client error: "+errorMessage) {
		return
	}

	t.Fatalf("set(foo): Error = %v, want ClientError: test fake", err)
}

func TestReplyError(t *testing.T) {
	c := newFakedClient("ERROR", ioutil.Discard)
	err := c.Set("foo", []byte("bar"), false)
	assert.Equal(t, ErrReplyError, err)
}

func TestMalformedStatsResponse(t *testing.T) {
	{
		c := newFakedClient("foobar", ioutil.Discard)
		_, err := c.Stats()
		assert.Equal(t, ProtocolError("malformed stats response"), err)
	}
}

func TestIncrValueError(t *testing.T) {
	c := newFakedClient("foobar", ioutil.Discard)

	err := c.Set("foo", []byte("42"), true)
	_, err = c.Increment("foo", 1, false)
	assert.IsType(t, &strconv.NumError{}, err)
}

func TestMalformedVersionResponse(t *testing.T) {
	{
		c := newFakedClient("Malformed Ver 1", ioutil.Discard)
		_, err := c.Version()
		assert.IsType(t, ProtocolError(""), err)
	}

	{
		c := newFakedClient("ERROR", ioutil.Discard)
		_, err := c.Version()
		assert.Equal(t, ErrReplyError, err)
	}
}
