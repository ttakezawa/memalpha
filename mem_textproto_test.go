package memalpha

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

type errorWriter struct{ error }

func (e errorWriter) Write(p []byte) (int, error) {
	return 0, e.error
}

type errorReader struct{ error }

func (e errorReader) Read(p []byte) (int, error) {
	return 0, e.error
}

func newFakedClient(response string, requestWriter io.Writer) *Client {
	return &Client{rw: bufio.NewReadWriter(
		bufio.NewReader(bytes.NewReader([]byte(response))),
		bufio.NewWriter(requestWriter),
	)}
}

func TestServerError(t *testing.T) {
	errorMessage := "test fake"
	c := newFakedClient("SERVER_ERROR "+errorMessage, ioutil.Discard)

	err := c.Set("foo", []byte("bar"), 0, 0, false)
	e, ok := err.(ServerError)
	if ok && strings.Contains(e.Error(), "server error: "+errorMessage) {
		return
	}

	t.Fatalf("set(foo): Error = %v, want ServerError: test fake", err)
}

func TestClientError(t *testing.T) {
	errorMessage := "test fake"
	c := newFakedClient("CLIENT_ERROR "+errorMessage, ioutil.Discard)

	err := c.Set("foo", []byte("bar"), 0, 0, false)
	e, ok := err.(ClientError)
	if ok && strings.Contains(e.Error(), "client error: "+errorMessage) {
		return
	}

	t.Fatalf("set(foo): Error = %v, want ClientError: test fake", err)
}

func TestReplyError(t *testing.T) {
	c := newFakedClient("ERROR", ioutil.Discard)
	err := c.Set("foo", []byte("bar"), 0, 0, false)
	assert.Equal(t, ErrReplyError, err)
}

func TestMalformedGetResponse(t *testing.T) {
	{
		c := newFakedClient("foobar", ioutil.Discard)
		_, _, err := c.Get("foo")
		got := err.Error()
		expected := "malformed response"
		comparison := func() bool {
			return strings.Contains(got, expected)
		}
		assert.Condition(t, comparison, fmt.Sprintf("%q should have prefix %q", got, expected))
	}

	{
		// Network Error by read
		expected := net.UnknownNetworkError("test")
		c := &Client{rw: bufio.NewReadWriter(
			bufio.NewReader(errorReader{expected}),
			bufio.NewWriter(ioutil.Discard),
		)}
		_, _, err := c.Get("foo")
		assert.Equal(t, expected, err)
	}

	{
		// Malformed CasID
		c := newFakedClient("VALUE foo 0 6 foo\r\nfoobar\r\nEND", ioutil.Discard)
		_, _, err := c.Get("foo")
		assert.IsType(t, &strconv.NumError{}, err)
	}

	{
		// Malformed body
		c := newFakedClient("VALUE foo 0 4\r\nfoobar\r\nEND", ioutil.Discard)
		_, _, err := c.Get("foo")
		assert.IsType(t, ProtocolError(""), err)
	}

	{
		// Get response misses "END"
		c := newFakedClient("VALUE foo 0 6\r\nfoobar\r\nNOT_END", ioutil.Discard)
		_, _, err := c.Get("foo")
		assert.IsType(t, ProtocolError(""), err)
	}

	{
		// Gets response missing "END"
		c := newFakedClient("VALUE foo 0 6\r\nfoobar\r\nNOT_END", ioutil.Discard)
		_, err := c.Gets([]string{"foo"})
		assert.IsType(t, ProtocolError(""), err)
	}
}

func TestMalformedSetResponse(t *testing.T) {
	c := newFakedClient("foobar", ioutil.Discard)
	err := c.Set("foo", []byte("bar"), 0, 0, false)
	assert.IsType(t, ProtocolError(fmt.Sprintf("unknown reply type: %s", string("foobar"))), err)
}

func TestMalformedStatsResponse(t *testing.T) {
	{
		c := newFakedClient("foobar", ioutil.Discard)
		_, err := c.Stats()
		assert.Equal(t, ProtocolError("malformed stats response"), err)
	}

	{
		expected := net.UnknownNetworkError("test")
		c := newFakedClient("foobar", errorWriter{expected})
		_, err := c.Stats()
		assert.Equal(t, expected, err)
	}
}

func TestIncrValueError(t *testing.T) {
	c := newFakedClient("foobar", ioutil.Discard)

	err := c.Set("foo", []byte("42"), 0, 0, true)
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
