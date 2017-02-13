package memalpha

import (
	"bufio"
	"bytes"
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

	err := c.Set("foo", []byte("bar"))
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

	err := c.Set("foo", []byte("bar"))
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

	err := c.Set("foo", []byte("bar"))
	assert.Equal(t, err, ErrReplyError)
}

func TestStatsProtocolError(t *testing.T) {
	response := bytes.NewReader([]byte("foobar"))
	request := bytes.NewBuffer([]byte{})

	serverReadWriter := bufio.NewReadWriter(bufio.NewReader(response), bufio.NewWriter(request))

	c := &Client{rw: serverReadWriter}

	_, err := c.Stats()
	assert.Equal(t, err, ProtocolError("malformed stats response"))
}
