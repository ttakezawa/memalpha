package memalpha

import (
	"bufio"
	"bytes"
	"strings"
	"testing"
)

func TestHandleServerError(t *testing.T) {
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
