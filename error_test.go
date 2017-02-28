package memalpha_test

import (
	"strings"
	"testing"

	"github.com/ttakezawa/memalpha"
)

func TestErrorMessage(t *testing.T) {
	var err error

	err = memalpha.ProtocolError("foo")
	if !strings.Contains(err.Error(), "protocol error: foo") {
		t.Errorf("%q should contain %q, want ", err, "protocol error: foo")
	}

	err = memalpha.ClientError("bar")
	if !strings.Contains(err.Error(), "client error: bar") {
		t.Errorf("%q should contain %q, want ", err, "client error: bar")
	}

	err = memalpha.ServerError("baz")
	if !strings.Contains(err.Error(), "server error: baz") {
		t.Errorf("%q should contain %q, want ", err, "server error: baz")
	}
}
