package memalpha_test

import (
	"testing"

	"context"

	"github.com/stretchr/testify/assert"
	"github.com/ttakezawa/memalpha"
	"github.com/ttakezawa/memalpha/internal/memdtest"
	"github.com/ttakezawa/memalpha/textproto"
)

func TestPool(t *testing.T) {
	memd := memdtest.NewServer(func(addr string) (memalpha.Conn, error) {
		return textproto.Dial(addr)
	})
	err := memd.Start()
	if err != nil {
		t.Skipf("skipping test; couldn't start memcached: %s", err)
	}
	defer func() { _ = memd.Shutdown() }()

	// max idle connections = 1
	pool := memalpha.NewPool(
		func(ctx context.Context) (memalpha.Conn, error) {
			return textproto.DialContext(ctx, memd.Addr)
		},
		1,
	)

	// create a connection
	conn1, err := pool.Get()
	assert.NoError(t, err)

	// create a connection
	conn2, err := pool.Get()
	assert.NoError(t, err)

	// put a connection into a pool.
	err = pool.Put(conn1)
	assert.NoError(t, err)

	// discard a connection.
	err = pool.Put(conn2)
	assert.NoError(t, err)

	// gets a pooled connection.
	_, err = pool.Get()
	assert.NoError(t, err)

	// get a connection using an canceled context.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = pool.GetContext(ctx)
	assert.Error(t, err)
}
