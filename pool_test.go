package memalpha

import (
	"testing"

	"context"

	"github.com/stretchr/testify/assert"
)

func TestPool(t *testing.T) {
	memd := newServer()
	err := memd.Start()
	if err != nil {
		t.Skipf("skipping test; couldn't start memcached: %s", err)
	}
	defer func() { _ = memd.Shutdown() }()

	// max idle connections = 1
	pool := NewPool(memd.conn.Addr, 1)

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
