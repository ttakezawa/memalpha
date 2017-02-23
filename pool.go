package memalpha

import (
	"context"
)

// Pool maintains a pool of connections.
type Pool struct {
	Addr      string
	idleConns chan *Conn
}

// NewPool creates a new pool.
func NewPool(addr string, maxIdleConns int) *Pool {
	return &Pool{
		Addr:      addr,
		idleConns: make(chan *Conn, maxIdleConns),
	}
}

// Get gets a connection.
func (p *Pool) Get() (*Conn, error) {
	return p.GetContext(context.Background())
}

// GetContext gets a connection using the provided context.
func (p *Pool) GetContext(ctx context.Context) (*Conn, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case c := <-p.idleConns:
		return c, nil
	default:
	}
	return DialContext(ctx, p.Addr)
}

// Put puts a connection into a pool.
func (p *Pool) Put(c *Conn) error {
	select {
	case p.idleConns <- c:
		return nil
	default:
		return c.Close()
	}
}
