package memalpha

import "testing"

func benchmarkSet(b *testing.B, c *Client) {
	key := "foo"
	value := []byte("bar")
	b.SetBytes(int64(len(key) + len(value)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.Set(key, value); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

func benchmarkSetGet(b *testing.B, c *Client) {
	key := "foo"
	value := []byte("bar")
	b.SetBytes(int64(len(key) + len(value)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.Set(key, value); err != nil {
			b.Fatal(err)
		}
		if _, _, err := c.Get(key); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

func BenchmarkSet(b *testing.B) {
	memd := newServer()
	err := memd.Start()
	if err != nil {
		b.Skipf("skipping test; couldn't start memcached: %s", err)
	}
	defer memd.Shutdown()

	benchmarkSet(b, memd.client)
}

func BenchmarkSetGet(b *testing.B) {
	memd := newServer()
	err := memd.Start()
	if err != nil {
		b.Skipf("skipping test; couldn't start memcached: %s", err)
	}
	defer memd.Shutdown()

	benchmarkSetGet(b, memd.client)
}
