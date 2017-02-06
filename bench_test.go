package memalpha

import "testing"

func benchmarkSet(b *testing.B) {
	_ = startMemcached(func(c *Client) {
		key := "foo"
		value := "bar"
		b.SetBytes(int64(len(key) + len(value)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := c.Set(key, value); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
	})
}

func benchmarkSetGet(b *testing.B) {
	_ = startMemcached(func(c *Client) {
		key := "foo"
		value := "bar"
		b.SetBytes(int64(len(key) + len(value)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := c.Set(key, value); err != nil {
				b.Fatal(err)
			}
			if _, err := c.Get(key); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
	})
}

func BenchmarkSet(b *testing.B) {
	benchmarkSet(b)
}

func BenchmarkSetGet(b *testing.B) {
	benchmarkSetGet(b)
}
