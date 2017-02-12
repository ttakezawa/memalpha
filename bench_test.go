package memalpha

import (
	"bytes"
	"testing"
)

func BenchmarkGetSmall(b *testing.B) { benchmarkGet(b, 5, 5) }
func BenchmarkGetLarge(b *testing.B) { benchmarkGet(b, 250, 500*1024) }
func BenchmarkSetSmall(b *testing.B) { benchmarkSet(b, 5, 5) }
func BenchmarkSetLarge(b *testing.B) { benchmarkSet(b, 250, 500*1024) }

func benchmarkGet(b *testing.B, keySize int, valueSize int) {
	key := string(bytes.Repeat([]byte("A"), keySize))
	value := bytes.Repeat([]byte("A"), valueSize)

	memd := newServer()
	if err := memd.Start(); err != nil {
		b.Skipf("skipping test; couldn't start memcached: %s", err)
	}
	defer memd.Shutdown()

	if err := memd.client.Set(key, value); err != nil {
		b.Skipf("skipping test; couldn't set(%s, %s) = %+v", key, value, err)
	}

	b.SetBytes(int64(keySize + valueSize))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := memd.client.Get(key); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

func benchmarkSet(b *testing.B, keySize int, valueSize int) {
	key := string(bytes.Repeat([]byte("A"), keySize))
	value := bytes.Repeat([]byte("A"), valueSize)

	memd := newServer()
	if err := memd.Start(); err != nil {
		b.Skipf("skipping test; couldn't start memcached: %s", err)
	}
	defer memd.Shutdown()

	if err := memd.client.Set(key, value); err != nil {
		b.Skipf("skipping test; couldn't set(%s, %s) = %+v", key, value, err)
	}

	b.SetBytes(int64(keySize + valueSize))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := memd.client.Set(key, value); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}
