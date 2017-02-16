package memalpha

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"testing"
)

func BenchmarkGetStubSmall(b *testing.B) { benchmarkGetStub(b, 5, 5) }
func BenchmarkGetStubLarge(b *testing.B) { benchmarkGetStub(b, 250, 1023*1024) }
func BenchmarkGetSmall(b *testing.B)     { benchmarkGet(b, 5, 5) }
func BenchmarkGetLarge(b *testing.B)     { benchmarkGet(b, 250, 1023*1024) }
func BenchmarkSetSmall(b *testing.B)     { benchmarkSet(b, 5, 5) }
func BenchmarkSetLarge(b *testing.B)     { benchmarkSet(b, 250, 1023*1024) }

type repeatReader struct {
	response []byte
	offset   int
}

func (r *repeatReader) Read(p []byte) (int, error) {
	r.offset = r.offset % len(r.response)
	n := copy(p, r.response[r.offset:])
	r.offset += n
	return n, nil
}

func newRepeatReader(response []byte) *repeatReader {
	return &repeatReader{response, 0}
}

// benchmark with mock server
func benchmarkGetStub(b *testing.B, keySize int, valueSize int) {
	key := string(bytes.Repeat([]byte("A"), keySize))
	value := bytes.Repeat([]byte("A"), valueSize)

	response := fmt.Sprintf("VALUE %s 0 %d\r\n%s\r\nEND\r\n", key, len(value), string(value))
	c := &Client{
		rw: bufio.NewReadWriter(
			bufio.NewReader(newRepeatReader([]byte(response))),
			bufio.NewWriter(ioutil.Discard),
		),
	}

	b.SetBytes(int64(keySize + valueSize))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := c.Get(key)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

func benchmarkGet(b *testing.B, keySize int, valueSize int) {
	key := string(bytes.Repeat([]byte("A"), keySize))
	value := bytes.Repeat([]byte("A"), valueSize)

	memd := newServer()
	if err := memd.Start(); err != nil {
		b.Skipf("skipping test; couldn't start memcached: %s", err)
	}
	defer memd.Shutdown()

	if err := memd.client.Set(key, value, 0, 0, false); err != nil {
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

	if err := memd.client.Set(key, value, 0, 0, false); err != nil {
		b.Skipf("skipping test; couldn't set(%s, %s) = %+v", key, value, err)
	}

	b.SetBytes(int64(keySize + valueSize))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := memd.client.Set(key, value, 0, 0, false); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}
