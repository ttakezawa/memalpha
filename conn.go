package memalpha

// Conn is a connection to a memcached server.
type Conn interface {
	Close() error
	Get(key string) (value []byte, flags uint32, err error)
	Gets(keys []string) (map[string]*Response, error)
	Set(key string, value []byte, flags uint32, exptime int, noreply bool) error
	Add(key string, value []byte, flags uint32, exptime int, noreply bool) error
	Replace(key string, value []byte, flags uint32, exptime int, noreply bool) error
	Append(key string, value []byte, noreply bool) error
	Prepend(key string, value []byte, noreply bool) error
	CompareAndSwap(key string, value []byte, casid uint64, flags uint32, exptime int, noreply bool) error
	Delete(key string, noreply bool) error
	Increment(key string, value uint64, noreply bool) (uint64, error)
	Decrement(key string, value uint64, noreply bool) (uint64, error)
	Touch(key string, exptime int32, noreply bool) error
	Stats() (map[string]string, error)
	StatsArg(argument string) (map[string]string, error)
	FlushAll(delay int, noreply bool) error
	Version() (string, error)
	Quit() error
}

// Response is a response of get
type Response struct {
	Value []byte
	Flags uint32
	CasID uint64
}
