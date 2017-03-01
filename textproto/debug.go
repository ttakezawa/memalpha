// +build debug

package textproto

import "log"

func debugf(format string, args ...interface{}) {
	log.Printf(format, args...)
}
