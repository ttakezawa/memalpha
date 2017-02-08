// +build debug

package memalpha

import "log"

func debugf(format string, args ...interface{}) {
	log.Printf(format, args...)
}
