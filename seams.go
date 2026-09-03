package factory

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"time"
)

// ErrNoEntropy reports that the random source could not supply the bytes an
// identifier is made of.
var ErrNoEntropy = errors.New("factory: random source failed")

// SystemClock is the default Clock: the standard library's.
func SystemClock() Clock { return systemClock{} }

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

func (systemClock) AfterFunc(d time.Duration, f func()) func() bool {
	return time.AfterFunc(d, f).Stop
}

// CryptoUUIDSource is the default UUIDSource: version 4 UUIDs from crypto/rand.
func CryptoUUIDSource() UUIDSource { return uuidSource{} }

type uuidSource struct{}

func (uuidSource) NewUUID() (string, error) { return newUUIDv4(rand.Reader) }

// newUUIDv4 reads sixteen bytes and stamps the version and variant nibbles.
//
// It reads with io.ReadFull and fails on a short read rather than using what it
// got: a partially filled buffer would become a SessionID that a later
// allocation could repeat.
func newUUIDv4(entropy io.Reader) (string, error) {
	var b [16]byte
	if _, err := io.ReadFull(entropy, b[:]); err != nil {
		return "", fmt.Errorf("%w: %v", ErrNoEntropy, err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
