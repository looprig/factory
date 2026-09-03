package factory

import (
	"bytes"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestSystemClockNowTracksTheWallClock(t *testing.T) {
	t.Parallel()

	before := time.Now()
	got := SystemClock().Now()
	after := time.Now()
	if got.Before(before) || got.After(after) {
		t.Fatalf("SystemClock().Now() = %v, want an instant between %v and %v", got, before, after)
	}
}

func TestSystemClockAfterFuncRuns(t *testing.T) {
	t.Parallel()

	fired := make(chan struct{})
	stop := SystemClock().AfterFunc(time.Millisecond, func() { close(fired) })
	select {
	case <-fired:
	case <-time.After(10 * time.Second):
		t.Fatal("SystemClock().AfterFunc did not run its function")
	}
	if stop() {
		t.Error("stop() = true after the function ran, want false")
	}
}

func TestSystemClockAfterFuncStops(t *testing.T) {
	t.Parallel()

	fired := make(chan struct{}, 1)
	stop := SystemClock().AfterFunc(time.Hour, func() { fired <- struct{}{} })
	if !stop() {
		t.Fatal("stop() = false for a timer that had not fired, want true")
	}
	select {
	case <-fired:
		t.Fatal("the function ran after stop() reported it had been prevented")
	default:
	}
}

var uuidV4Pattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestCryptoUUIDSourceProducesDistinctVersion4UUIDs(t *testing.T) {
	t.Parallel()

	source := CryptoUUIDSource()
	seen := map[string]bool{}
	for range 64 {
		got, err := source.NewUUID()
		if err != nil {
			t.Fatalf("NewUUID() = %v, want no error", err)
		}
		if !uuidV4Pattern.MatchString(got) {
			t.Fatalf("NewUUID() = %q, which is not a version 4 UUID with the RFC variant bits", got)
		}
		if seen[got] {
			t.Fatalf("NewUUID() returned %q twice", got)
		}
		seen[got] = true
	}
}

// TestUUIDVersionAndVariantBitsAreSet drives the two byte edits the format
// requires, against entropy that would otherwise leave them clear. Without it a
// generator that only hex-encoded random bytes would pass the pattern above
// about one time in 128 per call and fail intermittently instead of always.
func TestUUIDVersionAndVariantBitsAreSet(t *testing.T) {
	t.Parallel()

	got, err := newUUIDv4(bytes.NewReader(make([]byte, 16)))
	if err != nil {
		t.Fatalf("newUUIDv4(zeroes) = %v, want no error", err)
	}
	const want = "00000000-0000-4000-8000-000000000000"
	if got != want {
		t.Fatalf("newUUIDv4(zeroes) = %q, want %q", got, want)
	}

	ones := bytes.Repeat([]byte{0xff}, 16)
	got, err = newUUIDv4(bytes.NewReader(ones))
	if err != nil {
		t.Fatalf("newUUIDv4(ones) = %v, want no error", err)
	}
	if want := "ffffffff-ffff-4fff-bfff-ffffffffffff"; got != want {
		t.Fatalf("newUUIDv4(ones) = %q, want %q", got, want)
	}
}

// TestUUIDSourceFailsWhenEntropyDoes is the arm crypto/rand does not reach in
// practice. A generator that ignored a short read would emit a UUID made
// partly of an uninitialised buffer, which is exactly the value that must never
// become a SessionID.
func TestUUIDSourceFailsWhenEntropyDoes(t *testing.T) {
	t.Parallel()

	for name, entropy := range map[string]*bytes.Reader{
		"no entropy":    bytes.NewReader(nil),
		"short entropy": bytes.NewReader(make([]byte, 15)),
	} {
		got, err := newUUIDv4(entropy)
		if err == nil {
			t.Errorf("newUUIDv4(%s) = %q, want an error", name, got)
			continue
		}
		if !errors.Is(err, ErrNoEntropy) {
			t.Errorf("newUUIDv4(%s) error %v does not wrap ErrNoEntropy", name, err)
		}
		if got != "" {
			t.Errorf("newUUIDv4(%s) = %q alongside its error, want the empty string", name, got)
		}
	}
}

func TestSystemClockAndUUIDSourceAreTheDefaults(t *testing.T) {
	t.Parallel()

	server, err := New(RequiredOptions()...)
	if err != nil {
		t.Fatalf("New() = %v, want no error", err)
	}
	if _, ok := server.cfg.clock.(systemClock); !ok {
		t.Errorf("default clock is %T, want systemClock", server.cfg.clock)
	}
	if server.cfg.uuids == nil {
		t.Fatal("the composition has no default UUID source")
	}
	id, err := server.cfg.uuids.NewUUID()
	if err != nil {
		t.Fatalf("default UUID source failed: %v", err)
	}
	if !uuidV4Pattern.MatchString(strings.ToLower(id)) {
		t.Errorf("default UUID source produced %q", id)
	}
}
