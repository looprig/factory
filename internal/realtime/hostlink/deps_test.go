package hostlink_test

import (
	"context"
	"reflect"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/realtime/hostlink"
)

// TestDialerYieldsALinkForTheHostItWasAskedFor drives the seam a test double
// will stand in for once the pool exists. It also pins the result type: a
// Dialer returning something wider than Link would let the pool hold a
// connection it cannot close or attribute.
func TestDialerYieldsALinkForTheHostItWasAskedFor(t *testing.T) {
	t.Parallel()

	var dialer hostlink.Dialer = fakeDialer{}
	link, err := dialer.Dial(context.Background(), "host-1", "https://host-1.internal")
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if got := link.Host(); got != "host-1" {
		t.Errorf("Link.Host() = %q, want %q", got, "host-1")
	}
	if err := link.Close(context.Background()); err != nil {
		t.Errorf("Link.Close: %v", err)
	}

	dial, ok := reflect.TypeOf(&dialer).Elem().MethodByName("Dial")
	if !ok {
		t.Fatal("Dialer has no Dial method")
	}
	if want := reflect.TypeOf((*hostlink.Link)(nil)).Elem(); dial.Type.Out(0) != want {
		t.Errorf("Dial returns %s, want %s", dial.Type.Out(0), want)
	}
}

type fakeDialer struct{}

func (fakeDialer) Dial(_ context.Context, host sessionwire.HostID, _ sessionwire.InternalEndpoint) (hostlink.Link, error) {
	return fakeLink{host: host}, nil
}

type fakeLink struct{ host sessionwire.HostID }

func (l fakeLink) Host() sessionwire.HostID  { return l.host }
func (fakeLink) Close(context.Context) error { return nil }
