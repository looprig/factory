package admission

import (
	"context"
	"errors"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	"github.com/looprig/sessionstore"
)

func TestQuiesceRefusesEveryCommandEntryPoint(t *testing.T) {
	f := newServiceFixture(t)
	if err := f.service.Quiesce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := f.service.Quiesce(context.Background()); err != nil {
		t.Fatalf("repeated Quiesce: %v", err)
	}
	for name, call := range map[string]func() error{
		"create": func() error {
			_, _, err := f.service.AdmitCreate(context.Background(), identity.Principal{}, sessionwire.CreateRequest{})
			return err
		},
		"input": func() error {
			_, _, err := f.service.AdmitInput(context.Background(), identity.Principal{}, sessionwire.InputRequest{})
			return err
		},
		"interrupt": func() error {
			_, _, err := f.service.AdmitInterrupt(context.Background(), identity.Principal{}, sessionwire.InterruptRequest{})
			return err
		},
		"restore": func() error {
			_, _, err := f.service.AdmitRestore(context.Background(), identity.Principal{}, sessionwire.RestoreRequest{})
			return err
		},
		"gate": func() error {
			_, _, err := f.service.AdmitGateResponse(context.Background(), identity.Principal{}, sessionwire.GateResponseRequest{})
			return err
		},
		"legacy create": func() error {
			_, err := f.service.AdmitLegacyCreate(context.Background(), identity.Principal{}, LegacyCreateRequest{})
			return err
		},
	} {
		if err := call(); !errors.Is(err, ErrAdmissionQuiesced) {
			t.Errorf("%s after Quiesce = %v", name, err)
		}
	}
}

type blockingQuiesceAuthorizer struct {
	*serviceAuthorizer
	entered chan struct{}
	release chan struct{}
}

func (a *blockingQuiesceAuthorizer) AuthorizeControl(ctx context.Context, p identity.Principal, session sessionwire.SessionID, kind sessionstore.CommandKind) error {
	close(a.entered)
	<-a.release
	return a.serviceAuthorizer.AuthorizeControl(ctx, p, session, kind)
}

func TestQuiesceJoinsRealAdmissionAlreadyInsideService(t *testing.T) {
	f := newServiceFixture(t)
	resolvableSession(f)
	a := &blockingQuiesceAuthorizer{serviceAuthorizer: f.auth, entered: make(chan struct{}), release: make(chan struct{})}
	f.rebuild(t, func(c *Config) { c.Authorizer = a })
	admitted := make(chan error, 1)
	go func() {
		_, _, err := f.service.AdmitInterrupt(context.Background(), f.principal,
			sessionwire.InterruptRequest{CommandEnvelope: envelope("interrupt-a"), SessionID: "session-a"})
		admitted <- err
	}()
	<-a.entered
	quiesced := make(chan error, 1)
	go func() { quiesced <- f.service.Quiesce(context.Background()) }()
	select {
	case err := <-quiesced:
		t.Fatalf("Quiesce returned before admission ended: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	close(a.release)
	if err := <-admitted; err != nil {
		t.Fatalf("admission accepted before boundary = %v", err)
	}
	if err := <-quiesced; err != nil {
		t.Fatal(err)
	}
	if _, ok := f.commands.records["interrupt-a"]; !ok {
		t.Fatal("preboundary command was not durably admitted")
	}
}

func TestQuiesceWaitsForAnEnteredAdmissionAndCanceledWaiterCanRejoin(t *testing.T) {
	f := newServiceFixture(t)
	end, err := f.service.beginAdmission()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := f.service.Quiesce(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled wait = %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- f.service.Quiesce(context.Background()) }()
	select {
	case err := <-done:
		t.Fatalf("returned before admission ended: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	end()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Quiesce did not join completed admission")
	}
}

func TestFenceAdmissionsReturnsBeforeAnEnteredAdmissionAndRefusesNewOnes(t *testing.T) {
	f := newServiceFixture(t)
	end, err := f.service.beginAdmission()
	if err != nil {
		t.Fatal(err)
	}
	done := f.service.FenceAdmissions()
	select {
	case <-done:
		t.Fatal("fence claimed drained while admission active")
	default:
	}
	if _, err := f.service.beginAdmission(); !errors.Is(err, ErrAdmissionQuiesced) {
		t.Fatalf("new admission = %v", err)
	}
	end()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("drained signal did not close")
	}
}
