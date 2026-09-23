package factory

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/placement"
	"github.com/looprig/factory/internal/realtime/hostlink"
)

// TestEveryAttachFailureIsClassifiedOntoPlacementsVocabulary holds
// classifyAttach arm by arm. Each row is a different action placement takes --
// branch on a code, exclude a Host, skip a Host, or ABORT -- so a row moving to
// another arm is placement doing the wrong thing, and the abort rows are the
// ones a lax classifier would silently turn into "try the next Host" while the
// first Host may already hold the lease.
func TestEveryAttachFailureIsClassifiedOntoPlacementsVocabulary(t *testing.T) {
	t.Parallel()

	type arm int
	const (
		refused arm = iota
		unsupported
		unreachable
		failed
		unaddressable
		abort
	)
	refusal := sessionwire.HostLinkError{Code: sessionwire.HostLinkErrorEpochMismatch, CurrentLeaseEpoch: 93}
	for name, row := range map[string]struct {
		err  error
		want arm
	}{
		"host refusal":        {fmt.Errorf("attach: %w", &hostlink.HostRefusal{HostLinkError: refusal}), refused},
		"not advertised":      {&hostlink.UnsupportedMethodError{Method: sessionwire.HostLinkMethodAttach}, unsupported},
		"dial failed":         {fmt.Errorf("%w: host-a: refused", hostlink.ErrDialFailed), unreachable},
		"terminal disconnect": {&hostlink.HostDisconnect{Host: "host-a", Code: 3500}, unreachable},
		"between connections": {fmt.Errorf("hostlink: attach: %w", hostlink.ErrLinkReconnecting), unreachable},
		"link ceiling":        {fmt.Errorf("%w: 256 links", hostlink.ErrLinkLimit), unreachable},
		// B5 quality gate Q2: a link made terminal by a wire-version change
		// refuses BEFORE sending, so the Host is unreachable, not ambiguous.
		"wire version": {fmt.Errorf("hostlink: hostlink.attach: %w: host selected 2", hostlink.ErrUnsupportedProtocol), unreachable},
		// B5 quality gate Q1: the Host's own code-less answer.
		"host failure": {&hostlink.HostFailure{Method: sessionwire.HostLinkMethodAttach, Code: 100, Message: "internal server error"}, failed},
		// Gap 1: the candidate's base cannot carry this tenant's address.
		"tenant unaddressable": {&hostlink.EndpointError{Host: "host-a", Tenant: "tenant-a",
			Cause: &sessionwire.HostLinkEndpointError{Code: sessionwire.HostLinkEndpointCodeTooLong}}, unaddressable},
		// v0.7.2 gate S2: a link this replica closed refuses before sending,
		// so the Host never saw the attach and the next candidate may be tried.
		"link closed": {fmt.Errorf("hostlink: hostlink.attach: %w: host-a", hostlink.ErrLinkClosed), unreachable},
		// ...but an attach a close CANCELLED in flight may have left: abort.
		"cancelled by a close":     {fmt.Errorf("hostlink: hostlink.attach: %w", context.Canceled), abort},
		"pool closed":              {hostlink.ErrPoolClosed, abort},
		"malformed reply":          {fmt.Errorf("%w: empty", hostlink.ErrMalformedAttachReply), abort},
		"observation of elsewhere": {fmt.Errorf("%w: generation", hostlink.ErrAttachMismatch), abort},
		"transport after sending":  {errors.New("hostlink: hostlink.attach: context deadline exceeded"), abort},
	} {
		got := classifyAttach(row.err)
		var asRefusal *placement.AttachRefusal
		isRefusal := errors.As(got, &asRefusal)
		isUnsupported := errors.Is(got, placement.ErrAttachUnsupported)
		isUnreachable := errors.Is(got, placement.ErrHostUnreachable)
		isFailed := errors.Is(got, placement.ErrAttachFailed)
		isUnaddressable := errors.Is(got, placement.ErrTenantUnaddressable)
		if isUnaddressable != (row.want == unaddressable) {
			t.Errorf("%s: classifyAttach = %v, ErrTenantUnaddressable = %t", name, got, isUnaddressable)
		}
		if isFailed != (row.want == failed) {
			t.Errorf("%s: classifyAttach = %v, ErrAttachFailed = %t", name, got, isFailed)
		}
		switch row.want {
		case refused:
			if !isRefusal || asRefusal.HostLinkError != refusal {
				t.Errorf("%s: classifyAttach = %v, want *AttachRefusal carrying %+v", name, got, refusal)
			}
		case unsupported:
			if !isUnsupported || isUnreachable || isRefusal {
				t.Errorf("%s: classifyAttach = %v, want ErrAttachUnsupported alone", name, got)
			}
		case unreachable:
			if !isUnreachable || isUnsupported || isRefusal {
				t.Errorf("%s: classifyAttach = %v, want ErrHostUnreachable alone", name, got)
			}
		case failed:
			if isUnreachable || isUnsupported || isRefusal {
				t.Errorf("%s: classifyAttach = %v, want ErrAttachFailed alone", name, got)
			}
		case unaddressable:
			var code *sessionwire.HostLinkEndpointError
			if isUnreachable || isUnsupported || isRefusal || !errors.As(got, &code) || code.Code != sessionwire.HostLinkEndpointCodeTooLong {
				t.Errorf("%s: classifyAttach = %v, want ErrTenantUnaddressable alone, keeping Core's code", name, got)
			}
		case abort:
			if isRefusal || isUnsupported || isUnreachable || isFailed || isUnaddressable || !errors.Is(got, row.err) {
				t.Errorf("%s: classifyAttach = %v, want the error itself, unclassified", name, got)
			}
		}
		if row.want != refused && !errors.Is(got, row.err) {
			t.Errorf("%s: the cause was dropped: %v", name, got)
		}
	}
	if classifyAttach(nil) != nil {
		t.Error("classifyAttach(nil) is not nil")
	}
}

// TestThePlacementPathIsComposedWithTheReplicasLoggerActorAndHorizon holds three
// composition facts no in-tree test read (B5 spec gate X4, Q2, S3): the
// reconciler and the placement sweep write to the composed logger, an attach
// names the service identity as its actor, and the sweep's horizon is the
// apply deadline every admitted command is given -- set here to a value
// distinct from every other limit, so a horizon taken from any other field
// fails.
func TestThePlacementPathIsComposedWithTheReplicasLoggerActorAndHorizon(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	limits := DefaultReconcileLimits()
	limits.ApplyDeadline = 7*time.Minute + 13*time.Second
	options := append(RequiredOptions(),
		WithReconcileLimits(limits), WithLogger(logger), WithPendingCommands(FakeSeams{}), WithFakeJournals())
	server, err := New(options...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	reconciler, sweeper := server.components.placement, server.components.pending
	if got, want := reconciler.ActorID(), FakeServiceIdentity().Subject(); got != want || got == "" {
		t.Errorf("the attach actor is %q, want the service identity %q", got, want)
	}
	if reconciler.Logger() != logger || sweeper.Logger() != logger {
		t.Errorf("placement logs to %p/%p, want the composed logger %p", reconciler.Logger(), sweeper.Logger(), logger)
	}
	if got := sweeper.Horizon(); got != limits.ApplyDeadline {
		t.Errorf("the placement horizon is %v, want the apply deadline %v", got, limits.ApplyDeadline)
	}
}
