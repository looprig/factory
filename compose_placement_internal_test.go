package factory

import (
	"errors"
	"fmt"
	"testing"

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
		abort
	)
	refusal := sessionwire.HostLinkError{Code: sessionwire.HostLinkErrorEpochMismatch, CurrentLeaseEpoch: 93}
	for name, row := range map[string]struct {
		err  error
		want arm
	}{
		"host refusal":             {fmt.Errorf("attach: %w", &hostlink.HostRefusal{HostLinkError: refusal}), refused},
		"not advertised":           {&hostlink.UnsupportedMethodError{Method: sessionwire.HostLinkMethodAttach}, unsupported},
		"dial failed":              {fmt.Errorf("%w: host-a: refused", hostlink.ErrDialFailed), unreachable},
		"terminal disconnect":      {&hostlink.HostDisconnect{Host: "host-a", Code: 3500}, unreachable},
		"between connections":      {fmt.Errorf("hostlink: attach: %w", hostlink.ErrLinkReconnecting), unreachable},
		"link ceiling":             {fmt.Errorf("%w: 256 links", hostlink.ErrLinkLimit), unreachable},
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
		case abort:
			if isRefusal || isUnsupported || isUnreachable || !errors.Is(got, row.err) {
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
