package httpapi

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/looprig/factory/internal/admission"
	"github.com/looprig/factory/internal/realtime/hostlink"
)

// TestATransientCapabilityReadIsAnsweredRetryable is the v0.5.0 quality gate's
// F1 probe, kept as the regression: the owner's tenant link reconnecting, the
// pool's ceiling full and a failed dial -- wrapped exactly as the composition
// classifies them and admission then wraps them -- answer 503 unavailable,
// which is retryable, as a draining store does. The control is the same causes
// WITHOUT the classification: a plain fault, which stays 500.
func TestATransientCapabilityReadIsAnsweredRetryable(t *testing.T) {
	for _, cause := range []error{
		fmt.Errorf("hostlink: capability read: %w", hostlink.ErrLinkReconnecting),
		hostlink.ErrLinkLimit,
		fmt.Errorf("%w: host-a: handshake did not settle", hostlink.ErrDialFailed),
	} {
		classified := fmt.Errorf("admission: ask the owner whether it applies gate responses: %w",
			fmt.Errorf("%w: %w", admission.ErrGateResponderUnavailable, cause))
		got := admissionFailure(classified)
		if got.status != http.StatusServiceUnavailable || got.code != ErrorCodeUnavailable || !retryableStatus(got.status) {
			t.Errorf("%v -> status=%d code=%s retryable=%v, want 503 unavailable retryable", cause, got.status, got.code, retryableStatus(got.status))
		}
		plain := admissionFailure(fmt.Errorf("admission: ask the owner whether it applies gate responses: %w", errors.New(cause.Error())))
		if plain.status != http.StatusInternalServerError {
			t.Errorf("an unclassified fault -> %d, want 500 (the classification is the composition's)", plain.status)
		}
	}
}

func TestPrincipalCapabilityFaultIsAnsweredRetryable(t *testing.T) {
	err := fmt.Errorf("ask: %w", admission.ErrPrincipalResponderUnavailable)
	got := admissionFailure(err)
	if got.status != http.StatusServiceUnavailable || got.code != ErrorCodeUnavailable {
		t.Fatalf("admissionFailure = %+v", got)
	}
}
