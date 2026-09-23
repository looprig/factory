package factory

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/realtime/hostlink"
	"github.com/looprig/sessionstore"
)

// These cases hold the gates READ to the gate_response WRITE path's owner
// check (tests-lane I2.3, host v0.8.1 option A). A gate the store holds as
// "resident" stays resident after its Host released the session
// crash-equivalently or crashed -- only a successor re-publishes it -- so the
// read reported an answer button that could only ever get 409
// gate_not_resumable. The read now overlays the answerability from the SAME
// check the write makes: no fresh owner, or an owner that does not advertise
// hostlink.command.gate_response, reads "unavailable". The gate itself is
// never hidden.

func getGates(t *testing.T, server *Server) sessionwire.GatePage {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, e2eOrigin+"/v1/sessions/"+string(e2eSession)+"/gates", nil)
	request.Header.Set("Authorization", "Bearer "+FakeCredentialValue)
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET gates = %d %s", recorder.Code, recorder.Body)
	}
	var page sessionwire.GatePage
	if err := json.Unmarshal(recorder.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode %s: %v", recorder.Body, err)
	}
	return page
}

func TestTheGatesReadAgreesWithTheGateResponseWritePath(t *testing.T) {
	t.Parallel()

	capable := []string{sessionwire.HostLinkMethodBind, sessionwire.HostLinkCapabilityGateResponse}
	incapable := []string{sessionwire.HostLinkMethodBind, sessionwire.HostLinkMethodUnbind, sessionwire.HostLinkMethodAttach}
	fresh := func(*sessionstore.PutHostRegistrationRequest) bool { return true }
	for name, row := range map[string]struct {
		owner  func(*sessionstore.PutHostRegistrationRequest) bool
		dial   *scriptedDial
		want   sessionwire.GateAnswerability
		answer int
	}{
		"a fresh capable owner": {fresh, &scriptedDial{methods: capable}, sessionwire.GateAnswerabilityResident, http.StatusAccepted},
		"a fresh owner that cannot apply a gate response": {fresh, &scriptedDial{methods: incapable},
			sessionwire.GateAnswerabilityUnavailable, http.StatusConflict},
		"an owner that released the session (not accepting)": {func(r *sessionstore.PutHostRegistrationRequest) bool {
			r.Route.Accepting = false
			return true
		}, &scriptedDial{methods: capable}, sessionwire.GateAnswerabilityUnavailable, http.StatusConflict},
		"an owner releasing the session": {func(r *sessionstore.PutHostRegistrationRequest) bool {
			r.Route.Residency = sessionwire.SessionResidencyReleasing
			r.Route.Accepting = false
			return true
		}, &scriptedDial{methods: capable}, sessionwire.GateAnswerabilityUnavailable, http.StatusConflict},
		"no owner at all (crashed and lapsed)": {func(*sessionstore.PutHostRegistrationRequest) bool { return false },
			&scriptedDial{methods: capable}, sessionwire.GateAnswerabilityUnavailable, http.StatusConflict},
		// The write answers a transient 503; the read cannot promise an answer
		// it could not check, so it fails closed on the button.
		"the owner could not be asked": {fresh, &scriptedDial{dialErr: fmt.Errorf("%w: handshake did not settle", hostlink.ErrDialFailed)},
			sessionwire.GateAnswerabilityUnavailable, http.StatusServiceUnavailable},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store := gateWorldWithOwner(t, row.owner)
			server := composedGateServer(t, store, row.dial)
			page := getGates(t, server)
			if len(page.Gates) != 1 || page.Gates[0].GateID != "gate-a" {
				t.Fatalf("the gates read = %+v, want the open gate kept", page.Gates)
			}
			if got := page.Gates[0].Answerability; got != row.want {
				t.Fatalf("answerability = %q, want %q", got, row.want)
			}
			answer := postGateResponse(t, server, "answer-r")
			if answer.Code != row.answer {
				t.Fatalf("the answer = %d %s, want %d", answer.Code, answer.Body, row.answer)
			}
			if row.answer == http.StatusConflict && !strings.Contains(answer.Body.String(), string(sessionwire.ErrorCodeGateNotResumable)) {
				t.Fatalf("the answer = %s, want gate_not_resumable", answer.Body)
			}
		})
	}
}

