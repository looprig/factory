package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	internalidentity "github.com/looprig/factory/internal/identity"
	"github.com/looprig/sessionstore"
)

func TestBootstrapReturnsOnlyTheAuthenticatedTenant(t *testing.T) {
	t.Parallel()

	for _, tenant := range []sessionwire.TenantID{fixtureTenant, otherTenant} {
		tenant := tenant
		t.Run(string(tenant), func(t *testing.T) {
			t.Parallel()

			f := newFixture(t, withVerifierTenant(tenant))
			recorder := f.get("/v1/bootstrap")
			if recorder.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200; body %q", recorder.Code, recorder.Body)
			}
			if got, want := recorder.Body.String(), `{"tenant_id":"`+string(tenant)+`"}`+"\n"; got != want {
				t.Errorf("body = %q, want exactly %q", got, want)
			}
			if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
				t.Errorf("Cache-Control = %q, want no-store", got)
			}
			var shape map[string]json.RawMessage
			if err := json.Unmarshal(recorder.Body.Bytes(), &shape); err != nil {
				t.Fatalf("decoding bootstrap: %v", err)
			}
			if len(shape) != 1 {
				t.Errorf("bootstrap exposes %d members, want only tenant_id: %v", len(shape), shape)
			}
		})
	}
}

func TestBootstrapAuthorityCannotBeChosenByRequestInput(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	r := request(http.MethodGet, "/v1/bootstrap", strings.NewReader(`{"tenant_id":"tenant-b"}`))
	r.Header.Set("X-Tenant-ID", string(otherTenant))
	recorder := f.serve(r)
	if recorder.Code != http.StatusOK {
		t.Fatalf("body and header input answered %d, want 200; body %q", recorder.Code, recorder.Body)
	}
	if got, want := recorder.Body.String(), `{"tenant_id":"tenant-a"}`+"\n"; got != want {
		t.Errorf("body/header input selected authority: body = %q, want %q", got, want)
	}

	withQuery := f.get("/v1/bootstrap?tenant_id=" + string(otherTenant))
	if withQuery.Code != http.StatusBadRequest {
		t.Fatalf("tenant query answered %d, want 400; body %q", withQuery.Code, withQuery.Body)
	}
	if got := decodeEnvelope(t, withQuery).Error.Code; got != sessionwire.ErrorCodeInvalidRequest {
		t.Errorf("query error code = %q, want %q", got, sessionwire.ErrorCodeInvalidRequest)
	}

	withPath := f.get("/v1/bootstrap/" + string(otherTenant))
	if withPath.Code != http.StatusNotFound {
		t.Fatalf("tenant path answered %d, want 404; body %q", withPath.Code, withPath.Body)
	}
}

func TestBootstrapAuthenticationFailuresUseTheOrdinaryEnvelope(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		request func() *http.Request
		options []fixtureOption
		status  int
		body    string
	}{
		{
			name: "missing credential",
			request: func() *http.Request {
				r := httptest.NewRequest(http.MethodGet, fixtureOrigin+"/v1/bootstrap", nil)
				r.Host = fixtureHost
				return r
			},
			status: http.StatusUnauthorized,
			body:   `{"error":{"code":"unauthenticated","message":"the request presented no credential this deployment accepts","retryable":false}}`,
		},
		{
			name: "invalid credential",
			request: func() *http.Request {
				r := request(http.MethodGet, "/v1/bootstrap", nil)
				r.Header.Set("Authorization", "Bearer not-the-fixture-credential")
				return r
			},
			status: http.StatusUnauthorized,
			body:   `{"error":{"code":"unauthenticated","message":"the request presented no credential this deployment accepts","retryable":false}}`,
		},
		{
			name:    "credential verifier unavailable",
			request: func() *http.Request { return request(http.MethodGet, "/v1/bootstrap", nil) },
			options: []fixtureOption{withVerifierFailure(errors.New("credential verifier unavailable"))},
			status:  http.StatusServiceUnavailable,
			body:    `{"error":{"code":"unavailable","message":"credentials cannot be verified at the moment","retryable":true}}`,
		},
	}
	for _, probe := range cases {
		probe := probe
		t.Run(probe.name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t, probe.options...)
			recorder := f.serve(probe.request())
			if recorder.Code != probe.status {
				t.Fatalf("status = %d, want %d; body %q", recorder.Code, probe.status, recorder.Body)
			}
			if got := recorder.Body.String(); got != probe.body {
				t.Errorf("body = %q, want exactly %q", got, probe.body)
			}
			if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
				t.Errorf("Cache-Control = %q, want no-store", got)
			}
		})
	}
}

func TestBootstrapIsAReadThatNeedsNeitherCSRFNorDurableState(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.reads.fail = &sessionstore.StoreClosedError{}
	r := httptest.NewRequest(http.MethodGet, fixtureOrigin+"/v1/bootstrap", nil)
	r.Host = fixtureHost
	r.Header.Set("Origin", fixtureOrigin)
	r.AddCookie(&http.Cookie{Name: internalidentity.DefaultCookieName, Value: fixtureBearer})
	recorder := f.serve(r)
	if recorder.Code != http.StatusOK {
		t.Fatalf("cookie GET without a CSRF token on a draining store = %d, want 200; body %q", recorder.Code, recorder.Body)
	}
	requests, _ := f.reads.snapshot()
	if len(requests) != 0 || len(f.reads.listSnapshot()) != 0 {
		t.Errorf("bootstrap reached durable state: catalog=%+v list=%+v", requests, f.reads.listSnapshot())
	}
}
