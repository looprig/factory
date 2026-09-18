package hostlink_test

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/looprig/factory/internal/realtime/hostlink"
)

// A Host answers the WebSocket upgrade HTTP 400 unless the client names the
// JSON protocol (host@v0.2.1 selectsJSONProtocol), and Core's InternalEndpoint
// refuses the query-string alternative, so the header is the only route open
// to Factory. centrifuge-go's JSON client sends no subprotocol by itself; the
// dialer must. This is the defect that let Factory v0.1.x ship without ever
// having held a link to a Host: every stand-in accepted the header-less
// upgrade, so the suite could not see it.
func TestADialNamesTheJSONSubprotocolAHostRequires(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{})
	link, err := dialHost(t, host, dialerFor(t, serviceToken))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer link.Close(context.Background())

	upgrades := host.upgrades()
	if len(upgrades) != 1 {
		t.Fatalf("the Host saw %d upgrade requests, want 1", len(upgrades))
	}
	got := upgrades[0].Values("Sec-WebSocket-Protocol")
	if len(got) != 1 || got[0] != hostlink.JSONSubprotocol {
		t.Fatalf("Sec-WebSocket-Protocol = %q, want exactly [%q]", got, hostlink.JSONSubprotocol)
	}
}

// The fixture control: the stand-in's gate must refuse an upgrade without the
// header and pass one with it, or the case above pins nothing.
func TestTheStandInRefusesAnUpgradeThatDoesNotNameTheJSONProtocol(t *testing.T) {
	t.Parallel()

	host := newHostServer(t, hostOptions{})
	httpURL := "http" + strings.TrimPrefix(host.url, "ws")

	for name, tc := range map[string]struct {
		protocols []string
		want      int
	}{
		"no subprotocol":       {protocols: nil, want: http.StatusBadRequest},
		"another subprotocol":  {protocols: []string{"centrifuge-protobuf"}, want: http.StatusBadRequest},
		"repeated field":       {protocols: []string{hostlink.JSONSubprotocol, hostlink.JSONSubprotocol}, want: http.StatusBadRequest},
		"the JSON subprotocol": {protocols: []string{hostlink.JSONSubprotocol}, want: http.StatusSwitchingProtocols},
	} {
		// Sequential on purpose: the count below runs after the loop, and a
		// parallel subtest would run after this function returned.
		t.Run(name, func(t *testing.T) {
			request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, httpURL, nil)
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Connection", "Upgrade")
			request.Header.Set("Upgrade", "websocket")
			request.Header.Set("Sec-WebSocket-Version", "13")
			request.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
			for _, protocol := range tc.protocols {
				request.Header.Add("Sec-WebSocket-Protocol", protocol)
			}
			response, err := http.DefaultTransport.RoundTrip(request)
			if err != nil {
				t.Fatalf("upgrade request: %v", err)
			}
			defer response.Body.Close()
			if response.StatusCode != tc.want {
				t.Fatalf("stand-in answered HTTP %d, want %d", response.StatusCode, tc.want)
			}
		})
	}
	if got := len(host.upgrades()); got != 1 {
		t.Fatalf("%d upgrade requests passed the gate, want only the one that named the JSON protocol", got)
	}
}
