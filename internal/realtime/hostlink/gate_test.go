package hostlink

import (
	"net/http"
	"strings"
)

// JSONSubprotocol is the WebSocket subprotocol a Host requires on the HostLink
// upgrade. It is named here, in the package under test, so the stand-in
// servers and the dialer's own test agree on one literal rather than each
// spelling Host's gate from memory.
const JSONSubprotocol = "centrifuge-json"

// RequireJSONSubprotocol is the stand-in's copy of Host's selectsJSONProtocol
// gate (host@v0.2.1 internal/realtime/hostlink/centrifuge.go:207-241), which
// answers HTTP 400 BEFORE the WebSocket handler runs unless the client named
// the JSON protocol. Host's own tests always send the header, so Host never saw
// that Factory's dialer did not, and this package's stand-ins accepted whatever
// upgrade arrived -- which is why a Factory that had never held a link to any
// Host passed its whole suite. Every stand-in now sits behind this gate.
//
// It is deliberately the HEADER route only. Host also accepts format=json or
// cf_protocol=json in the query, but Core's InternalEndpoint.Validate refuses
// a query string, so that route is closed to Factory and a stand-in that
// honoured it would accept a dial a real Host cannot receive.
func RequireJSONSubprotocol(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if !namesJSONSubprotocol(request) {
			http.Error(writer, "HostLink requires the JSON protocol", http.StatusBadRequest)
			return
		}
		next.ServeHTTP(writer, request)
	})
}

// namesJSONSubprotocol mirrors Host's header rule exactly: one physical
// Sec-WebSocket-Protocol field, every comma-separated token equal to
// centrifuge-json. A repeated field is refused because gorilla reads only the
// first one and would silently ignore the rest.
func namesJSONSubprotocol(request *http.Request) bool {
	values := request.Header.Values("Sec-WebSocket-Protocol")
	if len(values) != 1 {
		return false
	}
	selected := false
	for _, token := range strings.Split(values[0], ",") {
		if strings.TrimSpace(token) != JSONSubprotocol {
			return false
		}
		selected = true
	}
	return selected
}
