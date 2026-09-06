package transport_test

import (
	"bufio"
	"crypto/rand"
	"encoding/base64"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestWebSocketCompressionIsRefusedInTheHandshake asserts that the embedded
// server does not negotiate permessage-deflate, and asserts it against a client
// that actually OFFERS the extension.
//
// This case exists because the obvious way to test the setting cannot work. A
// test driven by centrifuge-go observes nothing: the client library exposes
// Config.EnableCompression, but it surfaces neither the handshake response nor
// the negotiated extension, so `Compression: true` and `Compression: false`
// produce byte-identical observable behaviour through that API. A mutant that
// turned compression on therefore survived the entire rest of this package.
//
// So this case speaks the WebSocket opening handshake itself, over a plain TCP
// connection, using nothing but the standard library. That is why it can see
// the answer, and it is also why it adds no dependency: asserting on the
// negotiated extension does NOT require promoting gorilla/websocket to a direct
// requirement of this module.
//
// The two subtests are one assertion, not two. "The response carried no
// Sec-WebSocket-Extensions header" is worthless on its own -- a probe that
// forgot to offer the extension, or that misspelled the header, would produce
// exactly that result against any server. The `accepted` subtest is the reader:
// it drives the same probe against a server with compression ON and requires
// the header to come back. Only the pair distinguishes "refused" from
// "never asked".
func TestWebSocketCompressionIsRefusedInTheHandshake(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		compression bool
		wantHeader  bool
	}{
		// The production configuration. This is the claim under test.
		{name: "refused", compression: false, wantHeader: false},
		// The control. If this subtest ever fails, the "refused" subtest above
		// has stopped meaning anything and must not be trusted.
		{name: "accepted", compression: true, wantHeader: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			server := newServer(t, serverOptions{compression: tc.compression})
			extensions := negotiatedExtensions(t, server.addr)

			got := extensions != ""
			if got != tc.wantHeader {
				t.Fatalf("Sec-WebSocket-Extensions in the handshake response = %q (present=%v), want present=%v",
					extensions, got, tc.wantHeader)
			}
			// The control asserts the VALUE too, not merely presence: a server
			// echoing some unrelated extension would otherwise satisfy it.
			if tc.wantHeader && !strings.Contains(extensions, "permessage-deflate") {
				t.Fatalf("negotiated extensions = %q, want it to contain \"permessage-deflate\"", extensions)
			}
		})
	}
}

// negotiatedExtensions performs one WebSocket opening handshake that OFFERS
// permessage-deflate and returns the Sec-WebSocket-Extensions header the server
// answered with. An empty string means the server declined the extension.
//
// It fails the test rather than returning an error for anything that is not the
// server's negotiation decision, so a caller reading "" can only be reading a
// refusal and never a broken probe.
func negotiatedExtensions(t *testing.T, addr string) string {
	t.Helper()

	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		t.Fatalf("dialing %s: %v", addr, err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(waitFor)); err != nil {
		t.Fatalf("setting the probe deadline: %v", err)
	}

	// A fresh 16-byte key per handshake, as RFC 6455 section 4.1 requires.
	var key [16]byte
	if _, err := rand.Read(key[:]); err != nil {
		t.Fatalf("generating the handshake key: %v", err)
	}
	request := strings.Join([]string{
		"GET /connection/websocket HTTP/1.1",
		"Host: " + addr,
		"Upgrade: websocket",
		"Connection: Upgrade",
		"Sec-WebSocket-Version: 13",
		"Sec-WebSocket-Key: " + base64.StdEncoding.EncodeToString(key[:]),
		"Sec-WebSocket-Extensions: permessage-deflate; client_max_window_bits",
		"", "",
	}, "\r\n")
	if _, err := conn.Write([]byte(request)); err != nil {
		t.Fatalf("writing the handshake request: %v", err)
	}

	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatalf("reading the handshake response: %v", err)
	}
	defer response.Body.Close()

	// Anything other than 101 means the probe never got far enough to observe a
	// negotiation, and reporting "" for it would read as a refusal.
	if response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("handshake status = %d %s, want %d (the probe never reached a negotiation)",
			response.StatusCode, response.Status, http.StatusSwitchingProtocols)
	}
	if upgrade := response.Header.Get("Upgrade"); !strings.EqualFold(upgrade, "websocket") {
		t.Fatalf("handshake Upgrade header = %q, want \"websocket\"", upgrade)
	}

	return response.Header.Get("Sec-WebSocket-Extensions")
}
