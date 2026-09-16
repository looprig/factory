package factory_test

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	internalidentity "github.com/looprig/factory/internal/identity"
	"github.com/looprig/factory/internal/realtime/clientlink"
)

// TestAComposedFactoryAdmitsACookieUpgradeWithNoConnectToken is B7 measured
// end to end through the COMPOSED chain -- the real router's authentication,
// the real origin guard, the real ClientLink -- rather than through the
// handler alone: the property is that the principal the router verified at the
// upgrade is the one the handshake reuses, and only the composition has both
// halves.
//
// It is written over a raw socket, with the WebSocket handshake and one
// Centrifuge JSON connect frame spelled by hand, because the import boundary
// confines centrifuge and its client to internal/realtime, and the existing
// guard case already writes handshakes this way for the same reason.
//
// Three rows, each asserted absolutely:
//
//   - a cookie-authenticated upgrade followed by a connect frame carrying NO
//     token is answered with a connect result naming this build, so the link
//     is open;
//   - the same upgrade followed by a connect frame carrying a BOGUS token is
//     closed 3500: a token presented is a token verified, and the cookie does
//     not rescue it;
//   - an upgrade with no credential at all is refused by the router with 401
//     before any socket exists, which is what an unauthenticated browser gets.
func TestAComposedFactoryAdmitsACookieUpgradeWithNoConnectToken(t *testing.T) {
	t.Parallel()

	server := composed(t, &probe{})
	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	public := httptest.NewServer(server.Handler())
	t.Cleanup(public.Close)
	address := public.Listener.Addr().String()
	cookie := internalidentity.DefaultCookieName + "=" + factoryFakeCredential

	t.Run("no credential on the upgrade is refused by the router", func(t *testing.T) {
		conn, status := upgradeRealtime(t, address, "")
		defer conn.Close()
		if status != http.StatusUnauthorized {
			t.Fatalf("upgrade with no credential answered %d, want 401", status)
		}
	})

	t.Run("cookie upgrade, no token: connected as the cookie's principal", func(t *testing.T) {
		conn, status := upgradeRealtime(t, address, cookie)
		defer conn.Close()
		if status != http.StatusSwitchingProtocols {
			t.Fatalf("upgrade with the cookie answered %d, want 101", status)
		}
		writeTextFrame(t, conn, connectFrame(t, ""))
		opcode, payload := readFrame(t, conn)
		if opcode != 0x1 {
			t.Fatalf("the first frame after connect had opcode %#x (payload %q), want a text reply", opcode, payload)
		}
		var reply struct {
			ID      uint32          `json:"id"`
			Error   json.RawMessage `json:"error"`
			Connect *struct {
				Client string `json:"client"`
				Data   struct {
					FactoryVersion string `json:"factory_version"`
				} `json:"data"`
			} `json:"connect"`
		}
		if err := json.Unmarshal(payload, &reply); err != nil {
			t.Fatalf("decode connect reply %q: %v", payload, err)
		}
		if reply.ID != 1 || reply.Connect == nil || reply.Connect.Client == "" || len(reply.Error) != 0 {
			t.Fatalf("connect reply = %s, want id 1 with a connect result naming a client and no error", payload)
		}
		if reply.Connect.Data.FactoryVersion == "" {
			t.Errorf("connect reply %s names no factory_version; the reply is not this build's handshake", payload)
		}
	})

	t.Run("cookie upgrade, bogus token: the token is verified and refused", func(t *testing.T) {
		conn, status := upgradeRealtime(t, address, cookie)
		defer conn.Close()
		if status != http.StatusSwitchingProtocols {
			t.Fatalf("upgrade with the cookie answered %d, want 101", status)
		}
		writeTextFrame(t, conn, connectFrame(t, "not-a-credential-anyone-issued"))
		opcode, payload := readFrame(t, conn)
		if opcode != 0x8 {
			t.Fatalf("the first frame after a bogus-token connect had opcode %#x (payload %q), want a close", opcode, payload)
		}
		if len(payload) < 2 {
			t.Fatalf("close frame carries no code: %q", payload)
		}
		if code := binary.BigEndian.Uint16(payload[:2]); code != 3500 {
			t.Fatalf("closed with %d, want exactly 3500 (invalid token)", code)
		}
	})
}

// factoryFakeCredential is the one credential value FakeVerifier accepts, named
// here so the cookie a browser would send is spelled from the same constant the
// bearer cases use.
const factoryFakeCredential = "composed-credential"

// upgradeRealtime writes one WebSocket handshake to /v1/realtime from the
// deployment's own origin, with the given Cookie header when non-empty, and
// returns the connection and the status line's code. On 101 the connection is
// positioned after the response headers.
func upgradeRealtime(t *testing.T, address, cookie string) (net.Conn, int) {
	t.Helper()

	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatalf("dial %s: %v", address, err)
	}
	if err := conn.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
		t.Fatalf("SetDeadline: %v", err)
	}
	// The Host is the deployment's own, not the ephemeral listener's: the guard
	// refuses a request addressed to a host it does not serve, and the
	// trusted origin the composition names is what it serves.
	request := "GET /v1/realtime HTTP/1.1\r\n" +
		"Host: app.example.com\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Version: 13\r\n" +
		"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n" +
		"Origin: " + trustedBase + "\r\n"
	if cookie != "" {
		request += "Cookie: " + cookie + "\r\n"
	}
	request += "\r\n"
	if _, err := io.WriteString(conn, request); err != nil {
		t.Fatalf("write handshake: %v", err)
	}
	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatalf("read handshake response: %v", err)
	}
	if response.StatusCode != http.StatusSwitchingProtocols {
		defer response.Body.Close()
		body, _ := io.ReadAll(response.Body)
		t.Logf("upgrade answered %d: %s", response.StatusCode, body)
		return conn, response.StatusCode
	}
	// The bufio reader may hold bytes past the headers; hand them on.
	return &bufferedConn{Conn: conn, reader: reader}, response.StatusCode
}

type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) { return c.reader.Read(p) }

// connectFrame is the Centrifuge JSON protocol's connect command: id 1, the
// token as given, and the connect data this build's ClientLink reads.
func connectFrame(t *testing.T, token string) []byte {
	t.Helper()

	connect := map[string]any{"data": map[string]string{"protocol_version": clientlink.ProtocolVersion}}
	if token != "" {
		connect["token"] = token
	}
	frame, err := json.Marshal(map[string]any{"id": 1, "connect": connect})
	if err != nil {
		t.Fatalf("marshal connect frame: %v", err)
	}
	return frame
}

// writeTextFrame writes one masked client text frame (RFC 6455 section 5.2).
func writeTextFrame(t *testing.T, conn net.Conn, payload []byte) {
	t.Helper()

	header := []byte{0x81}
	switch n := len(payload); {
	case n < 126:
		header = append(header, byte(0x80|n))
	case n <= 0xFFFF:
		header = append(header, 0x80|126, byte(n>>8), byte(n))
	default:
		t.Fatalf("frame of %d bytes is larger than this helper writes", n)
	}
	mask := []byte{0x12, 0x34, 0x56, 0x78}
	header = append(header, mask...)
	masked := make([]byte, len(payload))
	for i, b := range payload {
		masked[i] = b ^ mask[i%4]
	}
	if _, err := conn.Write(append(header, masked...)); err != nil {
		t.Fatalf("write frame: %v", err)
	}
}

// readFrame reads one unmasked server frame and returns its opcode and payload.
func readFrame(t *testing.T, conn net.Conn) (byte, []byte) {
	t.Helper()

	head := make([]byte, 2)
	if _, err := io.ReadFull(conn, head); err != nil {
		t.Fatalf("read frame header: %v", err)
	}
	opcode := head[0] & 0x0F
	length := int(head[1] & 0x7F)
	switch length {
	case 126:
		ext := make([]byte, 2)
		if _, err := io.ReadFull(conn, ext); err != nil {
			t.Fatalf("read extended length: %v", err)
		}
		length = int(binary.BigEndian.Uint16(ext))
	case 127:
		ext := make([]byte, 8)
		if _, err := io.ReadFull(conn, ext); err != nil {
			t.Fatalf("read extended length: %v", err)
		}
		length = int(binary.BigEndian.Uint64(ext))
	}
	if head[1]&0x80 != 0 {
		t.Fatal("a server frame arrived masked, which RFC 6455 forbids")
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(conn, payload); err != nil {
		t.Fatalf("read frame payload: %v", err)
	}
	return opcode, payload
}
