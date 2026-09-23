package livetail_test

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// syncBuffer is a bytes.Buffer safe for the drainer to write while a test reads.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestAHostRecordNamingAnotherSessionOrTenantIsRefusedAndRepaired is F1 of the
// tests-lane wire freeze, end to end over a real HostLink. A Host publishes, on
// this session's channel, a record naming ANOTHER session and then one naming
// ANOTHER TENANT. Neither may reach this session's viewers; each is a hole in
// the stream and is repaired like any refused record -- the viewers get a
// session.reset -- and each is logged as a foreign record.
func TestAHostRecordNamingAnotherSessionOrTenantIsRefusedAndRepaired(t *testing.T) {
	t.Parallel()

	logs := &syncBuffer{}
	host := newStandIn(t, "host-1")
	r := newRig(t, rigOptions{logger: slog.New(slog.NewTextHandler(logs, nil))})
	r.dir.put(host.observation(tenantA, session, 3))
	r.watch(t, tenantA, session)
	channel := sessionwire.HostLinkChannel(tenantA, session)

	host.publish(t, channel, enduring(t, tenantA, session, 1))
	r.wait(t, "E1", func() bool { return len(r.viewers.of(tenantA, session)) == 1 })

	r.tips.set(3)
	host.publish(t, channel, enduring(t, tenantA, "s-foreign", 2))
	r.wait(t, "the reset for the foreign session's record", func() bool {
		return strings.Contains(joined(kinds(t, r.viewers.of(tenantA, session))), "R1/3")
	})
	r.wait(t, "the tail to be re-subscribed", func() bool { return host.count("subscribe", channel) >= 2 })

	r.tips.set(4)
	host.publish(t, channel, enduring(t, tenantB, session, 4))
	r.wait(t, "the reset for the foreign tenant's record", func() bool {
		return strings.Contains(joined(kinds(t, r.viewers.of(tenantA, session))), "R1/4")
	})

	// The control: the repaired tail still carries the session's own records.
	host.publish(t, channel, enduring(t, tenantA, session, 5))
	r.wait(t, "E5 after the repairs", func() bool {
		return strings.HasSuffix(joined(kinds(t, r.viewers.of(tenantA, session))), "E5")
	})
	for _, record := range r.viewers.of(tenantA, session) {
		if strings.Contains(record, "s-foreign") || strings.Contains(record, string(tenantB)) {
			t.Fatalf("a foreign record reached the viewers: %s", record)
		}
	}
	if got := strings.Count(logs.String(), "named another tenant or session"); got != 2 {
		t.Fatalf("logged %d foreign records, want 2:\n%s", got, logs.String())
	}
}
