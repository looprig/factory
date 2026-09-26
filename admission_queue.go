package factory

import (
	"context"
	"sync"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/internal/placement"
)

type admissionKey struct {
	tenant  sessionwire.TenantID
	session sessionwire.SessionID
}

type queuedAdmission struct {
	notice   placement.AdmissionNotice
	inFlight bool
	dirty    bool
}

// admissionQueue coalesces committed notices by session. A new notice during
// an in-flight attempt schedules one recheck at the tail of the FIFO.
type admissionQueue struct {
	mu    sync.Mutex
	ready chan struct{}
	limit int
	order []admissionKey
	items map[admissionKey]*queuedAdmission
}

func newAdmissionQueue(limit int) *admissionQueue {
	return &admissionQueue{ready: make(chan struct{}, 1), limit: limit, items: make(map[admissionKey]*queuedAdmission)}
}

func (q *admissionQueue) signal() {
	select {
	case q.ready <- struct{}{}:
	default:
	}
}

func (q *admissionQueue) enqueue(notice placement.AdmissionNotice) {
	if !notice.Pending {
		return
	}
	key := admissionKey{tenant: notice.TenantID, session: notice.SessionID}
	q.mu.Lock()
	defer q.mu.Unlock()
	if item := q.items[key]; item != nil {
		item.notice = notice
		if item.inFlight {
			item.dirty = true
		}
		return
	}
	if len(q.order) >= q.limit {
		return
	}
	q.items[key] = &queuedAdmission{notice: notice}
	q.order = append(q.order, key)
	q.signal()
}

func (q *admissionQueue) next(ctx context.Context) (placement.AdmissionNotice, admissionKey, bool) {
	for {
		q.mu.Lock()
		if len(q.order) > 0 {
			key := q.order[0]
			q.order = q.order[1:]
			item := q.items[key]
			item.inFlight = true
			notice := item.notice
			q.mu.Unlock()
			return notice, key, true
		}
		q.mu.Unlock()
		select {
		case <-ctx.Done():
			return placement.AdmissionNotice{}, admissionKey{}, false
		case <-q.ready:
		}
	}
}

func (q *admissionQueue) finish(key admissionKey) {
	q.mu.Lock()
	defer q.mu.Unlock()
	item := q.items[key]
	if item == nil {
		return
	}
	if !item.dirty {
		delete(q.items, key)
		return
	}
	item.inFlight = false
	item.dirty = false
	q.order = append(q.order, key)
	q.signal()
}
