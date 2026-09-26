package factory

import (
	"context"
	"testing"
	"time"

	"github.com/looprig/factory/internal/placement"
)

func TestAdmissionQueueKeepsColdSessionBehindWarmUpdates(t *testing.T) {
	q := newAdmissionQueue(2)
	warm := placement.AdmissionNotice{TenantID: "tenant-a", SessionID: "warm", CommandID: "create", Pending: true}
	cold := placement.AdmissionNotice{TenantID: "tenant-a", SessionID: "cold", CommandID: "input", Pending: true}
	q.enqueue(warm)
	first, key, ok := q.next(context.Background())
	if !ok {
		t.Fatal("queue closed")
	}
	if first.CommandID != "create" {
		t.Fatal(first)
	}
	for i := 0; i < 100; i++ {
		warm.CommandID = "input"
		q.enqueue(warm)
	}
	q.enqueue(cold)
	q.finish(key)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	second, key, ok := q.next(ctx)
	if !ok {
		t.Fatal("cold session absent")
	}
	if second.SessionID != "cold" {
		t.Fatalf("next session = %q, want cold", second.SessionID)
	}
	q.finish(key)
	third, _, ok := q.next(ctx)
	if !ok {
		t.Fatal("warm recheck absent")
	}
	if third.SessionID != "warm" || third.CommandID != "input" {
		t.Fatalf("recheck = %+v", third)
	}
}

func TestAdmissionQueueCoalescesQueuedCreateAndInput(t *testing.T) {
	q := newAdmissionQueue(2)
	q.enqueue(placement.AdmissionNotice{TenantID: "tenant-a", SessionID: "settled", CommandID: "old", Pending: false})
	q.enqueue(placement.AdmissionNotice{TenantID: "tenant-a", SessionID: "new", CommandID: "create", Pending: true})
	q.enqueue(placement.AdmissionNotice{TenantID: "tenant-a", SessionID: "new", CommandID: "input", Pending: true})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	entry, key, ok := q.next(ctx)
	if !ok {
		t.Fatal("queued session absent")
	}
	if entry.CommandID != "input" {
		t.Fatalf("coalesced notice = %+v", entry)
	}
	q.finish(key)
	if _, _, ok := q.next(ctx); ok {
		t.Fatal("a second placement or non-pending notice was queued")
	}
}
