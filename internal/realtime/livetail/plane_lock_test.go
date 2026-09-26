package livetail

import (
	"context"
	"sync"
	"testing"
	"time"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

func TestClassificationDoesNotHoldPlaneLock(t *testing.T) {
	p := &Plane{limit: 8, timeout: time.Second, sessions: make(map[sessionKey]*sessionState)}
	firstTenant := sessionwire.TenantID("tenant-a")
	secondTenant := sessionwire.TenantID("tenant-b")
	session := sessionwire.SessionID("session-a")
	p.Watching(firstTenant, session)
	p.Watching(secondTenant, session)
	first := p.newSink(firstTenant, session)
	second := p.newSink(secondTenant, session)
	first.Subscribed()
	second.Subscribed()

	entered := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan struct{})
	secondDone := make(chan struct{})
	var once sync.Once
	open := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() {
		open()
		<-firstDone
		<-secondDone
		_ = p.Close(context.Background())
	})
	first.classify = func(sessionKey, []byte) bool {
		close(entered)
		<-release
		return false
	}
	go func() {
		first.Publication([]byte(`{"type":"blocked"}`))
		close(firstDone)
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first publication never entered classification")
	}
	go func() {
		second.Publication([]byte(`{"type":"other"}`))
		close(secondDone)
	}()
	select {
	case <-secondDone:
	case <-time.After(2 * time.Second):
		t.Fatal("another session's publication blocked on classification")
	}
}
