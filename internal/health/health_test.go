package health

import (
	"testing"
	"time"
)

func TestUnarmedTrackerIsReady(t *testing.T) {
	tr := NewStreamTracker(time.Minute)
	if err := tr.Check(nil); err != nil {
		t.Fatalf("unarmed tracker must be ready (standalone mode), got %v", err)
	}
}

func TestConnectedTrackerIsReady(t *testing.T) {
	tr := NewStreamTracker(time.Minute)
	tr.SetConnected(true)
	if err := tr.Check(nil); err != nil {
		t.Fatalf("connected tracker must be ready, got %v", err)
	}
}

func TestDisconnectWithinGraceStaysReady(t *testing.T) {
	now := time.Now()
	tr := NewStreamTracker(time.Minute)
	tr.now = func() time.Time { return now }

	tr.SetConnected(true)
	tr.SetConnected(false)

	now = now.Add(30 * time.Second)
	if err := tr.Check(nil); err != nil {
		t.Fatalf("disconnect within grace must stay ready, got %v", err)
	}
}

func TestDisconnectPastGraceIsUnready(t *testing.T) {
	now := time.Now()
	tr := NewStreamTracker(time.Minute)
	tr.now = func() time.Time { return now }

	tr.SetConnected(true)
	tr.SetConnected(false)

	now = now.Add(61 * time.Second)
	if err := tr.Check(nil); err == nil {
		t.Fatal("disconnect past grace must be unready")
	}
}

func TestNeverConnectedArmsUnreadyAfterGrace(t *testing.T) {
	now := time.Now()
	tr := NewStreamTracker(time.Minute)
	tr.now = func() time.Time { return now }

	// The reconciler arms the tracker disconnected when the stream client is
	// built: initial bootstrap gets the grace window to connect, then flaps.
	tr.SetConnected(false)
	if err := tr.Check(nil); err != nil {
		t.Fatalf("within initial connect grace must be ready, got %v", err)
	}
	now = now.Add(61 * time.Second)
	if err := tr.Check(nil); err == nil {
		t.Fatal("never connected past grace must be unready")
	}
}

func TestReconnectRestoresReadiness(t *testing.T) {
	now := time.Now()
	tr := NewStreamTracker(time.Minute)
	tr.now = func() time.Time { return now }

	tr.SetConnected(true)
	tr.SetConnected(false)
	now = now.Add(90 * time.Second)
	if err := tr.Check(nil); err == nil {
		t.Fatal("expected unready past grace")
	}
	tr.SetConnected(true)
	if err := tr.Check(nil); err != nil {
		t.Fatalf("reconnect must restore readiness, got %v", err)
	}
}

func TestRepeatedDisconnectResetsGraceWindow(t *testing.T) {
	now := time.Now()
	tr := NewStreamTracker(time.Minute)
	tr.now = func() time.Time { return now }

	tr.SetConnected(true)
	tr.SetConnected(false)
	now = now.Add(50 * time.Second)
	tr.SetConnected(true)
	tr.SetConnected(false)
	now = now.Add(50 * time.Second) // 100s after first disconnect, 50s after last
	if err := tr.Check(nil); err != nil {
		t.Fatalf("grace window must restart on each disconnect, got %v", err)
	}
}
