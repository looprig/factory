package admission

import (
	"testing"

	sessionwire "github.com/looprig/core/sessionwire/v1"
)

// TestBeginHandsBackTheChosenShardsPositionAndDropsStaleOnesInRange covers the
// v0.3.0 regate's G2 and G3 on the rotor directly.
//
// G3: begin returns the position kept for the shard IT CHOSE, not a
// neighbour's; a mis-routed position is refused by the store and silently
// degrades into a walk from the head. G2: a count that shrinks while the rotor
// is still inside the new range (4 -> 3 with the rotor at 1) drops the
// vanished shard's position on that same pass, so a regrown shard 3 starts at
// the head rather than resuming into a rebuilt view.
func TestBeginHandsBackTheChosenShardsPositionAndDropsStaleOnesInRange(t *testing.T) {
	t.Parallel()

	r := &rotor{next: 1, cursors: map[int]sessionwire.Cursor{0: "c0", 1: "c1", 2: "c2", 3: "c3"}}
	shard, cursor, err := r.begin(3)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if shard != 1 || cursor != "c1" {
		t.Fatalf("begin(3) with the rotor at 1 = (%d, %q), want (1, \"c1\")", shard, cursor)
	}
	if _, kept := r.cursors[3]; kept {
		t.Fatal("a shrink while the rotor was in range kept the vanished shard's position")
	}
	for want := 2; want <= 3; want++ {
		shard, cursor, err = r.begin(4)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		if shard != want {
			t.Fatalf("rotor = %d, want %d", shard, want)
		}
	}
	if cursor != "" {
		t.Fatalf("the regrown shard 3 resumed from %q, want the head", cursor)
	}
}
