package store

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/anusha2211/instacloud-runtime/internal/api"
)

func node(name string) *api.Node {
	return &api.Node{Meta: api.Meta{Name: name}, Status: api.NodeReady}
}

func TestCreateGetUpdateConflict(t *testing.T) {
	s := New()
	n := node("n1")
	if err := s.Create(api.KindNode, n); err != nil {
		t.Fatal(err)
	}
	if n.ResourceVersion != 1 || n.Generation != 1 {
		t.Fatalf("rv=%d gen=%d", n.ResourceVersion, n.Generation)
	}
	if err := s.Create(api.KindNode, node("n1")); !IsExists(err) {
		t.Fatalf("want exists, got %v", err)
	}

	var a, b api.Node
	_ = s.Get(api.KindNode, "n1", &a)
	_ = s.Get(api.KindNode, "n1", &b)
	a.Zone = "z1"
	if err := s.Update(api.KindNode, &a); err != nil {
		t.Fatal(err)
	}
	b.Zone = "z2"
	if err := s.Update(api.KindNode, &b); !IsConflict(err) {
		t.Fatalf("stale write must conflict, got %v", err)
	}
	var got api.Node
	_ = s.Get(api.KindNode, "n1", &got)
	if got.Zone != "z1" || got.ResourceVersion != 2 {
		t.Fatalf("got %+v", got)
	}
}

func TestMutateRetriesOnConflict(t *testing.T) {
	s := New()
	_ = s.Create(api.KindNode, node("n1"))
	calls := 0
	err := Mutate(s, api.KindNode, "n1", func(n *api.Node) bool {
		calls++
		if calls == 1 { // simulate a concurrent writer sneaking in
			var other api.Node
			_ = s.Get(api.KindNode, "n1", &other)
			other.Message = "concurrent"
			_ = s.Update(api.KindNode, &other)
		}
		n.Zone = "z9"
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("expected a retry, calls=%d", calls)
	}
	var got api.Node
	_ = s.Get(api.KindNode, "n1", &got)
	if got.Zone != "z9" || got.Message != "concurrent" {
		t.Fatalf("lost update: %+v", got)
	}
}

func TestWatch(t *testing.T) {
	s := New()
	ch, stop := s.Watch(api.KindNode)
	defer stop()
	_ = s.Create(api.KindWorkload, &api.Workload{Meta: api.Meta{Name: "w"}}) // other kind, filtered
	_ = s.Create(api.KindNode, node("n1"))
	_ = s.Delete(api.KindNode, "n1", 0)

	want := []EventType{Added, Deleted}
	for _, w := range want {
		select {
		case e := <-ch:
			if e.Type != w || e.Name != "n1" {
				t.Fatalf("got %+v want %s", e, w)
			}
		case <-time.After(time.Second):
			t.Fatal("timeout")
		}
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	s := New()
	_ = s.Create(api.KindNode, node("n1"))
	_ = s.Create(api.KindNode, node("n2"))
	p := filepath.Join(t.TempDir(), "state.json")
	if err := s.SaveSnapshot(p); err != nil {
		t.Fatal(err)
	}
	r := New()
	if err := r.LoadSnapshot(p); err != nil {
		t.Fatal(err)
	}
	nodes, _ := List[api.Node](r, api.KindNode)
	if len(nodes) != 2 || r.Revision() != 2 {
		t.Fatalf("nodes=%d rev=%d", len(nodes), r.Revision())
	}
	// CAS still works after restore
	n := nodes[0]
	n.Zone = "z"
	if err := r.Update(api.KindNode, &n); err != nil {
		t.Fatal(err)
	}
}
