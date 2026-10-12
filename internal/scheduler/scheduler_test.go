package scheduler

import (
	"strings"
	"testing"

	"github.com/anusha2211/instacloud-runtime/internal/api"
)

func mkNode(name, zone string, cpu, mem int64) api.Node {
	r := api.Resources{MilliCPU: cpu, MemoryMB: mem}
	return api.Node{Meta: api.Meta{Name: name}, Zone: zone, Capacity: r, Allocatable: r, Status: api.NodeReady}
}

func mkInst(name, workload, node string, cpu, mem int64) api.Instance {
	return api.Instance{
		Meta:     api.Meta{Name: name},
		Workload: workload,
		Ordinal:  -1,
		Node:     node,
		Spec:     api.WorkloadSpec{Resources: api.Resources{MilliCPU: cpu, MemoryMB: mem}},
		Status:   api.InstanceStatus{Phase: api.PhaseRunning},
	}
}

func pending(workload string, cpu, mem int64) *api.Instance {
	i := mkInst("p", workload, "", cpu, mem)
	i.Status.Phase = api.PhasePending
	return &i
}

func TestFiltersAndScoring(t *testing.T) {
	tests := []struct {
		name     string
		nodes    []api.Node
		existing []api.Instance
		inst     func() *api.Instance
		want     string // node, or substring of error
		wantErr  bool
	}{
		{
			name:  "skips not-ready node",
			nodes: []api.Node{func() api.Node { n := mkNode("a", "z1", 4000, 8192); n.Status = api.NodeNotReady; return n }(), mkNode("b", "z1", 4000, 8192)},
			inst:  func() *api.Instance { return pending("web", 500, 512) },
			want:  "b",
		},
		{
			name:     "skips node without room",
			nodes:    []api.Node{mkNode("a", "z1", 1000, 1024), mkNode("b", "z1", 4000, 8192)},
			existing: []api.Instance{mkInst("x", "db", "a", 800, 256)},
			inst:     func() *api.Instance { return pending("web", 500, 512) },
			want:     "b",
		},
		{
			name:  "bin-packs different workloads onto the fuller node",
			nodes: []api.Node{mkNode("a", "z1", 4000, 8192), mkNode("b", "z1", 4000, 8192)},
			existing: []api.Instance{
				mkInst("x", "db", "b", 2000, 4096),
			},
			inst: func() *api.Instance { return pending("web", 500, 512) },
			want: "b",
		},
		{
			name:  "spreads replicas of the same workload",
			nodes: []api.Node{mkNode("a", "z1", 4000, 8192), mkNode("b", "z1", 4000, 8192)},
			existing: []api.Instance{
				mkInst("w1", "web", "b", 500, 512),
			},
			inst: func() *api.Instance { return pending("web", 500, 512) },
			want: "a",
		},
		{
			name:  "prefers a different zone",
			nodes: []api.Node{mkNode("a", "z1", 4000, 8192), mkNode("b", "z1", 4000, 8192), mkNode("c", "z2", 4000, 8192)},
			existing: []api.Instance{
				mkInst("w1", "web", "a", 500, 512),
			},
			inst: func() *api.Instance { return pending("web", 500, 512) },
			want: "c",
		},
		{
			name:  "anti-affinity is a hard constraint",
			nodes: []api.Node{mkNode("a", "z1", 4000, 8192)},
			existing: []api.Instance{
				mkInst("w1", "web", "a", 500, 512),
			},
			inst: func() *api.Instance {
				p := pending("web", 500, 512)
				p.Spec.Placement.AntiAffinity = true
				return p
			},
			want:    "anti-affinity",
			wantErr: true,
		},
		{
			name: "node selector",
			nodes: []api.Node{mkNode("a", "z1", 4000, 8192), func() api.Node {
				n := mkNode("b", "z1", 4000, 8192)
				n.Labels = map[string]string{"disk": "ssd"}
				return n
			}()},
			inst: func() *api.Instance {
				p := pending("db", 500, 512)
				p.Spec.Placement.NodeSelector = map[string]string{"disk": "ssd"}
				return p
			},
			want: "b",
		},
		{
			name:    "explains why nothing fits",
			nodes:   []api.Node{mkNode("a", "z1", 1000, 1024), mkNode("b", "z1", 4000, 512)},
			inst:    func() *api.Instance { return pending("web", 2000, 2048) },
			want:    "0/2 nodes available",
			wantErr: true,
		},
		{
			name:  "terminated instances free their capacity",
			nodes: []api.Node{mkNode("a", "z1", 1000, 1024)},
			existing: []api.Instance{func() api.Instance {
				i := mkInst("old", "web", "a", 1000, 1024)
				i.Status.Phase = api.PhaseTerminated
				return i
			}()},
			inst: func() *api.Instance { return pending("web", 1000, 1024) },
			want: "a",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewCluster(tt.nodes, tt.existing, nil)
			got, _, err := New().Schedule(tt.inst(), c)
			if tt.wantErr {
				if err == nil || !strings.Contains(err.Error(), tt.want) {
					t.Fatalf("want error containing %q, got node=%q err=%v", tt.want, got, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("got %q want %q", got, tt.want)
			}
		})
	}
}

func TestVolumeLocality(t *testing.T) {
	nodes := []api.Node{mkNode("a", "z1", 4000, 8192), mkNode("b", "z1", 4000, 8192)}
	vols := []api.Volume{{Meta: api.Meta{Name: "db-data-0"}, BoundNode: "b", Phase: api.VolumeBound}}
	inst := pending("db", 500, 512)
	inst.Ordinal = 0
	inst.Volumes = []string{"db-data-0"}

	got, _, err := New().Schedule(inst, NewCluster(nodes, nil, vols))
	if err != nil || got != "b" {
		t.Fatalf("want pinned to b, got %q %v", got, err)
	}

	// Bound node is dead: the replica may move.
	nodes[1].Status = api.NodeNotReady
	got, _, err = New().Schedule(inst, NewCluster(nodes, nil, vols))
	if err != nil || got != "a" {
		t.Fatalf("want a after b died, got %q %v", got, err)
	}
}

func TestAssumeAccountsWithinPass(t *testing.T) {
	nodes := []api.Node{mkNode("a", "z1", 1000, 1024), mkNode("b", "z1", 1000, 1024)}
	c := NewCluster(nodes, nil, nil)
	s := New()
	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		p := pending("web", 1000, 1024)
		n, _, err := s.Schedule(p, c)
		if err != nil {
			t.Fatal(err)
		}
		c.Assume(p, n)
		seen[n] = true
	}
	if len(seen) != 2 {
		t.Fatalf("expected both nodes used, got %v", seen)
	}
	if _, _, err := s.Schedule(pending("web", 1000, 1024), c); err == nil {
		t.Fatal("third replica must not fit")
	}
}
