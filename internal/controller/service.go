package controller

import (
	"sort"

	"github.com/anusha2211/instacloud-runtime/internal/api"
)

// reconcileService converges a stateless workload: interchangeable replicas,
// rolling updates with maxSurge=1 / maxUnavailable=0.
func (c *Controller) reconcileService(wl *api.Workload, insts []*api.Instance) {
	want := wl.Spec.Replicas
	var cur, old []*api.Instance
	for _, in := range insts {
		switch {
		case !in.Status.Phase.Live():
		case in.Generation == wl.Generation:
			cur = append(cur, in)
		default:
			old = append(old, in)
		}
	}
	readyCur, readyOld := countReady(cur), countReady(old)

	// 1. Create missing current-generation replicas. During a rollout only
	//    surge by one above the desired count.
	missing := want - len(cur)
	if len(old) > 0 {
		surge := want + 1 - (len(cur) + len(old))
		if surge < missing {
			missing = surge
		}
	}
	for i := 0; i < missing; i++ {
		_ = c.newInstance(wl, wl.Name+"-"+randSuffix(), -1, nil)
	}

	// 2. Retire old-generation replicas without dropping ready capacity below
	//    the desired count. Unready old replicas contribute nothing; remove
	//    them first.
	sortForRemoval(old)
	ready := readyCur + readyOld
	for _, in := range old {
		if !in.Status.Ready {
			c.terminate(in, "rollout: replaced by generation "+itoa(wl.Generation))
			continue
		}
		if ready-1 >= want {
			c.terminate(in, "rollout: replaced by generation "+itoa(wl.Generation))
			ready--
		}
	}

	// 3. Scale down surplus current replicas (least useful first).
	if extra := len(cur) - want; extra > 0 {
		sortForRemoval(cur)
		for _, in := range cur[:extra] {
			c.terminate(in, "scale down")
		}
	}
}

func countReady(insts []*api.Instance) int {
	n := 0
	for _, in := range insts {
		if in.Status.Ready {
			n++
		}
	}
	return n
}

// sortForRemoval orders instances so the cheapest to lose come first:
// unscheduled, then unready, then most recently created.
func sortForRemoval(insts []*api.Instance) {
	rank := func(in *api.Instance) int {
		switch {
		case in.Node == "":
			return 0
		case !in.Status.Ready:
			return 1
		}
		return 2
	}
	sort.SliceStable(insts, func(i, j int) bool {
		ri, rj := rank(insts[i]), rank(insts[j])
		if ri != rj {
			return ri < rj
		}
		return insts[i].CreatedAt.After(insts[j].CreatedAt)
	})
}
