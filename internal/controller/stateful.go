package controller

import (
	"github.com/anusha2211/instacloud-runtime/internal/api"
)

// reconcileStateful converges a stateful workload: ordinals 0..N-1 with
// stable names and one volume per (ordinal, volume spec). Changes happen one
// ordinal at a time: create in ascending order once the previous ordinal is
// ready, scale down and roll out in descending order.
func (c *Controller) reconcileStateful(wl *api.Workload, insts []*api.Instance, s *state) {
	want := wl.Spec.Replicas
	byOrd := map[int]*api.Instance{}
	busy := false // an ordinal is mid-transition; do one thing at a time
	for _, in := range insts {
		if in.Status.Phase == api.PhaseTerminated {
			continue
		}
		byOrd[in.Ordinal] = in
		switch in.Status.Phase {
		case api.PhaseTerminating, api.PhaseLost:
			busy = true
		}
	}

	// Volumes are created ahead of instances and are never deleted on scale
	// down: scaling back up reattaches the same data to the same ordinal.
	for ord := 0; ord < want; ord++ {
		for _, vs := range wl.Spec.Volumes {
			name := api.VolumeName(wl.Name, vs.Name, ord)
			if _, ok := s.volumes[name]; ok {
				continue
			}
			v := &api.Volume{Meta: api.Meta{Name: name}, Workload: wl.Name, Ordinal: ord,
				SizeMB: vs.SizeMB, MountPath: vs.MountPath, Phase: api.VolumeAvailable}
			if err := c.Store.Create(api.KindVolume, v); err == nil {
				instanceActions.Inc("create_volume")
				c.Log.Info("volume created", "volume", name, "workload", wl.Name, "ordinal", ord)
			}
		}
	}

	// Scale down: highest ordinal first, one at a time.
	highest := -1
	for ord := range byOrd {
		if ord > highest {
			highest = ord
		}
	}
	if highest >= want {
		if !busy {
			c.terminate(byOrd[highest], "scale down")
		}
		return
	}

	// Scale up / repair: lowest missing ordinal, only when all lower ordinals
	// are running and ready.
	for ord := 0; ord < want; ord++ {
		in, ok := byOrd[ord]
		if !ok {
			if busy {
				return
			}
			var vols []string
			for _, vs := range wl.Spec.Volumes {
				vols = append(vols, api.VolumeName(wl.Name, vs.Name, ord))
			}
			_ = c.newInstance(wl, api.InstanceName(wl.Name, ord), ord, vols)
			return
		}
		if !(in.Status.Phase == api.PhaseRunning && in.Status.Ready) {
			return // wait for this ordinal before touching higher ones
		}
	}

}
