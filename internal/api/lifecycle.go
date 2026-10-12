package api

// transitions is the instance lifecycle state machine. Keys are the current
// phase, values the phases it may move to. Terminating is reachable from every
// live phase (scale-down, rollout, delete) and Lost from every phase that may
// be running on a node.
var transitions = map[Phase][]Phase{
	PhasePending:     {PhaseScheduled, PhaseTerminating, PhaseTerminated},
	PhaseScheduled:   {PhaseStarting, PhaseRunning, PhaseFailed, PhaseLost, PhaseTerminating, PhasePending},
	PhaseStarting:    {PhaseRunning, PhaseFailed, PhaseUnhealthy, PhaseLost, PhaseTerminating},
	PhaseRunning:     {PhaseUnhealthy, PhaseFailed, PhaseLost, PhaseTerminating, PhaseStarting},
	PhaseUnhealthy:   {PhaseRunning, PhaseStarting, PhaseFailed, PhaseLost, PhaseTerminating},
	PhaseFailed:      {PhaseStarting, PhaseRunning, PhaseLost, PhaseTerminating},
	PhaseLost:        {PhaseTerminating, PhaseTerminated, PhasePending},
	PhaseTerminating: {PhaseTerminated},
	PhaseTerminated:  {},
}

// CanTransition reports whether an instance may move from one phase to another.
// Same-phase updates (e.g. readiness flips) are always allowed.
func CanTransition(from, to Phase) bool {
	if from == to {
		return true
	}
	for _, p := range transitions[from] {
		if p == to {
			return true
		}
	}
	return false
}

// Terminal reports whether the phase is final.
func (p Phase) Terminal() bool { return p == PhaseTerminated }

// Live reports whether an instance in this phase counts toward replicas.
func (p Phase) Live() bool {
	switch p {
	case PhaseTerminating, PhaseTerminated, PhaseLost:
		return false
	}
	return true
}
