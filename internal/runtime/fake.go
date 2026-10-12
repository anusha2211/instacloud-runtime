package runtime

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Fake is an in-memory Runtime for tests. Instances "run" until Crash or Stop.
type Fake struct {
	mu        sync.Mutex
	instances map[string]*fakeInst
	Starts    map[string]int
	FailStart map[string]error
}

type fakeInst struct {
	spec  InstanceSpec
	state State
}

func NewFake() *Fake {
	return &Fake{instances: map[string]*fakeInst{}, Starts: map[string]int{}, FailStart: map[string]error{}}
}

func (f *Fake) Name() string { return "fake" }

func (f *Fake) Start(_ context.Context, spec InstanceSpec) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.FailStart[spec.ID]; err != nil {
		return err
	}
	if in, ok := f.instances[spec.ID]; ok && in.state.Running {
		return fmt.Errorf("%s already running", spec.ID)
	}
	f.Starts[spec.ID]++
	f.instances[spec.ID] = &fakeInst{spec: spec, state: State{Running: true, PID: 1000 + f.Starts[spec.ID], StartedAt: time.Now(), Enforced: true}}
	return nil
}

func (f *Fake) Stop(_ context.Context, id string, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if in, ok := f.instances[id]; ok && in.state.Running {
		in.state.Running = false
		in.state.Reason = "Stopped"
		in.state.ExitedAt = time.Now()
	}
	return nil
}

func (f *Fake) Status(id string) (State, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	in, ok := f.instances[id]
	if !ok {
		return State{}, ErrNotFound
	}
	return in.state, nil
}

func (f *Fake) Remove(id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.instances, id)
	return nil
}

// Crash simulates the instance exiting with code and reason.
func (f *Fake) Crash(id string, code int, reason string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if in, ok := f.instances[id]; ok {
		in.state.Running = false
		in.state.ExitCode = code
		in.state.Reason = reason
		in.state.ExitedAt = time.Now()
	}
}

// Spec returns the spec an instance was last started with.
func (f *Fake) Spec(id string) (InstanceSpec, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	in, ok := f.instances[id]
	if !ok {
		return InstanceSpec{}, false
	}
	return in.spec, true
}
