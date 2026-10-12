package api

import (
	"fmt"
	"regexp"
	"strings"
)

var nameRE = regexp.MustCompile(`^[a-z]([a-z0-9-]{0,38}[a-z0-9])?$`)

// ValidName checks DNS-label-style names.
func ValidName(n string) error {
	if !nameRE.MatchString(n) {
		return fmt.Errorf("invalid name %q: must be lowercase alphanumeric or '-', start with a letter, max 40 chars", n)
	}
	return nil
}

// Defaults fills in optional fields.
func (s *WorkloadSpec) Defaults() {
	if s.Kind == "" {
		s.Kind = KindService
	}
	if s.HealthCheck != nil {
		if s.HealthCheck.IntervalSeconds == 0 {
			s.HealthCheck.IntervalSeconds = 2
		}
		if s.HealthCheck.TimeoutSeconds == 0 {
			s.HealthCheck.TimeoutSeconds = 1
		}
		if s.HealthCheck.FailureThreshold == 0 {
			s.HealthCheck.FailureThreshold = 3
		}
	}
	if s.TerminationGraceSeconds == 0 {
		s.TerminationGraceSeconds = 10
	}
	if s.Autoscale != nil && s.Replicas < s.Autoscale.MinReplicas {
		s.Replicas = s.Autoscale.MinReplicas
	}
}

// Validate returns all problems with a workload, joined.
func (w *Workload) Validate() error {
	var errs []string
	add := func(f string, a ...any) { errs = append(errs, fmt.Sprintf(f, a...)) }
	if err := ValidName(w.Name); err != nil {
		add("%v", err)
	}
	s := &w.Spec
	if s.Kind != KindService && s.Kind != KindStateful {
		add("spec.kind must be %q or %q", KindService, KindStateful)
	}
	if s.Replicas < 0 || s.Replicas > 1000 {
		add("spec.replicas must be in [0,1000]")
	}
	if len(s.Command) == 0 {
		add("spec.command is required")
	}
	if s.Resources.MilliCPU <= 0 || s.Resources.MemoryMB <= 0 {
		add("spec.resources.milliCPU and memoryMB must be > 0")
	}
	if len(s.Volumes) > 0 && s.Kind != KindStateful {
		add("spec.volumes is only allowed for kind=stateful")
	}
	seen := map[string]bool{}
	for i, v := range s.Volumes {
		if err := ValidName(v.Name); err != nil {
			add("spec.volumes[%d]: %v", i, err)
		}
		if seen[v.Name] {
			add("spec.volumes[%d]: duplicate name %q", i, v.Name)
		}
		seen[v.Name] = true
		if v.SizeMB <= 0 {
			add("spec.volumes[%d].sizeMB must be > 0", i)
		}
	}
	if h := s.HealthCheck; h != nil {
		if !strings.HasPrefix(h.Path, "/") {
			add("spec.healthCheck.path must start with /")
		}
		if !s.Port {
			add("spec.healthCheck requires spec.port=true")
		}
	}
	if a := s.Autoscale; a != nil {
		if s.Kind == KindStateful {
			add("spec.autoscale is not supported for stateful workloads")
		}
		if a.MinReplicas < 1 || a.MaxReplicas < a.MinReplicas {
			add("spec.autoscale: need 1 <= minReplicas <= maxReplicas")
		}
		if a.TargetRPSPerReplica <= 0 {
			add("spec.autoscale.targetRPSPerReplica must be > 0")
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return nil
}

// SpecChanged reports whether two specs differ in anything other than the
// replica count, i.e. whether an update requires a rollout.
func SpecChanged(a, b WorkloadSpec) bool {
	a.Replicas, b.Replicas = 0, 0
	return !jsonEqual(a, b)
}
