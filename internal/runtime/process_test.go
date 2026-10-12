package runtime

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/anusha2211/instacloud-runtime/internal/api"
)

func newTestProcess(t *testing.T) *Process {
	t.Helper()
	return NewProcess(t.TempDir(), "instacloud-test", slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func waitExit(t *testing.T, r Runtime, id string) State {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		st, err := r.Status(id)
		if err == nil && !st.Running {
			return st
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s did not exit", id)
	return State{}
}

func TestProcessExitCodeAndEnv(t *testing.T) {
	p := newTestProcess(t)
	vol := t.TempDir()
	spec := InstanceSpec{
		ID: "job-0", Workload: "job", Ordinal: 0,
		Command:   []string{"sh", "-c", `echo "$INSTANCE_ID $INSTANCE_ORDINAL $PORT $GREETING" > "$DATA_DIR/out"; exit 3`},
		Env:       map[string]string{"GREETING": "hi"},
		Port:      9999,
		Resources: api.Resources{MilliCPU: 100, MemoryMB: 64},
		Volumes:   map[string]string{"data": vol},
	}
	if err := p.Start(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	st := waitExit(t, p, "job-0")
	if st.ExitCode != 3 || st.Reason != "Exited" {
		t.Fatalf("got %+v", st)
	}
	b, _ := os.ReadFile(filepath.Join(vol, "out"))
	if strings.TrimSpace(string(b)) != "job-0 0 9999 hi" {
		t.Fatalf("env not passed: %q", b)
	}
	if err := p.Remove("job-0"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Status("job-0"); err != ErrNotFound {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestProcessGracefulStopThenKill(t *testing.T) {
	p := newTestProcess(t)
	// Ignores SIGTERM, so Stop must escalate to SIGKILL after grace.
	spec := InstanceSpec{ID: "stubborn", Ordinal: -1, Command: []string{"sh", "-c", "trap '' TERM; while true; do sleep 0.05; done"},
		Resources: api.Resources{MilliCPU: 100, MemoryMB: 64}}
	if err := p.Start(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	if err := p.Stop(context.Background(), "stubborn", 200*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	st, _ := p.Status("stubborn")
	if st.Running || st.Reason != "Killed" {
		t.Fatalf("got %+v", st)
	}
	if d := time.Since(start); d < 200*time.Millisecond {
		t.Fatalf("killed before grace period: %v", d)
	}
}

func TestCPUMax(t *testing.T) {
	cases := map[int64]string{500: "50000 100000", 2000: "200000 100000", 5: "1000 100000"}
	for in, want := range cases {
		if got := CPUMax(in); got != want {
			t.Errorf("CPUMax(%d)=%q want %q", in, got, want)
		}
	}
}

func TestCgroupFilesWritten(t *testing.T) {
	// Fake a cgroup v2 hierarchy in a temp dir to test the file protocol.
	base := t.TempDir()
	_ = os.WriteFile(filepath.Join(base, "cgroup.controllers"), []byte("cpuset cpu io memory pids"), 0o644)
	cg, err := DetectCgroups(base, "ic")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cg.Create("web-1", api.Resources{MilliCPU: 250, MemoryMB: 128}); err != nil {
		t.Fatal(err)
	}
	read := func(f string) string {
		b, _ := os.ReadFile(filepath.Join(base, "ic", "web-1", f))
		return string(b)
	}
	if read("cpu.max") != "25000 100000" || read("memory.max") != "134217728" {
		t.Fatalf("cpu.max=%q memory.max=%q", read("cpu.max"), read("memory.max"))
	}
	_ = os.WriteFile(filepath.Join(base, "ic", "web-1", "memory.events"), []byte("low 0\nhigh 0\nmax 4\noom 1\noom_kill 1\n"), 0o644)
	if !cg.OOMKilled("web-1") {
		t.Fatal("expected OOM detection")
	}
}

func TestDetectCgroupsMissing(t *testing.T) {
	if _, err := DetectCgroups(t.TempDir(), "ic"); err == nil {
		t.Fatal("expected error without cgroup.controllers")
	}
}
