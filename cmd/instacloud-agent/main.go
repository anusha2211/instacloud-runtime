// Command instacloud-agent runs on every machine. It registers the node with
// the control plane, runs assigned instances through the configured runtime
// and reports their status.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"errors"
	"github.com/anusha2211/instacloud-runtime/internal/agent"
	"github.com/anusha2211/instacloud-runtime/internal/api"
	"github.com/anusha2211/instacloud-runtime/internal/client"
	"github.com/anusha2211/instacloud-runtime/internal/logging"
	"github.com/anusha2211/instacloud-runtime/internal/runtime"
	"net/http"

	"github.com/anusha2211/instacloud-runtime/internal/metrics"
)

func main() {
	hostname, _ := os.Hostname()
	var (
		name        = flag.String("name", strings.ToLower(hostname), "node name")
		apiURL      = flag.String("api", "http://127.0.0.1:7070", "control plane URL")
		address     = flag.String("address", "127.0.0.1", "address at which instances on this node are reachable")
		zone        = flag.String("zone", "zone-a", "failure zone")
		labels      = flag.String("labels", "", "comma-separated key=value node labels")
		cpu         = flag.Int64("cpu", 4000, "node CPU capacity in millicores")
		mem         = flag.Int64("memory", 8192, "node memory capacity in MiB")
		resCPU      = flag.Int64("reserved-cpu", 250, "millicores reserved for the system")
		resMem      = flag.Int64("reserved-memory", 256, "MiB reserved for the system")
		portBase    = flag.Int("port-base", 20000, "first port of this node's instance port range")
		dataDir     = flag.String("data-dir", ".data", "root for instance logs")
		volRoot     = flag.String("volume-root", ".data/volumes", "root for volumes (shared between nodes in the demo, standing in for network block storage)")
		lease       = flag.Duration("lease", 10*time.Second, "self-fence stateful instances after this long without reaching the control plane")
		logLevel    = flag.String("log-level", "info", "debug|info|warn|error")
		metricsAddr = flag.String("metrics-listen", "", "serve /metrics on this address (empty to disable)")
	)
	flag.Parse()
	log := logging.New("agent", *logLevel)

	lbl := map[string]string{}
	for _, kv := range strings.Split(*labels, ",") {
		if k, v, ok := strings.Cut(kv, "="); ok {
			lbl[k] = v
		}
	}
	rt := runtime.NewProcess(filepath.Join(*dataDir, "logs", *name), "instacloud-"+*name, log)
	cfg := agent.Config{
		NodeName:      *name,
		Address:       *address,
		Zone:          *zone,
		Labels:        lbl,
		Capacity:      api.Resources{MilliCPU: *cpu, MemoryMB: *mem},
		Reserved:      api.Resources{MilliCPU: *resCPU, MemoryMB: *resMem},
		PortBase:      *portBase,
		VolumeRoot:    *volRoot,
		LeaseDuration: *lease,
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if *metricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics.Default.Handler())
		srv := &http.Server{Addr: *metricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("metrics server", "err", err)
			}
		}()
		defer srv.Close()
	}
	a := agent.New(cfg, client.New(*apiURL), rt, log)
	if err := a.Run(ctx); err != nil && ctx.Err() == nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
