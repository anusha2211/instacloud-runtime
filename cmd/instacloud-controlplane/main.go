// Command instacloud-controlplane runs the control plane: API server, state
// store, scheduler, workload reconciler, node monitor, autoscaler and (for
// the single-binary deployment used in development) the L7 router.
package main

import (
	"context"
	"errors"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/anusha2211/instacloud-runtime/internal/api"
	"github.com/anusha2211/instacloud-runtime/internal/apiserver"
	"github.com/anusha2211/instacloud-runtime/internal/autoscaler"
	"github.com/anusha2211/instacloud-runtime/internal/controller"
	"github.com/anusha2211/instacloud-runtime/internal/logging"
	"github.com/anusha2211/instacloud-runtime/internal/metrics"
	"github.com/anusha2211/instacloud-runtime/internal/nodemon"
	"github.com/anusha2211/instacloud-runtime/internal/router"
	"github.com/anusha2211/instacloud-runtime/internal/scheduler"
	"github.com/anusha2211/instacloud-runtime/internal/store"
)

func main() {
	var (
		listen     = flag.String("listen", ":7070", "API listen address")
		dataDir    = flag.String("data-dir", ".data/controlplane", "directory for state snapshots")
		logLevel   = flag.String("log-level", "info", "debug|info|warn|error")
		hbGrace    = flag.Duration("heartbeat-grace", 10*time.Second, "node heartbeat lease; no heartbeat for this long marks the node NotReady")
		fence      = flag.Duration("stateful-fence-delay", 12*time.Second, "wait after node loss before moving a stateful replica (must exceed the agent lease)")
		asEvery    = flag.Duration("autoscale-interval", 5*time.Second, "autoscaler evaluation interval")
		asWindow   = flag.Duration("scale-down-window", 30*time.Second, "autoscaler scale-down stabilisation window")
		routerAddr = flag.String("router-listen", ":8080", "L7 router listen address (empty to disable)")
	)
	flag.Parse()
	log := logging.New("controlplane", *logLevel)

	st := store.New()
	snap := filepath.Join(*dataDir, "state.json")
	if err := st.LoadSnapshot(snap); err != nil {
		log.Error("load snapshot", "err", err)
		os.Exit(1)
	}
	log.Info("state loaded", "revision", st.Revision(), "snapshot", snap)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	sched := &scheduler.Loop{Store: st, Scheduler: scheduler.New(), Log: log.With("loop", "scheduler")}
	go sched.Run(ctx)
	ctrl := &controller.Controller{Store: st, Log: log.With("loop", "reconciler"), StatefulFenceDelay: *fence}
	go ctrl.Run(ctx)
	mon := &nodemon.Monitor{Store: st, Log: log.With("loop", "nodemon"), Grace: *hbGrace}
	go mon.Run(ctx)

	var rt *router.Router
	if *routerAddr != "" {
		rt = router.New(log.With("loop", "router"))
		go rt.Sync(ctx, func(context.Context) ([]api.Instance, error) {
			return store.List[api.Instance](st, api.KindInstance)
		}, 500*time.Millisecond)
		rsrv := &http.Server{Addr: *routerAddr, Handler: rt, ReadHeaderTimeout: 5 * time.Second}
		go func() {
			log.Info("router listening", "addr", *routerAddr)
			if err := rsrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("router server", "err", err)
				cancel()
			}
		}()
		defer rsrv.Close()
	}
	if rt != nil {
		as := &autoscaler.Autoscaler{Store: st, Log: log.With("loop", "autoscaler"), Signal: rt.RPS,
			Interval: *asEvery, Tolerance: 0.1, ScaleDownWindow: *asWindow}
		go as.Run(ctx)
	}
	registerStateMetrics(st)

	// Periodic snapshots: a crash loses at most a few seconds of writes, and
	// every controller is level-triggered so it simply re-converges.
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		last := int64(-1)
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if rev := st.Revision(); rev != last {
					if err := st.SaveSnapshot(snap); err != nil {
						log.Error("snapshot", "err", err)
					}
					last = rev
				}
			}
		}
	}()

	var handler http.Handler = apiserver.New(st, log.With("loop", "api"))
	handler = logging.Middleware(log.With("loop", "http"), handler)
	srv := &http.Server{Addr: *listen, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = srv.Shutdown(shutdown)
	}()
	log.Info("api listening", "addr", *listen)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("api server", "err", err)
		os.Exit(1)
	}
	if err := st.SaveSnapshot(snap); err != nil {
		log.Error("final snapshot", "err", err)
	}
	log.Info("stopped")
}

// registerStateMetrics exports gauges derived from store contents, refreshed
// on every scrape: instances by workload and phase, nodes by status, and
// per-node requested vs allocatable resources.
func registerStateMetrics(st *store.Store) {
	byPhase := metrics.Default.Gauge("instacloud_instances", "Instances by workload and phase.", "workload", "phase")
	nodes := metrics.Default.Gauge("instacloud_nodes", "Nodes by status.", "status")
	alloc := metrics.Default.Gauge("instacloud_node_allocatable", "Allocatable resources per node.", "node", "resource")
	req := metrics.Default.Gauge("instacloud_node_requested", "Requested resources per node.", "node", "resource")
	metrics.Default.OnCollect(func() {
		insts, _ := store.List[api.Instance](st, api.KindInstance)
		ns, _ := store.List[api.Node](st, api.KindNode)
		byPhase.Reset()
		nodes.Reset()
		alloc.Reset()
		req.Reset()
		counts := map[[2]string]int{}
		used := map[string]api.Resources{}
		for _, in := range insts {
			counts[[2]string{in.Workload, string(in.Status.Phase)}]++
			if in.Holds() {
				used[in.Node] = used[in.Node].Add(in.Request())
			}
		}
		for k, n := range counts {
			byPhase.Set(float64(n), k[0], k[1])
		}
		status := map[string]int{}
		for _, n := range ns {
			status[string(n.Status)]++
			alloc.Set(float64(n.Allocatable.MilliCPU), n.Name, "cpu_millicores")
			alloc.Set(float64(n.Allocatable.MemoryMB), n.Name, "memory_mb")
			req.Set(float64(used[n.Name].MilliCPU), n.Name, "cpu_millicores")
			req.Set(float64(used[n.Name].MemoryMB), n.Name, "memory_mb")
		}
		for s, n := range status {
			nodes.Set(float64(n), s)
		}
	})
}
