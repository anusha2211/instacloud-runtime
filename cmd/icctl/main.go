// Command icctl is the InstaCloud CLI.
//
//	icctl apply -f workload.json
//	icctl get workloads|instances|nodes|volumes
//	icctl describe <workload>
//	icctl scale <workload> <replicas>
//	icctl delete <workload>
//	icctl wait <workload> [-ready N] [-timeout 60s]
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/anusha2211/instacloud-runtime/internal/api"
	"github.com/anusha2211/instacloud-runtime/internal/client"
)

func usage() {
	fmt.Fprintln(os.Stderr, `usage: icctl [--api URL] <command>

commands:
  apply -f FILE            create or update a workload from a JSON spec
  get workloads|instances|nodes|volumes
  describe WORKLOAD        workload status and its instances
  scale WORKLOAD N         set replica count
  delete WORKLOAD          delete a workload (instances are terminated)
  wait WORKLOAD [-ready N] [-timeout D]
                           block until N replicas (default: spec) are ready and up to date`)
	os.Exit(2)
}

func main() {
	apiURL := flag.String("api", envOr("INSTACLOUD_API", "http://127.0.0.1:7070"), "control plane URL")
	flag.Usage = usage
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		usage()
	}
	c := client.New(*apiURL)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var err error
	switch args[0] {
	case "apply":
		err = apply(ctx, c, args[1:])
	case "get":
		if len(args) < 2 {
			usage()
		}
		err = get(ctx, c, args[1])
	case "describe":
		if len(args) < 2 {
			usage()
		}
		err = describe(ctx, c, args[1])
	case "scale":
		if len(args) < 3 {
			usage()
		}
		var n int
		if _, err = fmt.Sscan(args[2], &n); err == nil {
			err = c.Scale(ctx, args[1], n)
		}
		if err == nil {
			fmt.Printf("workload/%s scaled to %d\n", args[1], n)
		}
	case "wait":
		if len(args) < 2 {
			usage()
		}
		cancel()
		err = wait(c, args[1], args[2:])
	case "delete":
		if len(args) < 2 {
			usage()
		}
		if err = c.DeleteWorkload(ctx, args[1]); err == nil {
			fmt.Printf("workload/%s deleted\n", args[1])
		}
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func apply(ctx context.Context, c *client.Client, args []string) error {
	fs := flag.NewFlagSet("apply", flag.ExitOnError)
	file := fs.String("f", "", "workload spec (JSON)")
	_ = fs.Parse(args)
	if *file == "" {
		return fmt.Errorf("apply: -f is required")
	}
	b, err := os.ReadFile(*file)
	if err != nil {
		return err
	}
	var w api.Workload
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&w); err != nil {
		return fmt.Errorf("%s: %w", *file, err)
	}
	out, err := c.Apply(ctx, &w)
	if err != nil {
		return err
	}
	fmt.Printf("workload/%s applied (generation %d, replicas %d)\n", out.Name, out.Generation, out.Spec.Replicas)
	return nil
}

func table(header string, rows [][]string) {
	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, header)
	for _, r := range rows {
		fmt.Fprintln(tw, strings.Join(r, "\t"))
	}
	tw.Flush()
}

func age(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	return fmt.Sprintf("%dh", int(d.Hours()))
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func get(ctx context.Context, c *client.Client, what string) error {
	switch strings.TrimSuffix(what, "s") {
	case "workload":
		ws, err := c.Workloads(ctx)
		if err != nil {
			return err
		}
		var rows [][]string
		for _, w := range ws {
			as := "-"
			if a := w.Spec.Autoscale; a != nil {
				as = fmt.Sprintf("%d-%d @%.0frps", a.MinReplicas, a.MaxReplicas, a.TargetRPSPerReplica)
			}
			rows = append(rows, []string{w.Name, string(w.Spec.Kind),
				fmt.Sprintf("%d/%d", w.Status.ReadyReplicas, w.Spec.Replicas),
				fmt.Sprint(w.Status.UpdatedReplicas), fmt.Sprint(w.Generation), as, age(w.CreatedAt)})
		}
		table("NAME\tKIND\tREADY\tUP-TO-DATE\tGEN\tAUTOSCALE\tAGE", rows)
	case "instance":
		is, err := c.Instances(ctx)
		if err != nil {
			return err
		}
		sort.Slice(is, func(i, j int) bool {
			if is[i].Workload != is[j].Workload {
				return is[i].Workload < is[j].Workload
			}
			return is[i].Name < is[j].Name
		})
		var rows [][]string
		for _, in := range is {
			rows = append(rows, []string{in.Name, in.Workload, string(in.Status.Phase), fmt.Sprint(in.Status.Ready),
				fmt.Sprint(in.Status.Restarts), orDash(in.Node), orDash(in.Status.Endpoint), fmt.Sprint(in.Generation),
				orDash(in.Status.Message)})
		}
		table("NAME\tWORKLOAD\tPHASE\tREADY\tRESTARTS\tNODE\tENDPOINT\tGEN\tMESSAGE", rows)
	case "node":
		ns, err := c.Nodes(ctx)
		if err != nil {
			return err
		}
		is, err := c.Instances(ctx)
		if err != nil {
			return err
		}
		used := map[string]api.Resources{}
		count := map[string]int{}
		for _, in := range is {
			if in.Holds() {
				used[in.Node] = used[in.Node].Add(in.Request())
				count[in.Node]++
			}
		}
		var rows [][]string
		for _, n := range ns {
			u := used[n.Name]
			rows = append(rows, []string{n.Name, string(n.Status), n.Zone,
				fmt.Sprintf("%d/%dm", u.MilliCPU, n.Allocatable.MilliCPU),
				fmt.Sprintf("%d/%dMi", u.MemoryMB, n.Allocatable.MemoryMB),
				fmt.Sprint(count[n.Name]), age(n.LastHeartbeat)})
		}
		table("NAME\tSTATUS\tZONE\tCPU(req/alloc)\tMEM(req/alloc)\tINSTANCES\tLAST-HEARTBEAT", rows)
	case "volume":
		vs, err := c.Volumes(ctx)
		if err != nil {
			return err
		}
		var rows [][]string
		for _, v := range vs {
			rows = append(rows, []string{v.Name, v.Workload, fmt.Sprint(v.Ordinal), fmt.Sprintf("%dMi", v.SizeMB),
				string(v.Phase), orDash(v.BoundNode), orDash(v.Instance)})
		}
		table("NAME\tWORKLOAD\tORDINAL\tSIZE\tPHASE\tNODE\tINSTANCE", rows)
	default:
		return fmt.Errorf("unknown resource %q", what)
	}
	return nil
}

func describe(ctx context.Context, c *client.Client, name string) error {
	w, err := c.GetWorkload(ctx, name)
	if err != nil {
		return err
	}
	b, _ := json.MarshalIndent(w, "", "  ")
	fmt.Println(string(b))
	is, err := c.Instances(ctx)
	if err != nil {
		return err
	}
	fmt.Println("\ninstances:")
	for _, in := range is {
		if in.Workload == name {
			fmt.Printf("  %-14s %-11s ready=%-5v restarts=%d node=%s %s\n", in.Name, in.Status.Phase,
				in.Status.Ready, in.Status.Restarts, orDash(in.Node), in.Status.Message)
		}
	}
	return nil
}

func wait(c *client.Client, name string, args []string) error {
	fs := flag.NewFlagSet("wait", flag.ExitOnError)
	ready := fs.Int("ready", -1, "ready replicas to wait for (default: spec.replicas)")
	timeout := fs.Duration("timeout", 60*time.Second, "give up after this long")
	_ = fs.Parse(args)
	deadline := time.Now().Add(*timeout)
	var last string
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		w, err := c.GetWorkload(ctx, name)
		cancel()
		if err == nil {
			want := *ready
			if want < 0 {
				want = w.Spec.Replicas
			}
			last = fmt.Sprintf("ready %d/%d, up-to-date %d", w.Status.ReadyReplicas, want, w.Status.UpdatedReplicas)
			if w.Status.ObservedGeneration == w.Generation && w.Status.ReadyReplicas >= want &&
				w.Status.UpdatedReplicas >= want && w.Status.Replicas == w.Spec.Replicas {
				fmt.Printf("workload/%s ready (%s)\n", name, last)
				return nil
			}
		} else {
			last = err.Error()
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for %s: %s", name, last)
		}
		time.Sleep(500 * time.Millisecond)
	}
}
