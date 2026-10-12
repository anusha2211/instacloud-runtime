// Command demoapp is a small workload used by the demo and examples. It
// serves:
//
//	GET  /           instance identity
//	GET  /healthz    health (fails after POST /sick)
//	POST /sick       start failing health checks
//	POST /crash      exit(1) immediately
//	PUT  /kv/<key>   store body under DATA_DIR (stateful workloads)
//	GET  /kv/<key>   read it back
//	GET  /work?ms=N  burn CPU for N ms (load for the autoscaler)
package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}
	id := os.Getenv("INSTANCE_ID")
	dataDir := os.Getenv("DATA_DIR")
	var sick atomic.Bool

	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "hello from %s (version %s)\n", id, os.Getenv("VERSION"))
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if sick.Load() {
			http.Error(w, "sick", http.StatusInternalServerError)
			return
		}
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("POST /sick", func(w http.ResponseWriter, r *http.Request) {
		sick.Store(true)
		fmt.Fprintf(w, "%s is now failing health checks\n", id)
	})
	mux.HandleFunc("POST /crash", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "%s crashing\n", id)
		go func() { time.Sleep(50 * time.Millisecond); os.Exit(1) }()
	})
	mux.HandleFunc("GET /work", func(w http.ResponseWriter, r *http.Request) {
		ms, _ := strconv.Atoi(r.URL.Query().Get("ms"))
		end := time.Now().Add(time.Duration(ms) * time.Millisecond)
		n := 0
		for time.Now().Before(end) {
			n++
		}
		fmt.Fprintf(w, "%s did %d iterations\n", id, n)
	})
	mux.HandleFunc("/kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		if dataDir == "" {
			http.Error(w, "no volume attached", http.StatusBadRequest)
			return
		}
		key := filepath.Base(r.PathValue("key"))
		path := filepath.Join(dataDir, key)
		switch r.Method {
		case http.MethodPut:
			b, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
			if err := os.WriteFile(path, b, 0o644); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			fmt.Fprintf(w, "%s stored %s\n", id, key)
		case http.MethodGet:
			b, err := os.ReadFile(path)
			if err != nil {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			fmt.Fprintf(w, "%s: %s=%s\n", id, key, strings.TrimSpace(string(b)))
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	log.Printf("%s listening on :%s data=%q", id, port, dataDir)
	log.Fatal(http.ListenAndServe("127.0.0.1:"+port, mux))
}
