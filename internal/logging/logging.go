// Package logging sets up structured JSON logs shared by all binaries.
// Components log with consistent keys (workload, instance, node, phase) so a
// replica's history can be followed across processes with one query.
package logging

import (
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"
)

// New returns a JSON logger tagged with the component name.
func New(component, level string) *slog.Logger {
	var lv slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lv = slog.LevelDebug
	case "warn":
		lv = slog.LevelWarn
	case "error":
		lv = slog.LevelError
	default:
		lv = slog.LevelInfo
	}
	h := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: lv})
	return slog.New(h).With("component", component)
}

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(c int) { w.code = c; w.ResponseWriter.WriteHeader(c) }

func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Middleware logs every request at debug level, and errors (>=500) at warn.
func Middleware(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, code: http.StatusOK}
		next.ServeHTTP(sw, r)
		lv := slog.LevelDebug
		if sw.code >= 500 {
			lv = slog.LevelWarn
		}
		log.Log(r.Context(), lv, "http request", "method", r.Method, "path", r.URL.Path,
			"code", sw.code, "duration_ms", time.Since(start).Milliseconds())
	})
}
