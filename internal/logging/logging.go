// Package logging sets up structured JSON logs shared by all binaries.
// Components log with consistent keys (workload, instance, node, phase) so a
// replica's history can be followed across processes with one query.
package logging

import (
	"log/slog"
	"os"
	"strings"
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
