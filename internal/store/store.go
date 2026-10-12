// Package store is the control plane's state: an in-memory versioned KV with
// compare-and-swap writes and watches. Every write bumps a global revision
// (stamped as ResourceVersion), and updates must carry the version they read,
// so the scheduler/reconciler/agents can share objects without locks.
// Swap this out for etcd when we need HA.
package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/anusha2211/instacloud-runtime/internal/api"
)

var (
	ErrNotFound = errors.New("not found")
	ErrExists   = errors.New("already exists")
	ErrConflict = errors.New("resource version conflict")
)

// EventType describes a change.
type EventType string

const (
	Added    EventType = "ADDED"
	Modified EventType = "MODIFIED"
	Deleted  EventType = "DELETED"
)

// Event is delivered to watchers. Object holds the JSON of the new value (or
// the last value for deletes).
type Event struct {
	Type     EventType       `json:"type"`
	Kind     string          `json:"kind"`
	Name     string          `json:"name"`
	Revision int64           `json:"revision"`
	Object   json.RawMessage `json:"object"`
}

type entry struct {
	data json.RawMessage
	rev  int64
}

type watcher struct {
	kind string
	ch   chan Event
}

// Store is safe for concurrent use.
type Store struct {
	mu       sync.RWMutex
	rev      int64
	data     map[string]map[string]entry // kind -> name -> entry
	watchers map[*watcher]struct{}
	now      func() time.Time
}

func New() *Store {
	return &Store{
		data:     map[string]map[string]entry{},
		watchers: map[*watcher]struct{}{},
		now:      time.Now,
	}
}

// Revision returns the current store revision.
func (s *Store) Revision() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.rev
}

// Create stores a new object. It fails with ErrExists if the name is taken.
func (s *Store) Create(kind string, obj api.Object) error {
	m := obj.GetMeta()
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.bucket(kind)[m.Name]; ok {
		return fmt.Errorf("%s/%s: %w", kind, m.Name, ErrExists)
	}
	if m.CreatedAt.IsZero() {
		m.CreatedAt = s.now().UTC()
	}
	if m.Generation == 0 {
		m.Generation = 1
	}
	return s.put(kind, obj, Added)
}

// Update replaces an object if its ResourceVersion matches the stored one.
// A ResourceVersion of 0 means "unconditional" and is only meant for tests and
// admin tooling.
func (s *Store) Update(kind string, obj api.Object) error {
	m := obj.GetMeta()
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.bucket(kind)[m.Name]
	if !ok {
		return fmt.Errorf("%s/%s: %w", kind, m.Name, ErrNotFound)
	}
	if m.ResourceVersion != 0 && m.ResourceVersion != cur.rev {
		return fmt.Errorf("%s/%s: have %d, stored %d: %w", kind, m.Name, m.ResourceVersion, cur.rev, ErrConflict)
	}
	return s.put(kind, obj, Modified)
}

// Delete removes an object. If rv != 0 it must match.
func (s *Store) Delete(kind, name string, rv int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.bucket(kind)
	cur, ok := b[name]
	if !ok {
		return fmt.Errorf("%s/%s: %w", kind, name, ErrNotFound)
	}
	if rv != 0 && rv != cur.rev {
		return fmt.Errorf("%s/%s: %w", kind, name, ErrConflict)
	}
	delete(b, name)
	s.rev++
	s.notify(Event{Type: Deleted, Kind: kind, Name: name, Revision: s.rev, Object: cur.data})
	return nil
}

// Get decodes an object into out.
func (s *Store) Get(kind, name string, out api.Object) error {
	s.mu.RLock()
	e, ok := s.bucket(kind)[name]
	s.mu.RUnlock()
	if !ok {
		return fmt.Errorf("%s/%s: %w", kind, name, ErrNotFound)
	}
	return json.Unmarshal(e.data, out)
}

// GetRaw returns the stored JSON.
func (s *Store) GetRaw(kind, name string) (json.RawMessage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.bucket(kind)[name]
	if !ok {
		return nil, fmt.Errorf("%s/%s: %w", kind, name, ErrNotFound)
	}
	return e.data, nil
}

// ListRaw returns all objects of a kind sorted by name, plus the revision the
// list is consistent at.
func (s *Store) ListRaw(kind string) ([]json.RawMessage, int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b := s.data[kind]
	names := make([]string, 0, len(b))
	for n := range b {
		names = append(names, n)
	}
	sort.Strings(names)
	out := make([]json.RawMessage, 0, len(names))
	for _, n := range names {
		out = append(out, b[n].data)
	}
	return out, s.rev
}

// Watch streams events for kind ("" = all kinds) until stop is called. The
// channel is buffered; a watcher that falls too far behind is dropped (its
// channel closed) and must re-list, exactly like an etcd compaction.
func (s *Store) Watch(kind string) (<-chan Event, func()) {
	w := &watcher{kind: kind, ch: make(chan Event, 1024)}
	s.mu.Lock()
	s.watchers[w] = struct{}{}
	s.mu.Unlock()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			s.mu.Lock()
			if _, ok := s.watchers[w]; ok {
				delete(s.watchers, w)
				close(w.ch)
			}
			s.mu.Unlock()
		})
	}
	return w.ch, stop
}

func (s *Store) bucket(kind string) map[string]entry {
	b, ok := s.data[kind]
	if !ok {
		b = map[string]entry{}
		s.data[kind] = b
	}
	return b
}

// put must be called with mu held.
func (s *Store) put(kind string, obj api.Object, t EventType) error {
	s.rev++
	m := obj.GetMeta()
	old := m.ResourceVersion
	m.ResourceVersion = s.rev
	data, err := json.Marshal(obj)
	if err != nil {
		s.rev--
		m.ResourceVersion = old
		return err
	}
	s.bucket(kind)[m.Name] = entry{data: data, rev: s.rev}
	s.notify(Event{Type: t, Kind: kind, Name: m.Name, Revision: s.rev, Object: data})
	return nil
}

func (s *Store) notify(e Event) {
	for w := range s.watchers {
		if w.kind != "" && w.kind != e.Kind {
			continue
		}
		select {
		case w.ch <- e:
		default: // slow consumer: drop it, it will re-list
			delete(s.watchers, w)
			close(w.ch)
		}
	}
}

type snapshot struct {
	Revision int64                                 `json:"revision"`
	Data     map[string]map[string]json.RawMessage `json:"data"`
}

// SaveSnapshot atomically writes the whole store to path.
func (s *Store) SaveSnapshot(path string) error {
	s.mu.RLock()
	snap := snapshot{Revision: s.rev, Data: map[string]map[string]json.RawMessage{}}
	for k, b := range s.data {
		snap.Data[k] = map[string]json.RawMessage{}
		for n, e := range b {
			snap.Data[k][n] = e.data
		}
	}
	s.mu.RUnlock()
	buf, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, buf, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// LoadSnapshot restores a snapshot written by SaveSnapshot. Missing files are
// not an error.
func (s *Store) LoadSnapshot(path string) error {
	buf, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var snap snapshot
	if err := json.Unmarshal(buf, &snap); err != nil {
		return fmt.Errorf("decode snapshot: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rev = snap.Revision
	s.data = map[string]map[string]entry{}
	for k, b := range snap.Data {
		for n, raw := range b {
			var m struct {
				Meta api.Meta `json:"metadata"`
			}
			_ = json.Unmarshal(raw, &m)
			s.bucket(k)[n] = entry{data: raw, rev: m.Meta.ResourceVersion}
		}
	}
	return nil
}

// IsNotFound / IsConflict are convenience helpers.
func IsNotFound(err error) bool { return errors.Is(err, ErrNotFound) }
func IsConflict(err error) bool { return errors.Is(err, ErrConflict) }
func IsExists(err error) bool   { return errors.Is(err, ErrExists) }

// Key formats kind/name for logs.
func Key(kind, name string) string { return strings.Join([]string{kind, name}, "/") }
