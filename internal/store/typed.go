package store

import (
	"encoding/json"

	"github.com/anusha2211/instacloud-runtime/internal/api"
)

// List decodes every object of a kind. T is the value type, e.g. api.Node.
func List[T any](s *Store, kind string) ([]T, error) {
	raws, _ := s.ListRaw(kind)
	out := make([]T, 0, len(raws))
	for _, r := range raws {
		var v T
		if err := json.Unmarshal(r, &v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

// Mutate reads an object, applies fn and writes it back, retrying on
// conflict. fn returning false skips the write.
func Mutate[T any, P interface {
	*T
	api.Object
}](s *Store, kind, name string, fn func(P) bool) error {
	for attempt := 0; ; attempt++ {
		var v T
		p := P(&v)
		if err := s.Get(kind, name, p); err != nil {
			return err
		}
		if !fn(p) {
			return nil
		}
		err := s.Update(kind, p)
		if err == nil || !IsConflict(err) || attempt >= 10 {
			return err
		}
	}
}
