// Package client is a small HTTP client for the control-plane API, used by
// the node agent, the router and icctl.
package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/anusha2211/instacloud-runtime/internal/api"
	"github.com/anusha2211/instacloud-runtime/internal/apiserver"
	"github.com/anusha2211/instacloud-runtime/internal/store"
)

type Client struct {
	Base string
	HTTP *http.Client
}

func New(base string) *Client {
	return &Client{Base: strings.TrimRight(base, "/"), HTTP: &http.Client{Timeout: 10 * time.Second}}
}

// APIError is a non-2xx response.
type APIError struct {
	Code int
	Msg  string
}

func (e *APIError) Error() string { return fmt.Sprintf("api: %d: %s", e.Code, e.Msg) }

// IsConflict reports a 409 from the API.
func IsConflict(err error) bool {
	ae, ok := err.(*APIError)
	return ok && ae.Code == http.StatusConflict
}

func IsNotFound(err error) bool {
	ae, ok := err.(*APIError)
	return ok && ae.Code == http.StatusNotFound
}

func (c *Client) do(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.Base+path, body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		var e struct {
			Error string `json:"error"`
		}
		b, _ := io.ReadAll(resp.Body)
		if json.Unmarshal(b, &e) != nil || e.Error == "" {
			e.Error = strings.TrimSpace(string(b))
		}
		return &APIError{Code: resp.StatusCode, Msg: e.Error}
	}
	if out != nil && resp.StatusCode != http.StatusNoContent {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func list[T any](ctx context.Context, c *Client, kind string) ([]T, error) {
	var resp struct {
		Items []T `json:"items"`
	}
	err := c.do(ctx, http.MethodGet, "/v1/"+kind, nil, &resp)
	return resp.Items, err
}

func (c *Client) Workloads(ctx context.Context) ([]api.Workload, error) {
	return list[api.Workload](ctx, c, api.KindWorkload)
}
func (c *Client) Instances(ctx context.Context) ([]api.Instance, error) {
	return list[api.Instance](ctx, c, api.KindInstance)
}
func (c *Client) Nodes(ctx context.Context) ([]api.Node, error) {
	return list[api.Node](ctx, c, api.KindNode)
}
func (c *Client) Volumes(ctx context.Context) ([]api.Volume, error) {
	return list[api.Volume](ctx, c, api.KindVolume)
}

func (c *Client) GetWorkload(ctx context.Context, name string) (*api.Workload, error) {
	var w api.Workload
	return &w, c.do(ctx, http.MethodGet, "/v1/workloads/"+name, nil, &w)
}

func (c *Client) Apply(ctx context.Context, w *api.Workload) (*api.Workload, error) {
	var out api.Workload
	return &out, c.do(ctx, http.MethodPut, "/v1/workloads/"+w.Name, w, &out)
}

func (c *Client) Scale(ctx context.Context, name string, replicas int) error {
	return c.do(ctx, http.MethodPost, "/v1/workloads/"+name+"/scale", map[string]int{"replicas": replicas}, nil)
}

func (c *Client) DeleteWorkload(ctx context.Context, name string) error {
	return c.do(ctx, http.MethodDelete, "/v1/workloads/"+name, nil, nil)
}

func (c *Client) Register(ctx context.Context, node string, req apiserver.RegisterRequest) error {
	return c.do(ctx, http.MethodPost, "/v1/nodes/"+node+"/register", req, nil)
}

func (c *Client) Heartbeat(ctx context.Context, node string) error {
	return c.do(ctx, http.MethodPost, "/v1/nodes/"+node+"/heartbeat", nil, nil)
}

func (c *Client) Assignments(ctx context.Context, node string) ([]apiserver.Assignment, error) {
	var out []apiserver.Assignment
	return out, c.do(ctx, http.MethodGet, "/v1/nodes/"+node+"/assignments", nil, &out)
}

func (c *Client) ReportStatus(ctx context.Context, instance, node string, st api.InstanceStatus) error {
	return c.do(ctx, http.MethodPut, "/v1/instances/"+instance+"/status",
		apiserver.StatusUpdate{Node: node, Status: st}, nil)
}

// Watch streams store events of a kind until ctx is done or the stream ends.
func (c *Client) Watch(ctx context.Context, kind string, fn func(store.Event)) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.Base+"/v1/watch?kind="+kind, nil)
	if err != nil {
		return err
	}
	hc := &http.Client{} // no timeout: long-lived stream
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 4<<20)
	for sc.Scan() {
		var e store.Event
		if err := json.Unmarshal(sc.Bytes(), &e); err == nil {
			fn(e)
		}
	}
	return sc.Err()
}
