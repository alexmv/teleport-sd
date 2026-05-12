// teleport-sd is a Prometheus HTTP Service Discovery endpoint backed by
// Teleport application registrations.
//
// It listens on :9091 and exposes a single endpoint, /targets, which returns
// JSON in the Prometheus http_sd format. Each distinct query-string key is a
// required label match against the Teleport application's static labels.
// Repeating a key OR's its values; different keys AND together.
//
// Examples:
//
//	GET /targets?type=prometheus&exporter=node-exporter
//
// returns every Teleport app whose static labels include both
//
//	type=prometheus  AND  exporter=node-exporter
//
//	GET /targets?type=prometheus&exporter=node-exporter&exporter=cadvisor
//
// returns every Teleport app whose static labels include type=prometheus AND
// (exporter=node-exporter OR exporter=cadvisor).
//
// The Teleport application list is cached in memory. On every request we
// refresh the cache if our last attempt was longer ago than minRefreshInterval;
// if the refresh fails we serve the previous cached snapshot (logging the
// error) so a transient Teleport outage does not blank out Prometheus targets.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gravitational/teleport/api/client"
	"github.com/gravitational/teleport/api/defaults"
	"github.com/gravitational/teleport/api/types"
)

// httpSDTarget is the JSON shape Prometheus expects from an http_sd endpoint:
// a list of {targets, labels} groups.
type httpSDTarget struct {
	Targets []string          `json:"targets"`
	Labels  map[string]string `json:"labels"`
}

// cachedApps holds a snapshot of Teleport applications plus when it was taken.
// We hand out the slice unchanged; callers must not mutate it.
type cachedApps struct {
	apps      []types.Application
	fetchedAt time.Time
}

type server struct {
	clt                *client.Client
	minRefreshInterval time.Duration
	teleportTimeout    time.Duration

	mu    sync.Mutex // guards the fields below
	cache *cachedApps
	// lastRefreshAttempt is the wall-clock time of the most recent fetch
	// attempt regardless of outcome. We rate-limit refreshes against this so a
	// failing Teleport isn't hit on every request.
	lastRefreshAttempt time.Time
	// inflight is non-nil while a refresh is in progress; concurrent callers
	// wait on it instead of stampeding Teleport.
	inflight chan struct{}

	// warnedLabels tracks Teleport label names we've already logged a warning
	// for, so we don't spam the log on every scrape.
	warnedLabels sync.Map
}

// validPromLabelName reports whether s matches Prometheus' label-name grammar
// /[a-zA-Z_][a-zA-Z0-9_]*/. Teleport allows characters (notably hyphens) that
// Prometheus rejects; passing such a label through would make Prometheus drop
// the whole target group.
func validPromLabelName(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '_', c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z':
		case i > 0 && c >= '0' && c <= '9':
		default:
			return false
		}
	}
	return true
}

func (s *server) warnInvalidLabelName(name string) {
	if _, loaded := s.warnedLabels.LoadOrStore(name, struct{}{}); !loaded {
		log.Printf("skipping Teleport label %q: not a valid Prometheus label name", name)
	}
}

// getApps returns the current cached app list, refreshing it from Teleport if
// our last attempt is older than minRefreshInterval. If a refresh fails but we
// have a previous snapshot, we return that snapshot and a nil error (logging
// the failure). Only if we have *no* snapshot at all does an error surface.
func (s *server) getApps(ctx context.Context) ([]types.Application, error) {
	s.mu.Lock()
	if s.cache != nil && time.Since(s.lastRefreshAttempt) < s.minRefreshInterval {
		apps := s.cache.apps
		s.mu.Unlock()
		return apps, nil
	}
	if s.inflight != nil {
		ch := s.inflight
		s.mu.Unlock()
		<-ch
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.cache == nil {
			return nil, errors.New("teleport refresh failed and no cached data available")
		}
		return s.cache.apps, nil
	}
	s.inflight = make(chan struct{})
	prev := s.cache
	s.mu.Unlock()

	return s.doRefresh(ctx, prev)
}

// doRefresh runs the actual Teleport fetch as the leader. The deferred
// cleanup always closes s.inflight so coalesced waiters wake up, even if
// fetchApps panics — net/http would recover such a panic but leave our
// coalescing state stuck otherwise.
func (s *server) doRefresh(ctx context.Context, prev *cachedApps) ([]types.Application, error) {
	fetchCtx, cancel := context.WithTimeout(ctx, s.teleportTimeout)
	defer cancel()

	defer func() {
		s.mu.Lock()
		s.lastRefreshAttempt = time.Now()
		close(s.inflight)
		s.inflight = nil
		s.mu.Unlock()
	}()

	apps, err := s.fetchApps(fetchCtx)
	if err != nil {
		if prev != nil {
			log.Printf("teleport refresh failed, serving stale cache (age %s): %v",
				time.Since(prev.fetchedAt).Round(time.Second), err)
			return prev.apps, nil
		}
		return nil, err
	}
	s.mu.Lock()
	s.cache = &cachedApps{apps: apps, fetchedAt: time.Now()}
	s.mu.Unlock()
	return apps, nil
}

// fetchApps pulls every AppServer from Teleport and returns the unique set of
// Applications they proxy. We use GetApplicationServers rather than GetApps so
// we only return apps that are currently being proxied (i.e. reachable).
// Multiple AppServers can proxy the same Application name (HA); we dedupe by
// app name.
func (s *server) fetchApps(ctx context.Context) ([]types.Application, error) {
	servers, err := s.clt.GetApplicationServers(ctx, defaults.Namespace)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(servers))
	apps := make([]types.Application, 0, len(servers))
	for _, srv := range servers {
		app := srv.GetApp()
		if app == nil {
			continue
		}
		name := app.GetName()
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		apps = append(apps, app)
	}
	return apps, nil
}

// handleTargets implements GET /targets. Each distinct query-string key is a
// required label match (AND across keys). When a key is repeated
// (e.g. ?foo=a&foo=b) the values are OR'd — the app matches if its label for
// that key equals any of the supplied values.
func (s *server) handleTargets(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	required := r.URL.Query()
	apps, err := s.getApps(r.Context())
	if err != nil {
		log.Printf("getApps failed with no cached fallback: %v", err)
		http.Error(w, "service discovery unavailable", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if r.Method == http.MethodHead {
		return
	}

	out := make([]httpSDTarget, 0, len(apps))
	for _, app := range apps {
		labels := app.GetStaticLabels()
		if !labelsMatch(labels, required) {
			continue
		}
		// Pass through every Teleport static label as a Prometheus label.
		// Prometheus will use this group's labels verbatim (subject to its
		// own relabel pipeline, which in our case is empty). Skip any
		// Teleport label whose name isn't a valid Prometheus label name —
		// Prometheus would otherwise reject the whole target group.
		promLabels := make(map[string]string, len(labels)+1)
		for k, v := range labels {
			if !validPromLabelName(k) {
				s.warnInvalidLabelName(k)
				continue
			}
			promLabels[k] = v
		}
		// Convention: instance is the app name (which is what the local
		// Teleport proxy routes by). Don't overwrite if a label already
		// supplies one.
		if _, ok := promLabels["instance"]; !ok {
			promLabels["instance"] = app.GetName()
		}
		out = append(out, httpSDTarget{
			Targets: []string{app.GetName()},
			Labels:  promLabels,
		})
	}

	if err := json.NewEncoder(w).Encode(out); err != nil {
		// Headers are already sent; just log.
		log.Printf("encoding response failed: %v", err)
	}
}

// labelsMatch returns true iff, for every key in required, the app's label for
// that key equals at least one of the values supplied for it. Different keys
// are AND'd; multiple values for the same key are OR'd. Empty required
// matches everything.
func labelsMatch(labels map[string]string, required map[string][]string) bool {
	for k, vs := range required {
		got, ok := labels[k]
		if !ok {
			return false
		}
		matched := false
		for _, v := range vs {
			if got == v {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func main() {
	var (
		listenAddr      = flag.String("listen", ":9091", "address to listen on")
		identityFile    = flag.String("identity", "/var/lib/teleport/bot-sd/identity", "path to tbot-written identity file")
		proxyAddr       = flag.String("proxy", "", "Teleport proxy address (e.g. teleport.example.com:443) — required")
		minRefresh      = flag.Duration("min-refresh", 30*time.Second, "minimum age before re-fetching from Teleport")
		teleportTimeout = flag.Duration("teleport-timeout", 10*time.Second, "timeout for a single Teleport API call")
		identityReload  = flag.Duration("identity-reload", 5*time.Minute, "how often to re-read the identity file from disk (tbot rotates it periodically)")
		shutdownTimeout = flag.Duration("shutdown-timeout", 5*time.Second, "max time to wait for in-flight requests to drain on shutdown")
	)
	flag.Parse()

	if *proxyAddr == "" {
		log.Fatal("--proxy is required (e.g. --proxy teleport.example.com:443)")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Use the dynamic credential loader so tbot's periodic identity renewals
	// (every ~hour by default) are picked up without restarting this process.
	// A bare LoadIdentityFile reads the file once at startup, which would
	// cause the client to fail with expired-credentials errors after the
	// first tbot rotation.
	creds, err := client.NewDynamicIdentityFileCreds(*identityFile)
	if err != nil {
		log.Fatalf("failed to load identity file %s: %v", *identityFile, err)
	}

	// Reload the identity file from disk on a fixed interval. tbot writes a
	// new identity atomically, so re-reading is safe at any time; we just
	// poll because there isn't an inotify hook in the public API. The
	// interval should be short enough that we always pick up a renewed
	// identity well before the *previous* one expires.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(*identityReload)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := creds.Reload(); err != nil {
					log.Printf("identity reload failed: %v", err)
				}
			}
		}
	}()

	// Construct the Teleport client with a background context so its lifetime
	// is governed by clt.Close() (deferred until after the HTTP server has
	// drained). If we passed the signal-cancellable ctx, in-flight /targets
	// handlers could see the client torn down mid-drain on SIGTERM.
	clt, err := client.New(context.Background(), client.Config{
		Addrs:       []string{*proxyAddr},
		Credentials: []client.Credentials{creds},
	})
	if err != nil {
		log.Fatalf("failed to create Teleport client: %v", err)
	}
	defer clt.Close()

	// Sanity check the connection up front so misconfiguration fails loudly
	// at startup rather than silently on the first scrape.
	pingCtx, cancelPing := context.WithTimeout(ctx, *teleportTimeout)
	if _, err := clt.Ping(pingCtx); err != nil {
		cancelPing()
		log.Fatalf("failed to ping Teleport at %s: %v", *proxyAddr, err)
	}
	cancelPing()

	s := &server{
		clt:                clt,
		minRefreshInterval: *minRefresh,
		teleportTimeout:    *teleportTimeout,
	}

	// Warm the cache so the first scrape is fast and so we fail loudly at
	// startup if our identity can't list apps.
	warmCtx, cancelWarm := context.WithTimeout(ctx, *teleportTimeout)
	if _, err := s.getApps(warmCtx); err != nil {
		cancelWarm()
		log.Fatalf("initial app fetch failed: %v", err)
	}
	cancelWarm()

	mux := http.NewServeMux()
	mux.HandleFunc("/targets", s.handleTargets)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{
		Addr:              *listenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		log.Printf("listening on %s, talking to Teleport at %s", *listenAddr, *proxyAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	select {
	case err := <-serverErr:
		log.Fatalf("server failed: %v", err)
	case <-ctx.Done():
		log.Print("shutdown signal received, draining")
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), *shutdownTimeout)
		defer cancelShutdown()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("graceful shutdown failed: %v", err)
		}
	}
	wg.Wait()
}
