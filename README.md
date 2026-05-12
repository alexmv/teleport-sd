# teleport-sd

A Prometheus HTTP service discovery endpoint backed by Teleport application
registrations. Listens on `:9091`, exposes `GET /targets`, and treats every
query parameter as a required label match against each Teleport app's static
labels.

## Build

```
cd teleport-sd
go build -o teleport-sd
```

The pinned `github.com/gravitational/teleport/api` major in `go.mod`
must match the Teleport cluster's major (Teleport allows the client to
be at most one major version behind the cluster, never ahead). See the
next section for how to bump it when the cluster is upgraded.

## Upgrading the Teleport API dependency

When the cluster is upgraded to a new major version (e.g. v17 → v18),
we must update the API using a commit hash.  You can use a branch tip
(e.g., `refs/heads/branch/v17`, `refs/heads/branch/v18`) or a tag
(`refs/tags/v17.7.23`).

```shell
SHA=$(git ls-remote https://github.com/gravitational/teleport refs/heads/branch/v18 | cut -f1)
go get github.com/gravitational/teleport/api@$SHA
go mod tidy
go build -o teleport-sd
go vet ./...
```

The `// vNN.X.Y` trailing comment that `go.mod` carries on the
`teleport/api` require line is **not** maintained by `go get` or `go
mod tidy` — it's purely a manual annotation. Edit it by hand after
upgrading if you want it to reflect the new version.

If the API has changed between major versions, `go build` will fail at
the specific call site — fix the call, then commit `main.go`,
`go.mod`, and `go.sum` together.

Also check the Teleport changelog (https://goteleport.com/changelog/)
for any RBAC-relevant changes; the role spec below may need adjusting.


## tbot setup

The Teleport Go API client needs a concatenated PEM "identity file." tbot's
default `directory` storage destination writes credentials as separate files
(`key`, `tlscert`, `tlscacerts`, ...) which `LoadIdentityFile` cannot consume
directly. Add an explicit `identity` output to `/etc/tbot.yaml`:

```yaml
version: v2
proxy_server: teleport.example.com:443
onboarding:
  # ...
storage:
  type: directory
  path: /var/lib/teleport/bot
services:
  # Prometheus proxies all exporter traffic through this proxy
  - type: application-proxy
    listen: tcp://127.0.0.1:8080
outputs:
  - type: identity
    destination:
      type: directory
      path: /var/lib/teleport/bot-sd
```

The bot's role needs `app_labels` and `app_server` read/list:

```yaml
kind: role
version: v7
metadata:
  name: prometheus-sd
spec:
  allow:
    app_labels:
      # You may choose to limit this
      "*": "*"
    rules:
      - resources: [app_server]
        verbs: [list, read]
```

## Run

```
./teleport-sd \
    --proxy teleport.example.com:443 \
    --identity /var/lib/teleport/bot-sd/identity \
    --listen :9091 \
    --min-refresh 30s \
    --identity-reload 5m \
    --teleport-timeout 10s \
    --shutdown-timeout 5s
```

(All flag values above are the defaults; `--proxy` and `--identity` are the
only ones you usually need to set explicitly. Run `./teleport-sd --help` for
the full list.)

The service uses `DynamicIdentityFileCreds`, which re-reads the identity
file on `--identity-reload` intervals so tbot's hourly renewals are picked
up without a restart. Keep `--identity-reload` well below the cert TTL.

On SIGINT/SIGTERM the service stops accepting new connections and waits up
to `--shutdown-timeout` for in-flight `/targets` requests to drain before
closing the Teleport client.

## Prometheus config

One job per exporter type, all pointing at the same SD endpoint with a
different query filter. No relabel_configs needed — the SD endpoint emits
target groups whose labels are already the Teleport app's static labels, plus
an `instance` label defaulting to the app name (which is what the local
Teleport proxy routes by). A Teleport static label named `instance` wins over
the default if one is present.

**Important:** `proxy_url` is required. The target string emitted by
the SD endpoint is the Teleport *app name*, not a host:port —
Prometheus relies on the local Teleport app proxy at `127.0.0.1:8080`
to route by app name as the request's `Host` header. Drop `proxy_url`
and scrapes will fail trying to DNS-resolve the app name.

Any Teleport static label whose name isn't a valid Prometheus label name
(`[a-zA-Z_][a-zA-Z0-9_]*` — notably hyphens are rejected) is dropped from the
emitted target group, with a one-shot log warning per unique offending name.
Prometheus would otherwise reject the whole group.

```yaml
scrape_configs:
  - job_name: node
    proxy_url: "http://127.0.0.1:8080"
    scheme: http
    http_sd_configs:
      - url: "http://127.0.0.1:9091/targets?type=prometheus&exporter=node-exporter"
        refresh_interval: 1m

  - job_name: postgres
    proxy_url: "http://127.0.0.1:8080"
    scheme: http
    http_sd_configs:
      - url: "http://127.0.0.1:9091/targets?type=prometheus&exporter=postgres-exporter"
        refresh_interval: 1m
  # ... etc
```

## Caching semantics

- `--min-refresh` (default 30s) is the minimum age before a request triggers
  a fresh Teleport fetch.
- If the Teleport fetch fails but a previous snapshot exists, the previous
  snapshot is served and the error is logged. Prometheus won't see target
  set flapping during transient Teleport unavailability.
- If the very first fetch (at startup) fails, the process exits — we'd
  rather fail loudly than serve empty target lists.
- Concurrent requests that arrive while a refresh is in flight wait on the
  same refresh (request coalescing) rather than stampeding Teleport.

## Endpoints

- `GET /targets?key1=val1&key2=val2&...` — returns matching apps as Prometheus
  http_sd JSON. Distinct query keys are AND'd; repeating a key OR's its values
  (`?exporter=node-exporter&exporter=cadvisor` matches either).
- `GET /healthz` — returns 200 OK if the process is running. (It does not
  attempt to contact Teleport; if you want a deep healthcheck, scrape
  `/targets?` and check the response.)
