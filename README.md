# bloom.

**Bloom turns a collection of ordinary, heterogeneous devices into a unified inference swarm.** Every participating device becomes an independently addressable inference endpoint whilst simultaneously contributing computational capacity to a shared, load-balanced execution environment.

Bloom operates without mandatory cloud infrastructure, internet connectivity, central accounts, or a dedicated orchestration server. Bloom is not itself a model runner; it is the infrastructure that makes multiple model runners collectively accessible, manageable and computationally useful.

## Status

Bloom runs as an autonomous inference node with optional LAN discovery and authenticated routing to configured peers.

- `bloomd`, a persistent Rust daemon that serves the Bloom network API and an OpenAI-compatible inference API over loopback HTTP or configured HTTPS
- Ollama harness adapter (attached or managed mode)
- Local resource telemetry (CPU, memory, storage, GPU/NPU detection, thermal, power)
- SQLite-backed execution ledger, event log and capacity snapshots
- Atomic local resource reservations, drain mode, job cancellation, SSE streaming
- Opt-in mDNS discovery and advertisement, with a separate registry of candidates
- HTTPS forwarding to explicitly configured peers, ranked by live capacity hints (free slots, model residency, memory headroom), with preferred-node routing and failover before the response begins

See [the roadmap](docs/roadmap.md) for milestone status and the work remaining for production acceptance.

## Components

| Component | Crate | Responsibility |
| --------- | ----- | -------------- |
| Bloomd | `crates/bloomd` | Persistent system daemon (binary `bloomd`) |
| Core domain | `crates/bloom-core` | Ids, capacity vector, node descriptors, config, routing options |
| Harness contract | `crates/bloom-harness` | Universal `InferenceHarness` trait + mock harness |
| Ollama adapter | `crates/bloom-harness-ollama` | Ollama lifecycle, inventory, load/unload, streaming inference, cancellation |
| Telemetry | `crates/bloom-resources` | Capacity vector sampling via `sysinfo` and `/sys` probes |
| Storage | `crates/bloom-storage` | SQLite ledger (jobs, events, kv state) |
| Network API | `crates/bloom-api` | Axum router: `/v1/*` (OpenAI-compatible) and `/bloom/v1/*` (control) |
| Discovery | `crates/bloom-discovery` | LAN advertisement and candidate registry |
| CLI and node app | `crates/bloom-cli` | Operator CLI (`bloom`) and Goya node dashboard (`bloom-node-app`) |

## Quick start

```sh
cargo build -p bloomd -p bloom-cli
./target/debug/bloomd                          # defaults: 127.0.0.1:7420, ~/.local/share/bloom
```

With Ollama running locally:

```sh
./target/debug/bloom infer "Summarise the Bloom architecture" --model qwen2.5:7b --stream
./target/debug/bloom status
./target/debug/bloom resources
```

Configuration is read from `bloomd.toml` in the working directory, `~/.config/bloom/bloomd.toml`, or `/etc/bloom/bloomd.toml`. See `platform/linux/bloomd.toml.example` and `platform/linux/bloomd.service` for a systemd deployment.

## LAN and peer setup

Enable `[swarm] discovery = true` to browse for Bloom nodes on the LAN. Set
`advertise = true` and `advertise_https_port` to announce the port of a local
HTTPS endpoint. This can be a reverse proxy while Bloom stays on loopback, or
Bloom's own TLS listener configured with `api.tls_cert` and `api.tls_key`.
An advertised port without the HTTPS override uses the daemon's non-loopback
HTTP bind.
Browse results from `bloom discovery` are unauthenticated identity claims.

Serve each inference peer through HTTPS, either with Bloom's TLS listener or
an HTTPS reverse proxy. Set `api.token_env` for operator access and
`api.peer_token_env` for a distinct
credential limited to node status and direct inference. Supply the token
values through the daemon environment. The Linux unit reads
`/etc/bloom/bloomd.env` if present.

On the routing node, configure each peer explicitly.

```toml
[swarm]
discovery = true
# ca_pem = "/etc/bloom/lan-ca.pem"

[[swarm.peers]]
node_id = "verified-peer-id"
url = "https://bloom-peer.example"
token_env = "BLOOM_PEER_TOKEN"
```

Set `BLOOM_PEER_TOKEN` in the routing daemon's environment to the receiving
node's peer token. The client verifies the certificate and peer ID before
sending inference. For a private CA, set `swarm.ca_pem` to the trusted PEM
certificate bundle. Set `ollama.enabled = false` on a node dedicated to routing.
With the receiving peer's token available as `BLOOM_API_TOKEN`, you can fetch
its authenticated ID and print the peer entry without exposing the token.

```sh
bloom --url https://bloom-peer.example --ca-pem /etc/bloom/lan-ca.pem \
  peer-config --token-env BLOOM_PEER_TOKEN
```

```sh
bloom discovery
bloom status
bloom infer --swarm --model qwen2.5:7b --stream "Summarise the Bloom architecture"
```

Use `--preferred-node <id>` to select a node first and `--no-failover` to limit
the request to its first target. The executing node owns the job ledger.
The operator CLI can drain or resume a node (`bloom drain`, `bloom drain
--resume`), cancel a job (`bloom cancel <id>`), and manage model residency
(`bloom load <model>`, `bloom unload <model>`).

## Network API

OpenAI-compatible (with the Bloom routing extension):

```json
{
  "model": "qwen2.5:7b",
  "messages": [{"role": "user", "content": "Analyse this document."}],
  "stream": true,
  "bloom": {
    "routing": "direct",
    "preferred_node": null,
    "allow_failover": true,
    "allow_model_loading": true,
    "allow_cloud": false
  }
}
```

Control surface:

| Endpoint | Function |
| -------- | -------- |
| `GET /v1/models` | Harness model inventory, OpenAI format |
| `POST /v1/chat/completions` | Direct or swarm inference, streaming or buffered |
| `POST /bloom/v1/inference` | Bloom-native inference with job ledger tracking |
| `GET /bloom/v1/swarm`, `/bloom/v1/nodes` | Node descriptor, status, capacity freshness |
| `GET /bloom/v1/discovery` | Unauthenticated LAN advertisements, available to the operator |
| `GET /bloom/v1/resources` | Latest capacity vector |
| `GET /bloom/v1/jobs`, `POST /bloom/v1/jobs/cancel` | Execution ledger, cancellation |
| `POST /bloom/v1/models/load`, `/bloom/v1/models/unload` | Model residency management |
| `POST /bloom/v1/reservations` | Atomic local resource reservation |
| `POST /bloom/v1/nodes/drain` | Refuse new work (GUI-safe, job-safe) |
| `GET /bloom/v1/events` | SSE event stream |
| `POST /v1/embeddings`, `/v1/audio/transcriptions` | Reserved; `501` until the harness matrix lands |

With an operator token configured, send `Authorization: Bearer <token>` on
each request. The CLI accepts this token from `BLOOM_API_TOKEN`. A daemon
without authentication can bind only to loopback. The default built-in
listener uses plaintext; configure `api.tls_cert` and `api.tls_key` or place
it behind HTTPS for access across the network.

## Development

```sh
cargo test --workspace        # unit + API integration tests
cargo clippy --workspace --all-targets
scripts/check.sh              # fmt + clippy -D warnings + tests (the kaptaind gate)
uni --fail-under 80 .
```

See `docs/architecture.md` for the component ontology, execution lifecycle and design decisions, and `docs/roadmap.md` for the phased plan and acceptance targets.
