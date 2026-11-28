# Bloom architecture

This document records the implemented node and swarm architecture. It complements the Bloom roadmap.

## Component ontology

| Component | Status | Notes |
| --------- | ------ | ----- |
| **Bloomd** | Implemented | Tokio daemon; owns the harness, telemetry loop, ledger and HTTP/HTTPS surface |
| Harness | Contract implemented | `InferenceHarness` trait in `bloom-harness`; Ollama adapter + mock |
| Swarm | Configured peers | An operator provisions each trusted ID, HTTPS URL and bearer credential |
| Router | Hint-ranked | Prefers the requested node, then ranks verified peers by their scheduling hint; can fall back before any response is forwarded |
| Registry | Local plus discovery | SQLite holds the local ID and capacity snapshot. A bounded in-memory registry tracks mDNS records |
| Ledger | Implemented | `jobs` table: queued → running → completed / failed / cancelled / interrupted |
| Control Centre | Not yet | Goya scenes are Milestone 04; the HTTP control API is ready for it |
| Gateway | Direct and swarm | OpenAI-compatible chat and Bloom-native inference can forward to trusted peers |

## Daemon shape

`bloomd` validates the bind, credentials and optional TLS certificate, locks the data directory, opens SQLite, recovers interrupted jobs and loads the node ID. It then initializes telemetry and optional LAN discovery, starts the harness and serves HTTP or HTTPS. A harness startup failure leaves the control API available, and `ollama.enabled = false` makes the daemon a routing-only node. Shutdown drains connections for up to ten seconds before stopping the harness.

Operator connections can close independently of inference connections. When an inference connection closes before completion, the daemon releases the local execution slot and records the interruption. At startup, the exclusive data lock allows the daemon to mark jobs left active by a prior process as interrupted.

## Harness contract

The trait matches the specification contract with two additions: `kind()` for harness identification and `unload_model()` for residency management. Streaming is a pinned `Send` stream of typed events (`Start`, `Chunk`, `Usage`, `Done`, `Failed`), so adapters can map native stream formats (Ollama NDJSON, harness stdout, etc.) without leaking transport details into the API layer.

Two execution modes are supported for Ollama:

- **attached** — Ollama is already running; `start()` polls `/api/version` until reachable
- **managed** — `bloomd` spawns `ollama serve` and kills it on shutdown

Cancellation is cooperative: each in-flight request registers a watch channel; `cancel` signals the pump task, which stops reading the upstream stream and emits `Done { finish_reason: "cancelled" }`. Dropping the stream also tears the request down (client disconnects propagate).

## Telemetry and the capacity vector

`bloom-resources` samples the full capacity vector from §5 of the specification at the configured interval: CPU (arch, brand, cores, utilisation, available threads), GPU presence via `/sys/class/drm` (vendor resolved from PCI ids), NPU presence via `/sys/class/accel`, memory, storage (disk chosen by mount-point prefix), model inventory with residency, network hints, inference load, thermal readings (throttle flag at ≥85 °C) and power (battery status from `/sys/class/power_supply`). Unknown values are reported as `null` rather than fabricated — restricted and partially instrumented devices must not falsely advertise capacity.

Snapshots are cached in the daemon state (`/bloom/v1/resources`) and persisted to the `kv` table (`capacity:<node>`).

## Reservations

`ReservationRegistry` performs the atomic check-and-insert under a single mutex: expired reservations are swept first, the outstanding total is computed, and a request succeeds only when `requested ≤ available − reserved`. This is the local primitive the distributed scheduler will build on in Milestone 03; stale telemetry can never oversubscribe the executing node because the reservation decision is made against live local state at the executing node.

## Execution lifecycle

1. Receive inference request (OpenAI shape, optional `bloom` extension)
2. Resolve requirements (model id, modality via inventory capabilities)
3. Eligibility: draining flag and active-job limit gate new work
4. Residency: load through the harness when `allow_model_loading` is set, otherwise fail with `404`
5. Ledger: job recorded (`queued` → `running`), `job.started` event published
6. Dispatch through the selected harness
7. Stream response (OpenAI SSE chunks or Bloom-native SSE events); output size and usage accounted on completion
8. Release and account: terminal state written (`completed`, `cancelled`, `interrupted`, `failed`), event published

Failures before output produce a failed job and a clean HTTP error. Failures after streaming begins mark the job `interrupted`; outputs from independent attempts are never silently concatenated.

## Peer transport and trust

mDNS advertisements on `_bloom._tcp.local.` populate the candidate registry.
The discovery API exposes identity claims and addresses. An operator
provisions trusted peers separately using authenticated endpoint information.

The router checks the HTTPS certificate and reads the peer's authenticated
node descriptor before forwarding a prompt. The same probe
(`GET /bloom/v1/nodes?model=<id>`) returns a `scheduling` hint: free
execution slots, whether the model is resident, and available memory net of
live reservations. Probes run concurrently. A requested node goes first, then
peers rank by (has a free slot, model resident, slots, memory), with
configured order breaking ties. The hint is advisory. The executing node still
makes the binding admission decision against its own live state. It sends requests in direct
mode to stop further forwarding. A scoped peer credential permits node
status and direct inference. The executing node owns the job, cancellation
and resource admission. Gateway failover ends once it forwards the response.

## Testing

- Unit tests: config parsing, capacity aggregation, reservation races, NDJSON parsing, mock harness streaming, SQLite ledger lifecycle
- Integration tests (`bloom-api/tests/api.rs`): the full HTTP surface against the mock harness — models, resources, swarm, buffered and streaming chat, drain gating, oversubscription conflicts, fail-closed auth, cancellation including mid-stream, and the SSE event bus
