# Bloom roadmap

Proposed milestones, adapted from the original specification. Status reflects this repository.

## 01 — Autonomous inference node — **implemented (first pass)**

Linux daemon (`bloomd`), Ollama adapter (attached + managed), direct OpenAI-compatible API with the `bloom` routing extension, local telemetry, SQLite ledger, reservations, drain, cancellation, SSE events, operator CLI.

Outstanding for hardening: sustained-load soak, idle resource profiling against the acceptance targets below.

## 02 — LAN swarm formation

Implemented: opt-in mDNS/DNS-SD browsing and advertisement, an in-memory candidate registry, and explicit provisioning of authenticated HTTPS peers.

Outstanding: interactive pairing, cryptographic node identities, QUIC transport, distributed registry replication, and validation on a multi-device LAN.

Acceptance: discovery under 3 s on an eligible LAN; full local operation without WAN.

## 03 — Distributed inference

Implemented: configured-peer forwarding, preferred-node selection, streaming proxy, pre-response failover, local admission on the executing node, and hint-based peer ranking. Peers are probed concurrently and ranked by free execution slots, model residency, slot count and live memory headroom (net of active reservations). Peers that predate the hint rank last but stay eligible.

Outstanding: a distributed reservation protocol, measured-throughput scoring, and measured scheduling overhead.

Acceptance: warm scheduling overhead under 25 ms p95; zero reservation race failures; unauthorized inference rejected.

## 04 — Goya Control Centre

Native scenes (`overview`, `nodes`, `node`, `models`, `jobs`, `routing`, `security`, `resources`, `settings`) speaking exclusively through the control API. Closing or recompiling the frontend must never interrupt executing jobs — the daemon already owns execution and the ledger.

## 05 — Cross-platform expansion

Windows service, macOS launchd, Android/iOS foreground-service constraints, restricted television clients. Availability must be advertised honestly (`AvailabilityClass::Continuous` vs `Restricted { reason }` already models this).

## 06 — Advanced resource orchestration

Ollama residency telemetry uses the live running-model inventory. Model prewarming, replication, thermal scheduling, measured-throughput scoring and the expanded harness matrix (llama.cpp, MLX, ONNX Runtime, vLLM, Iberia) remain open.

## 07 — Enterprise hardening

Implemented: bearer credential comparison, a scoped peer credential, exclusive data-directory ownership and recovery of unfinished ledger entries after restart.

Outstanding: per-client quotas, fault injection, security verification, enterprise auditability, and a 72-hour swarm endurance test.

## Initial engineering acceptance targets

| Test | Target |
| ---- | ------ |
| Node discovery | Under 3 s on an eligible LAN |
| Warm scheduling overhead | Under 25 ms p95 |
| Node telemetry freshness | Under 2 s during normal operation |
| Daemon idle CPU usage | Under 1% on reference desktop hardware |
| Daemon idle memory | Under 50 MB, excluding harness |
| Frontend independence | No interrupted jobs when GUI closes |
| Internet independence | Full local operation without WAN |
| Resource oversubscription | Zero reservation race failures |
| Endurance | 72-hour continuous swarm test |
| Unauthorized inference | Zero accepted requests |

Discovery and scheduling targets are gated on Milestones 02–03. Telemetry freshness and oversubscription are covered by the current design and tests.
