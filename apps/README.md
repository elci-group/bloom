# Bloom Node Goya app

`bloom-node.gscene` is a device dashboard for exposing each client device as a
Bloom compute node. It shows the local node, available resources, models, LAN
discovery and trusted peers. The scene uses stable Goya element IDs so a native
host can connect the controls to Bloom without putting credentials in the
scene.

## Host integration

A Goya scene is the view description. It does not make HTTP requests or start
services by itself. The `bloom-node-app` controller:

1. Connects to that device's local `bloomd` API using `BLOOM_API_TOKEN` when
   configured.
2. Refreshes `bloom.connection`, `bloom.node_state`, `bloom.node_id`,
   `bloom.cpu_value`, `bloom.memory_value`, `bloom.slots_value`,
   `bloom.accelerator_value`, `bloom.model_value`, `bloom.discovery_value` and
   `bloom.peer_value` from the API. The relevant endpoints are
   `GET /bloom/v1/swarm`, `GET /bloom/v1/nodes`,
   `GET /bloom/v1/resources`, `GET /bloom/v1/discovery` and
   `GET /v1/models`.
3. Maps `bloom.refresh` to a refresh and `bloom.pause_sharing` to
   `POST /bloom/v1/nodes/drain` with `{"enabled":true}`. On
   `bloom.enable_sharing`, it verifies HTTPS and advertisement settings,
   checks that the public endpoint returns this device's node ID, then calls
   the drain endpoint with `{"enabled":false}`.
4. Reports setup errors in `bloom.action_status` and keeps the enable control
   unavailable until a safe remote endpoint is configured.

Pausing stops new inference on the node. Enabling sharing resumes inference
after the controller confirms that the secure endpoint and LAN advertisement
are already configured. The app does not change daemon bind settings or turn
mDNS advertisement on at runtime.

## Secure access setup

Bloom can serve HTTPS directly, so a reverse proxy is optional. Generate a
device certificate and private key with:

```sh
bloom-node-app --generate-cert-for bloom-node.local
```

This writes `bloom-node.crt` and a mode-600 `bloom-node.key` under
`~/.config/bloom/tls`. Use a DNS name or IP address that other devices on the
LAN can resolve and reach. Configure the daemon to bind on the LAN and use the
certificate:

```toml
[api]
bind = "0.0.0.0:7420"
token_env = "BLOOM_API_TOKEN"
peer_token_env = "BLOOM_INFERENCE_TOKEN"
tls_cert = "~/.config/bloom/tls/bloom-node.crt"
tls_key = "~/.config/bloom/tls/bloom-node.key"

[swarm]
discovery = true
advertise = true
advertise_https_port = 7420
```

Set both tokens in the daemon environment and restart `bloomd`. The app checks
that the HTTPS URL presents this node's ID before enabling inference. Use the
same certificate as the trust bundle for the app and routing peers:

```sh
BLOOM_API_TOKEN=... bloom-node-app \
  --public-url https://bloom-node.local:7420 \
  --ca-pem ~/.config/bloom/tls/bloom-node.crt
```

On routing devices, set `swarm.ca_pem` to the certificate and provision this
node's ID, HTTPS URL, and peer token. The certificate is self-signed: share it
with devices you trust, and verify its fingerprint out of band. The app never
displays or stores bearer tokens in scene text. Discovery records remain
untrusted identity claims until an operator provisions them as peers.

The `bloom-node-app` binary in `bloom-cli` is a Linux controller for the scene.
It opens the installed `goya-scene` host, reads button events, updates the
scene, talks to local `bloomd`, and verifies the public HTTPS endpoint before
resuming inference. It refuses to enable sharing unless
`swarm.advertise=true`, operator authentication is configured, the advertised
HTTPS port matches the URL, and the public endpoint presents the local node ID
over a valid TLS connection.

The operator still needs to install Goya, configure HTTPS and LAN
advertisement, and restart `bloomd` after editing its configuration. A reverse
proxy can be used instead of the direct TLS listener when one is already
available. For a private CA, pass `--ca-pem /path/to/lan-ca.pem` and install
that same bundle on routing nodes through `swarm.ca_pem`. The app keeps normal
certificate and node-identity verification on.

Use `--config`, `--api-url` or `--goya-scene` when the local paths or addresses
differ from their defaults. `BLOOM_PUBLIC_URL` can replace `--public-url`. The
app never writes credentials into the scene.

## Bloom Pocket (Android, Aragon)

`bloom-pocket.gscene` is a phone-side Bloom node card written for Aragon, Goya's
Android scene compiler. It uses the GSSH/4-live profile: live `text-from`
labels, `present-when` to switch between the share and pause controls, and
durable state (`sharing`, `slots`, `node_name`) kept in Android's default local
store (`SharedPreferences`, `android.preferences.v1`). The "Fewer" slots button
is hidden until at least two slots are offered, so the count never drops below
one.

The scene is self-contained. Aragon refuses `host` state and D2 effects, so this
app does not talk to `bloomd`. It records which name, sharing setting and slot
count the operator wants for the phone.

Compile it into an Android Gradle project with an Aragon build that includes
the live profile (`crates/goya-sdk/src/android_live.rs`):

```sh
aragon-android-project apps/bloom-pocket.gscene target/aragon/bloom-pocket --scene
```

The output directory must not already exist. Building the APK
(`./gradlew assembleRelease` in the output) needs the locked toolchain in Goya's
`docs/aragon/android-toolchain-lock.md`: Temurin JDK 21.0.13, Android SDK
platform 35 rev 2 and build-tools 34.0.0.
