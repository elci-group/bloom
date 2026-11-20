# macOS user service

This package runs `bloomd` as a per-user `launchd` agent. It starts at login and
stops at logout, so the sample advertises restricted availability. It binds the
API to loopback and contains no credentials.

## Install

Build the macOS `bloomd` binary and install it at the path used by the plist:

```sh
sudo install -d -m 755 /usr/local/libexec
sudo install -m 755 target/release/bloomd /usr/local/libexec/bloomd
```

Create the service directories and copy the configuration template:

```sh
mkdir -p "$HOME/Library/LaunchAgents" \
  "$HOME/Library/Application Support/Bloom" \
  "$HOME/Library/Application Support/Bloom/data" \
  "$HOME/Library/Logs/Bloom"
cp platform/macos/bloomd.toml.example \
  "$HOME/Library/Application Support/Bloom/bloomd.toml"
```

Replace `/Users/REPLACE_ME` with your home directory in both the config and
plist. Set `data_dir` to an absolute path owned by your user. Copy the plist:

```sh
cp platform/macos/org.bloom.bloomd.plist \
  "$HOME/Library/LaunchAgents/org.bloom.bloomd.plist"
```

Validate and load the agent:

```sh
plutil -lint "$HOME/Library/LaunchAgents/org.bloom.bloomd.plist"
launchctl bootstrap "gui/$(id -u)" \
  "$HOME/Library/LaunchAgents/org.bloom.bloomd.plist"
```

To stop and unload it:

```sh
launchctl bootout "gui/$(id -u)" \
  "$HOME/Library/LaunchAgents/org.bloom.bloomd.plist"
```

Logs are written to `~/Library/Logs/Bloom/bloomd.log`. Configure API
credentials outside the plist if remote binding is required; do not put secrets
in the plist or example config.
