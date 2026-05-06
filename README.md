# PurrNet EOS Transport

An [Epic Online Services](https://dev.epicgames.com/en-US/services) P2P transport for [PurrNet](https://purrnet.dev/), built on top of the [PlayEveryWare EOS Unity Plugin](https://github.com/PlayEveryWare/eos_plugin_for_unity_upm).

Use it when you want EOS-friend-list / lobby flows to drive your PurrNet connection without standing up a relay of your own.

## Install

Add the package to your Unity project's `Packages/manifest.json`:

```json
"dev.purrnet.eostransport": "https://github.com/PurrNet/PurrNetEOSTransport.git?path=/Assets/EOSTransport#release"
```

For the in-development branch, swap `#release` for `#dev`.

### Required dependencies

| Package | Why |
|---|---|
| [PurrNet](https://github.com/PurrNet/PurrNet) | The networking layer this transport plugs into. |
| [PlayEveryWare EOS Plugin](https://github.com/PlayEveryWare/eos_plugin_for_unity_upm) (`com.playeveryware.eos`) | Provides the EOS SDK + `EOSManager`. The transport is gated by a `versionDefine` on this package, so the runtime code only compiles when it's present. |

You'll also need an Epic dev account and product credentials configured via **Tools → EOS Plugin → EOS Configuration**.

## Usage

1. Add the `EOSTransport` component to your `NetworkManager`.
2. Configure `socketName` (any string both peers agree on) and `remoteProductUserId` (the host's EOS Product User ID — set this on the client side before connecting).
3. Server starts via `NetworkManager.StartServer()`; client connects via `NetworkManager.StartClient()`.

Host loopback is handled automatically — calling `StartClient()` on the same `NetworkManager` after `StartServer()` short-circuits to in-process delivery instead of routing through EOS.

## Attribution

Originally derived from [`quentinleon/PurrNetEOSTransport`](https://github.com/quentinleon/PurrNetEOSTransport) (MIT, © 2025 Quentin Leon). This version is substantially rewritten — it adds an `EOSPeer` abstraction, fragmentation queue with `LimitExceeded` backpressure, host loopback, channel-to-reliability mapping, and notification-handle cleanup guards. Maintained under the PurrNet org.

Both copyright lines are preserved in [`LICENSE`](./LICENSE).

## License

MIT — see [`LICENSE`](./LICENSE).
