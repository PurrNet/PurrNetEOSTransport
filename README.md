# PurrNet EOS Transport

[Epic Online Services](https://dev.epicgames.com/en-US/services) P2P transport for [PurrNet](https://purrnet.dev/). Drop it on your `NetworkManager` and PurrNet rides on top of EOS. No relay to host, no extra glue.

## Install

Easiest path: open **Tools / PurrNet / PurrNet Packages** and hit install on EOS Transport. One click and you're done.

If you'd rather pull it in by hand, open Unity's Package Manager, click **Add package from git URL**, and paste:

```
https://github.com/PurrNet/PurrNetEOSTransport.git?path=/Assets/EOSTransport#dev
```

You can also pin to a specific release tag with `#v1.0.0-beta.1` (or whatever the latest tag is) instead of `#dev`.

You'll also need the [PlayEveryWare EOS Plugin](https://github.com/PlayEveryWare/eos_plugin_for_unity_upm). Same flow, Package Manager, **Add package from git URL**:

```
https://github.com/PlayEveryWare/eos_plugin_for_unity_upm.git
```

That gives you the EOS SDK and `EOSManager`. The transport's runtime code is gated by a `versionDefine` on `com.playeveryware.eos`, so it only compiles once the plugin is in.

Then set up your Epic dev account and product credentials under **Tools / EOS Plugin / EOS Configuration**.

## Usage

1. Add the `EOSTransport` component to your `NetworkManager`.
2. Set `socketName` (any string, both peers just need to agree on it).
3. On the client, set `remoteProductUserId` to the host's EOS Product User ID before connecting.
4. `NetworkManager.StartServer()` to host, `NetworkManager.StartClient()` to join.

If you call `StartClient()` on the same `NetworkManager` that's already hosting, it short-circuits to in-process delivery instead of going through EOS. Host loopback, no extra wiring.

## Attribution

Forked from [`quentinleon/PurrNetEOSTransport`](https://github.com/quentinleon/PurrNetEOSTransport) (MIT, © 2025 Quentin Leon) and rewritten with an `EOSPeer` abstraction, a fragmentation queue with `LimitExceeded` backpressure, host loopback, channel-to-reliability mapping, and notification-handle cleanup. Both copyright lines live in [`LICENSE`](./LICENSE).

## License

MIT. See [`LICENSE`](./LICENSE).
