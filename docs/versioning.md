# Versioning policy

`livekit-rs-voice` releases are tagged `vX.Y.Z.R` and published as multi-arch (linux/amd64, linux/arm64) Docker images to Docker Hub and GHCR. One tag releases both services — `livekit-rs-voice` and `livekit-rs-egress` — as a matched image pair.

## Components

- **`X.Y.Z` — wire/protocol level**, mirroring [livekit-server](https://github.com/livekit/livekit) releases. `1.13.5` means "the protocol of livekit-server 1.13.5". It is bumped whenever we pick up upstream protocol patches, so it always reflects the LiveKit protocol level the server implements.
- **`R` — build/patch revision**: a counter for changes that do not alter the wire protocol (bug fixes, robustness, tests, tooling, docs). `1.13.5.1` is protocol level 1.13.5 with our first post-release fix set.

## What bumps what

| Change | Component |
|---|---|
| Protocol/wire changes, upstream protocol patches | `X.Y.Z` |
| Bug fixes, robustness, tests, tooling, docs | `R` |

## Wire version vs release version

The `server_version` advertised in the `JoinResponse` (and the Cargo package version) is the protocol level `X.Y.Z`, independent of the release revision. A release tagged `v1.13.5.3` still advertises `server_version: 1.13.5`.

## Releasing

```bash
git tag v1.13.5.1
git push origin v1.13.5.1
```

This triggers the Release workflow, which natively builds both images for amd64 and arm64 and publishes `n1snt/livekit-rs-voice:1.13.5.1` and `n1snt/livekit-rs-egress:1.13.5.1` (plus `:latest`) on Docker Hub and GHCR.

Tags are immutable pointers to image digests: fixes ship in a new revision tag, never by rewriting an existing tag.