# Contributing

Thanks for considering a contribution to `livekit-rs-voice`. The project is
deliberately small, so please read this before opening a PR. The full rules
live in [AGENTS.md](AGENTS.md); this file is the short version.

## Scope

- Audio only. Video is out of scope.
- Single node, with Rust-native Redis clustering planned. There is no
  multi-node operation or room migration yet.
- Wire compatibility with LiveKit is the contract. Behavior, config, and the
  API mirror `livekit-server`; anything that does not is listed under
  "Differences from LiveKit" in the readme.

## Running the checks

All four must pass before you submit:

```bash
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all
```

CI runs the same checks on every push and PR.

## Submitting a change

- Open an issue first for anything non-trivial, then a pull request that links
  to it.
- Keep the checks above green and add or update tests with your change: unit
  tests beside the module, plus the integration tests in
  `crates/lk-server/tests/`.
- Update `CHANGELOG.md` (Keep a Changelog format) for any user-visible change.
- If users are affected, also update the "Differences from LiveKit" section in
  `readme.md`.

## License

By contributing you agree that your work is licensed under the same
Apache-2.0 license as the project.
