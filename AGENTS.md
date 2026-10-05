# Zeron checkout work

Read [CONTRIBUTORS.md](CONTRIBUTORS.md), [ARCHITECTURE.md](ARCHITECTURE.md),
and [CONTEXT.md](CONTEXT.md). Preserve existing branches and unrelated changes.

## Setup and verification

- Run `bash scripts/setup.sh` in a fresh checkout or worktree. It fetches the
  locked Cargo dependencies, installs the npm lockfile under `edge`, and builds
  the edge locally with Wrangler's dry run. Rust binaries compile on first use;
  setup does not install host tools or start the daemon, app, or edge server.
- Rust follows `rust-toolchain.toml`. Use Node 24 as in edge CI. Host libraries
  for native builds are listed in CONTRIBUTORS.md; Linux also needs a C compiler.
- The T3 Check action reuses `scripts/ci/test-core.sh` and both edge test tiers,
  with two build/test workers. The Rust script requires cargo-nextest. Linux
  hosts can provision its pinned binary with `bash scripts/ci/install-nextest.sh`;
  macOS CI records the corresponding verified archive and checksum.
- Run the additional native/UI, mobile, protocol, and integration checks required
  for the crates touched by a change. Check does not replace the contributor
  guide's platform-specific gates or prove installed app behavior.
- Import `t3.json` actions into the selected T3 project and environment. Setup
  runs on worktree creation and must finish before the agent starts. Verify a
  fresh automatic run separately from successful standalone setup.

## Parallel work and machines

- Give independent tasks separate branches and worktrees. Each checkout owns
  its `target`, `edge/node_modules`, edge build, and `.wrangler` state. Keep one
  integration owner for overlapping crate, Cargo.lock, and protocol edits.
- Never launch development builds against the installed app's data directory or
  IPC port. Set unique `ZERON_DATA_DIR` and `ZERON_IPC_PORT`, and use
  `ZERON_HARNESS=mock` for synthetic work without a real provider or account.
- On macOS, `scripts/run-macos-dev.sh` accepts `ZERON_DEV_DATA_DIR`,
  `ZERON_DEV_IPC_PORT`, and `ZERON_DEV_BUNDLE_ID`. Give every concurrent app a
  distinct port and bundle identity as well as a checkout-owned data directory.
  The demo script has fixed shared `/tmp` paths and a fixed port, so do not run
  concurrent demos or assume it is isolated per worktree.
- Edge development needs an unused port, for example
  `npm run dev --prefix edge -- --port 27641`. Keep its local persistent state
  inside the checkout and do not attach to another task's server. Serialize
  heavy native builds/tests on a shared host rather than starting many at once.
- Exchange committed branches between machines. T3 grouping does not sync
  source or dependencies. Existing upstream worktrees and installed app profiles
  remain separate from this fork's development checkout.
- Setup and Check do not log in, install/update services, publish releases,
  deploy Cloudflare resources, or use live agent credentials. Keep provider and
  messaging acceptance separate from offline fixtures and source checks.
