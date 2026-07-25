# Working on adbc-driver-monetdb

An ADBC driver for MonetDB in Rust: reads decode the binary result-set protocol
(`Xexportbin`) directly into Arrow record batches, writes stream Arrow columns through
`COPY BINARY ... ON CLIENT`, and a thin Python shim exposes it all to polars/pandas via
`adbc-driver-manager`. The [README](README.md) covers usage and layout. This file
covers the conventions that are easy to get wrong.

## Documentation map

- **Read [docs/design-decisions.md](docs/design-decisions.md) before changing or
  proposing component boundaries, client identity, result scheduling or memory,
  error semantics, test infrastructure, release design, or performance work.** It is
  a decision record, not a backlog. Do not reopen a measured optimization rejection
  without new reproducible evidence; record the commands, workload, environment, and
  repeated results in the pull request.
- The README is the primary user guide. `docs/monetdb.md` is the source template for
  generated ADBC validation documentation and the standalone DBC release package.
  Keep both current when installation, connection options, support policy,
  feature/type coverage, or packaging behavior changes.

## Hard support policy — no compatibility code

- **MonetDB Dec2025 (11.55)+ and little-endian servers only.** Fail fast at connect;
  never add fallbacks for older servers and never decode text-protocol result sets.
- **Current-stable consumers only:** Python ≥ 3.13, polars ≥ 1.42,
  adbc-driver-manager ≥ 1.11. Don't add version-compat shims — bump the pins instead.
- **No PyPI release (no `v*` tag) until every required row in the README's release
  baseline is supported and the acceptance gates are green.** Optional or
  backend-inapplicable rows may remain unsupported only when the matrix names them
  explicitly and tests verify `NotImplemented`. No development-status classifiers or
  alpha messaging anywhere.

## The monetdb-rust submodule is a fork, not vendored code

- `monetdb-rust/` is a git submodule of [wlaur/monetdb-rust](https://github.com/wlaur/monetdb-rust),
  a fork of the official `MonetDB/monetdb-rust`. Clone with `--recurse-submodules`.
- Protocol work is committed *inside the submodule* (push to the fork), then the pointer
  bump is committed here. Keep those changes upstream-shaped: MPL-2.0 headers on every
  file (`./checklicense.py` in the fork enforces this), no driver-specific hacks.
- License boundary: anything generic-MAPI belongs in the fork (MPL-2.0); anything
  ADBC/Arrow-specific belongs in `crates/` (MIT).

## Rust

- Edition 2024. Gates (CI denies warnings): `cargo fmt --all -- --check`,
  `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`.
- **Every `unsafe {}` block carries a `// SAFETY:` comment** (rationale + error modes) —
  `clippy::undocumented_unsafe_blocks` — and unsafe operations inside `unsafe fn` need
  explicit blocks (`unsafe_op_in_unsafe_fn`). Both are workspace lints; CI makes them
  errors.
- One `arrow` major per workspace (currently 58; `adbc_core` 0.23 requires `<59`). Bump
  `adbc_core`/`adbc_ffi`/`arrow` together, never independently.
- `cargo audit --deny warnings` is a CI gate. A temporary advisory exception requires
  the exact RustSec ID, rationale, owner, and removal deadline in the workflow change.
- Wire-format facts must cite the MonetDB source or docs in a comment (see the frame
  parser). Known trap: the temporal structs' field named `ms` holds **microseconds** —
  don't "fix" it.

## Python

- `uv` only, never pip; committed lockfile (CI runs `uv sync --locked`).
- **pyright (strict) is the only type checker** — zero errors. Blanket `# type: ignore`
  is disabled; use rule-specific `# pyright: ignore[rule]`, and delete a suppression in
  the same change that makes it unnecessary (`reportUnnecessaryTypeIgnoreComment` is an
  error). `ruff check` + `ruff format` gate everything else.
- Two names are load-bearing for polars integration; never rename them:
  the import package `adbc_driver_monetdb` (polars resolves `adbc_driver_{scheme}` from
  `monetdb://` URIs), and `dbapi.connect` returning a plain
  `adbc_driver_manager.dbapi.Connection` (polars detects ADBC by that class's module and
  isinstance-checks it on the write path).
- Mixed build via maturin: the driver cdylib ships as the extension module
  `adbc_driver_monetdb._native` (abi3, cp313). `uv sync` builds it; the editable install
  drops the compiled `_native` inside the source package directory.
- Don't write unnecessary code comments or docstrings.

## Tests

- `uv run pytest -m "not integration"` needs no server. Integration tests need
  `MONETDB_TEST_URI` and a dockerized `monetdb/monetdb:Dec2025-SP3` (command in
  `tests/conftest.py`); they skip when the variable is unset.
- Not-yet-implemented behavior: write the test now and mark it
  `xfail(strict=False, reason="...")`; remove the marker when the milestone lands.
  Don't delete or skip instead.
- Never run pytest against an *installed wheel* from the repo root — the source package
  shadows it on `sys.path` (CI runs the wheel suites from `runner.temp` for exactly this
  reason). The editable dev install is fine from anywhere.

## CI, branch protection, release

- `main` takes pull requests only (0 approvals required, `enforce_admins` on — applies
  to everyone): branch → PR → all 19 required checks green → merge. Delete merged
  branches.
- Required status checks are matched by exact job name; renaming a job in
  `.github/workflows/ci.yml` requires updating the branch-protection rule in the same
  change.
- Release (once feature parity is reached): bump the version in `pyproject.toml`,
  `Cargo.toml`, `uv.lock`, and all `packaging/dbc/MANIFEST.*.toml` files, PR, then tag `v<version>` —
  the workflow builds wheels for all four platforms and publishes via PyPI Trusted
  Publishing, then creates the GitHub Release.

## Public repo hygiene

Keep the repo self-contained: no references to private projects, internal benchmarks, or
company infrastructure. Cite public sources only (the MonetDB repo's protocol docs,
pymonetdb, apache/arrow-adbc, polars).
