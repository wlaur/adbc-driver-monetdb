# Contributing

Report reproducible bugs and request features through this repository's
[issue tracker](https://github.com/wlaur/adbc-driver-monetdb/issues). Include the MonetDB version,
client platform, a minimal query or Arrow schema, and the complete ADBC status and SQLSTATE when
available.

Do not disclose a suspected security vulnerability in a public issue. Use GitHub's
[private vulnerability reporting](https://github.com/wlaur/adbc-driver-monetdb/security/advisories/new)
instead. Contributions and project discussions must follow the
[GitHub Community Guidelines](https://docs.github.com/en/site-policy/github-terms/github-community-guidelines).

## Development setup

Clone the repository with its protocol fork and install the locked development environment:

```sh
git clone --recurse-submodules https://github.com/wlaur/adbc-driver-monetdb
cd adbc-driver-monetdb
uv sync --locked
docker compose up -d
```

The Compose service pins the native ARM64 Dec2025-SP3 image built from the public
[`wlaur/monetdb-container`](https://github.com/wlaur/monetdb-container) recipe.
Set `MONETDB_TEST_URI=monetdb://monetdb:monetdb@localhost:50000/test` for integration tests.
Protocol-generic MAPI changes belong in the `monetdb-rust` submodule; Arrow- or ADBC-specific
changes belong in this repository.
Durable architecture and performance choices are recorded in
[`docs/design-decisions.md`](docs/design-decisions.md); it is a decision record, not a backlog.

## Pull requests

Create a topic branch and keep commits focused. Follow the existing Rust and Python style, add
tests for behavior changes, and preserve the no-compatibility support policy in `AGENTS.md`. Run
the same gates as CI before opening a pull request:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo audit --deny warnings
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest -m "not integration"
MONETDB_TEST_URI=monetdb://monetdb:monetdb@localhost:50000/test uv run pytest -m integration
```

Changes are accepted under the repository's MIT and MPL-2.0 license boundary described in
`AGENTS.md` and `NOTICE`.

## Review matrix

A green happy-path test is not evidence for neighboring states. Review behavior changes against
the applicable dimensions below and add a regression for every mechanism the change relies on:

- zero, one, and many parameter rows; zero, inline, and server-resident result rows;
- autocommit and explicit transactions, including failure, rollback, and retry;
- complete consumption, early reader close, explicit cancellation, timeout, and server error;
- cache hit, eviction while idle, eviction while the connection is busy, and session close;
- same-session and external DDL, distinguishing “the server kept the plan” from actual
  invalidation;
- Linux, macOS, Windows, and compile-time little-/big-endian branches;
- the ownership boundary between MAPI, Arrow/ADBC, the Python shim, and downstream SQLAlchemy.

Treat test names, documentation claims, support metadata, and release settings as assertions that
must have executable gates. Dependency changes require the released dependency revision, the
driver wheel must be exercised by the downstream dialect before release, and tag builds must
repeat the required matrix against the exact release ref. Performance changes need repeated
before/after measurements of the affected end-to-end workload; isolated codec improvements are
not enough when network or server time dominates.

## Release recovery

The first PyPI release uses the pending Trusted Publisher configured for owner `wlaur`, repository
`adbc-driver-monetdb`, workflow `ci.yml`, and environment `pypi`. PyPI returning 404 before that
first publish is expected: a pending publisher authorizes the initial upload but does not register
or reserve the project name.

Release uploads fail if any filename already exists on PyPI. If publishing succeeds but a later
job fails, rerun only the failed jobs in GitHub Actions; do not rerun the complete workflow or
delete and recreate the tag. This preserves the protected-main provenance check and prevents a
partial or conflicting upload from being reported as successful.
