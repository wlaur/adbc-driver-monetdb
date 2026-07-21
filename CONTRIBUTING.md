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

Set `MONETDB_TEST_URI=monetdb://monetdb:monetdb@localhost:50000/test` for integration tests.
Protocol-generic MAPI changes belong in the `monetdb-rust` submodule; Arrow- or ADBC-specific
changes belong in this repository.

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

## Release recovery

Release uploads fail if any filename already exists on PyPI. If publishing succeeds but a later
job fails, rerun only the failed jobs in GitHub Actions; do not rerun the complete workflow or
delete and recreate the tag. This preserves the protected-main provenance check and prevents a
partial or conflicting upload from being reported as successful.
