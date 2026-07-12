# adbc-driver-monetdb

[ADBC](https://arrow.apache.org/adbc/) driver for [MonetDB](https://www.monetdb.org/), written in Rust.

Arrow-native reads and writes for MonetDB: polars, pandas, and every other ADBC consumer get
columnar result sets (MonetDB's binary result-set protocol decoded directly into Arrow record
batches) and bulk ingestion (`COPY BINARY ... ON CLIENT` streamed from Arrow buffers) through one
standard interface. The implementation plan lives in [docs/PLAN.md](docs/PLAN.md).

## Usage

```python
import polars as pl
from adbc_driver_monetdb import dbapi

with dbapi.connect("monetdb://user:password@localhost:50000/db") as conn:
    df = pl.read_database("SELECT * FROM trades", conn)
    df.write_database("trades_copy", conn, if_table_exists="append", engine="adbc")

# or resolved from the URI scheme:
df = pl.read_database_uri("SELECT 1", "monetdb://localhost:50000/db", engine="adbc")
```

## Support policy

- MonetDB **Dec2025 (11.55) and newer**, little-endian servers only
- Python **3.13+** (one abi3 wheel per platform), current-stable polars / adbc-driver-manager
- Platforms: Linux x86_64 + aarch64 (manylinux), macOS arm64, Windows x86_64

## Repository layout

| Path | |
|---|---|
| `crates/adbc-monetdb` | the ADBC driver (cdylib exporting `AdbcDriverMonetdbInit`) |
| `crates/monetdb-arrow` | MonetDB binary wire format ⇄ Arrow conversion |
| `vendor/monetdb-rust` | fork of [MonetDB/monetdb-rust](https://github.com/MonetDB/monetdb-rust) (git submodule, MPL-2.0) — MAPI protocol layer |
| `python/adbc_driver_monetdb` | Python shim over `adbc-driver-manager`; ships the cdylib as `adbc_driver_monetdb._native` |

## Development

```sh
git clone --recurse-submodules https://github.com/wlaur/adbc-driver-monetdb
uv sync                                # installs deps + builds the extension via maturin
uv run pytest -m "not integration"     # python tests (no server needed)
cargo test --workspace                 # rust tests

# integration tests against a real server:
docker run -d -p 50000:50000 -e MDB_DB_ADMIN_PASS=monetdb -e MDB_CREATE_DBS=test \
    monetdb/monetdb:Dec2025-SP3
MONETDB_TEST_URI=monetdb://monetdb:monetdb@localhost:50000/test uv run pytest -m integration
```

Lint/typecheck: `uv run ruff check .`, `uv run ruff format --check .`, `uv run pyright`,
`cargo clippy --workspace --all-targets`, `cargo fmt --all --check`.

## License

MIT. The vendored `monetdb` protocol crate (`vendor/monetdb-rust`) is MPL-2.0.
