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

The DB-API connection starts with autocommit disabled, as required by PEP 249. Call
`conn.commit()`, use the connection context manager, or pass `autocommit=True` explicitly.

The same connection works directly with pandas 3:

```python
import pandas as pd

with dbapi.connect("monetdb://user:password@localhost:50000/db") as conn:
    df = pd.read_sql("SELECT * FROM trades", conn, dtype_backend="pyarrow")
    df.to_sql("trades_copy", conn, if_exists="append", index=False)
```

## Support policy

- MonetDB **Dec2025 (11.55) and newer**, little-endian servers only
- Python **3.13+** (one abi3 wheel per platform), polars **1.42+**, pandas **3.0+**,
  adbc-driver-manager **1.11+**
- Platforms: Linux x86_64 + aarch64 (manylinux), macOS arm64, Windows x86_64

## ADBC coverage

| Surface | Status |
|---|---|
| Arrow query streams and affected-row counts | Supported |
| Prepared statements, positional binds, and `executemany` | Supported |
| Bulk ingest: create, append, replace, create-append, schema, temporary tables | Supported |
| Transactions and autocommit | Supported |
| `GetInfo`, `GetObjects`, `GetTableSchema`, `GetTableTypes`, `ExecuteSchema` | Supported |
| Query cancellation | Not supported by the current Rust MAPI transport |
| Partitioned results | Not supported; MAPI exposes one sequential result channel |
| Substrait plans | Not supported by MonetDB |
| `GetStatistics` | Not implemented |

The reusable ADBC validation suite currently passes 242 tests and three subtests against
Dec2025-SP3. Its six skips cover four cross-catalog operations, statistics, and
negative-scale decimals that MonetDB itself does not support.

## Repository layout

| Path | |
|---|---|
| `crates/adbc-monetdb` | the ADBC driver (cdylib exporting `AdbcDriverMonetdbInit`) |
| `crates/monetdb-arrow` | MonetDB binary wire format ⇄ Arrow conversion |
| `monetdb-rust` | [our fork](https://github.com/wlaur/monetdb-rust) of [MonetDB/monetdb-rust](https://github.com/MonetDB/monetdb-rust) (git submodule, MPL-2.0) — MAPI protocol layer |
| `adbc_driver_monetdb` | Python shim over `adbc-driver-manager`; ships the cdylib as `adbc_driver_monetdb._native` |

## Development

```sh
git clone --recurse-submodules https://github.com/wlaur/adbc-driver-monetdb
uv sync                                # installs deps + builds the extension via maturin
uv run pytest -m "not integration"     # python tests (no server needed)
cargo test --workspace                 # rust tests

# integration tests against a real server:
docker run -d --platform linux/amd64 -p 50000:50000 \
    -e MDB_DB_ADMIN_PASS=monetdb -e MDB_CREATE_DBS=test \
    monetdb/monetdb:Dec2025-SP3@sha256:a71e6e8c8402beadc51aebf944b465ee5b185c7ae4a9e6808b5d9133ee921786
MONETDB_TEST_URI=monetdb://monetdb:monetdb@localhost:50000/test uv run pytest -m integration

# run the reusable ADBC conformance suite:
MONETDB_TEST_URI=monetdb://monetdb:monetdb@localhost:50000/test \
    uv run pytest tests/validation
```

Lint/typecheck: `uv run ruff check .`, `uv run ruff format --check .`, `uv run pyright`,
`cargo clippy --workspace --all-targets`, `cargo fmt --all --check`.

## License

MIT. The `monetdb` protocol crate (`monetdb-rust`, our fork of MonetDB/monetdb-rust) is MPL-2.0;
its license and corresponding-source notice are included in wheels and source distributions.
