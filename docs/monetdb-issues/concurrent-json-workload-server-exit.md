# mserver5 exits under a concurrent wide-JSON workload

**Status: not filable.** No backtrace and no standalone reproduction. Not re-tested against
`master` at `ee305e491c`, because there is nothing to run: the only known trigger is a
multi-client application workload, not a script.

## What happens

Several reader connections plus a concurrent ADBC writer, against a table with a large `JSON`
column, terminate the server mid-query. Clients see `IO: unexpected end of file`; the
container is gone afterwards.

Observed on MonetDB 11.55.7 with this driver at 0.12.0, three attempts out of three, so it is
deterministic rather than a race that occasionally trips. Earlier notes recorded it as
intermittent and unexplained; neither the server release nor the driver version current at the
time fixed it.

The ingredients appear to be concurrent readers, a concurrent ADBC writer, and a wide `JSON`
column. Building a synthetic case is the next step before reporting.

## Not the same bug as the `sys.sessions` crash

[`sessions-catalog-null-dereference`](sessions-catalog-null-dereference.md) was found while
investigating this crash, in the subsystem originally suspected, but it is **not** the cause
of this one and a report must not claim it is.

The observer connection in the workload that hit this connects as the `monetdb` administrator,
so `admin` is true for it and `their_be->mvc->user_id` is never evaluated. That is not a
guess: 200,000 `sys.sessions` reads as `monetdb` under heavy connection churn produced zero
crashes, while the same workload as a non-admin crashed after 88.

So the original hypothesis — concurrent enumeration of `sys.sessions` during session teardown
— is positively **excluded** here, even though it turned out to be a real bug for other users.

## What was tried against the admin path, without reproducing anything

All on `monetdb/monetdb:latest` (amd64, 11.55.7), `mclient` only, connecting as `monetdb`:

| workload | duration | crashes |
|---|---:|---:|
| 4× temp-table create/insert/drop + 2× `sys.sessions`/`sys.tables` polling + 2× connection churn | 60 s | 0 |
| 4× full scan of `json.text(json.filter(json.filter(json.filter(content,'$.parts'),0),'$.text'))` over a 400 MB `json` column + 1× batched writer + 2× catalogue polling + 2× connection churn | 180 s | 0 |

The second covers the vheap-growth-under-concurrent-scan theory (a large `json` column being
appended to while four sessions scan it), which was the most plausible remaining mechanism. It
did not fire, though the emulated x86-64 container is roughly 5× slower than the original
arm64 run, which widens every window and may equally well hide a narrow one.

## What is still needed

1. A core dump with usable registers. `--ulimit core=-1` does produce a core in the dbfarm on
   the official image, but under Rosetta the kernel writes aarch64 registers into an x86-64
   core and gdb cannot unwind it. Either reproduce on a real amd64 host, or reproduce on the
   arm64 image where native gdb works.
2. Confirmation that it reproduces at all outside the original application. Nothing found so
   far contradicts the possibility that the trigger is something the client does rather than
   something the server does on its own.
