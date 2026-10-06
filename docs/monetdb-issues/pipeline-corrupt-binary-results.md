# Pipeline engine returns corrupt binary results

**Status:** reproduces on `master` at `ee305e491c` (56.0.0) with the pipeline engine enabled.
One case is deterministic and reproduces on an idle machine in under a minute; two more appeared
only under sustained load. All three passed on the same build with the engine disabled, and on
11.55.7.

The server does **not** crash. `merovingian.log` records no `SIGSEGV` and no `SIGABRT` across
the whole campaign — only the normal `TERM` at shutdown. A query following a corrupt result
runs fine once the client reconnects.

## Summary

With `GDKdebug` bit 19 set, some queries produce a binary result (`Xexportbin`) that does not
match its own declared schema. Three distinct symptoms, all of them the client refusing to
decode a malformed stream:

| Suite / query | Error |
| --- | --- |
| TPC-DS SF1 `98` | `ArrowInvalid: External error: column has 10064 bytes; expected 5032` |
| TPC-H SF10 `07_volume_shipping` | `DataError: INVALID_DATA: invalid utf-8 encoding in result set` |
| RTABench `0017_top_selling_month_product` | `OperationalError: IO: unexpected end of file` |

The third is a consequence rather than a separate fault: once a result's framing is wrong, the
next read runs off the end of the stream and the connection desynchronizes. In the TPC-H case
this is visible in order — `07_volume_shipping` returns invalid UTF-8, and only then does
`10_returned_items` fail with `IO: unexpected end of file`, followed by
`INVALID_STATE: connection has been closed`.

**This is a silent-wrong-answer class of defect.** All three were caught only because the
corruption happened to be structurally invalid — a byte count that does not match, a string
that is not valid UTF-8. Corruption that still parses would be returned to the application as a
successful query with wrong values, and nothing in a client would notice.

Any binary-protocol client is affected; this is not specific to one driver.

## TPC-DS Q98 — deterministic

Reproduced on demand, on an idle machine, on a freshly populated TPC-DS SF1 database:

```text
ArrowInvalid: External error: column has 10064 bytes; expected 5032
```

Exactly 2×, byte-identical to the same query on an earlier `56.0.0` build (`bbc2d72f02`), so
this has survived the commits between the two. Q98 projects decimal arithmetic that can promote
to 128-bit, so the most likely explanation is a column declared 8 bytes wide being filled with
16-byte values.

Q99 runs normally immediately afterwards, and the suite completes.

### Reproduction

Needs a populated TPC-DS SF1 database. With one, the query alone is enough; run it through a
client that uses the binary result protocol, with the server started as
`mserver5 ... -d524288`. The control is the same query on the same build without `-d524288`,
which passes.

## TPC-H Q07 and RTABench Q17 — load-dependent

Both failed during a full seven-suite campaign, after roughly an hour of continuous load.
Re-running TPC-H SF10 alone on an idle machine, same build and same `gdk_debug=524288`,
`07_volume_shipping` **passed** at 108 ms and the suite completed.

So these two do not reproduce in isolation. A defect that appears under sustained parallel load
and vanishes on an idle machine is the signature of a race, which is consistent with where it
sits: the morsel-driven runtime hands the same MAL fragment to a pool of workers operating on
private stacks over shared output state.

Characterising it properly needs a load harness rather than a single query, which is why this
half is recorded as an observation rather than a reproduction.

## Enabling the pipeline engine

Off by default, behind `GDKdebug` bit 19 (524288), which is not in the documented mask list in
`gdk/gdk.h`:

```sh
docker run ... -e MSERVER5_EXTRA_ARGS=-d524288 <image>
```

At runtime, `SELECT sys.debug(524288);` *sets* the mask and returns the **previous** value, so
it is not a way to read the current state without changing it.

## What would settle it

1. Whether Q98's 2× is a 128-bit decimal promotion filling an 8-byte-declared column — a
   targeted look at the result-schema construction for that plan shape.
2. Whether the load-dependent corruption is the same root cause or a separate race. If the
   framing error in Q98 is a width declaration and the UTF-8 case is a torn write, they are
   different bugs that happen to share a symptom.
3. Whether corruption that *does* parse is also occurring. Every check here is structural; a
   value-level comparison of pipeline-on results against pipeline-off results, over a suite
   with known answers, is the thing that would find it.
