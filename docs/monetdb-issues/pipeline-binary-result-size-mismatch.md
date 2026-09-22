# Pipeline engine: binary result buffer disagrees with the declared schema

**Status:** observed on `56.0.0` built from `master` at `bbc2d72f02`, with the pipeline engine
enabled. **Not re-tested** at `ee305e491c`: reproducing it needs a populated TPC-DS database,
which is the expensive part rather than the interesting part.

## Summary

TPC-DS Q98, run with the pipeline engine enabled, fails on the client with

```text
ArrowInvalid: External error: column has 10064 bytes; expected 5032
```

Exactly 2×. The server's binary result buffer disagrees with its own declared schema. Q98
projects decimal arithmetic that can promote to 128-bit, so the most likely explanation is a
column declared 8 bytes wide being filled with 16-byte values.

The same query passes on 11.55.7, and on the same `56.0.0` build with the pipeline engine
disabled.

Any binary-protocol client hits this; it is not specific to this driver.

## Enabling the pipeline engine

Off by default, behind `GDKdebug` bit 19 (524288), which is not in the documented mask list in
`gdk/gdk.h`:

```sh
docker run ... -e MSERVER5_EXTRA_ARGS=-d524288 <image>
```

At runtime: `SELECT sys.debug(524288);` — this *sets* and returns the **previous** value.
`sys.debug(0)` disables it. Use `sys.debugflags()` to read without modifying.

## What a re-test needs

1. A TPC-DS SF1 database.
2. Q98 executed through a client that uses the binary result protocol.
3. The same query with the pipeline disabled, as the control.

Worth doing at the same time as any other pipeline-engine verification, since the setup cost
is the same.
