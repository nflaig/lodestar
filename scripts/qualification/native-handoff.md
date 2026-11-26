# Native payload handoff qualification

`native-handoff.mjs` exercises the compiled builder's production payload parser,
session validation, signer, asynchronous journal and actual journal worker. Each
run persists 324 distinct test revisions in temporary directories, abruptly stops
the worker after acknowledgement, then recovers and compares complete
execution-payload SSZ, execution requests, value, ordered transactions, sidecars,
signed bid roots, signatures and ledger records. Original in-memory snapshots must remain
readable after the worker messages. The worker wrapper adds timings around the
real implementation; it does not implement an alternate retention protocol.

Build `@lodestar/builder` and its dependencies first, then run:

```sh
node scripts/qualification/native-handoff.mjs /path/to/lodestar /path/to/captures /tmp/handoff.json /path/to/scratch
```

The capture directory must contain `0-changed.json`, `12-changed.json` and
`21-changed.json`, complete `zbuilder_getPayloadV2` responses from isolated native
fixtures. zbuilder's `test-native-control` optional HTTP benchmark can produce each
file using `NATIVE_CONTROL_HTTP_BENCH=0`, `12` or `21`, and
`NATIVE_CONTROL_HTTP_CAPTURE=/path/to/captures`. Do not query an active production
constructor for this qualification: offering a payload can alter grant ownership.

These are transport and persistence tests. They use a public fixture signing key
and synthetic revision hashes, and do not prove EVM execution or network reveal
acceptance. SSZ timings measure components, not an implemented binary control API.
Timings from a workstation do not establish production latency or win rate.
The optional scratch parent defaults to the operating system temp directory;
choose the intended filesystem explicitly for storage measurements. The output
records its filesystem type. Missing worker write/fsync timings fail the run,
and synchronous main-thread retention time is measured separately. Abrupt worker
termination proves recovery does not rely on a graceful shutdown flush; it does
not simulate a machine power failure or a live selected-payload reveal.
