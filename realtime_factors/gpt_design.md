# realtime_factors Low-Latency Design (GPT)

## 1. Goals

- Update time-series (TS) factor state per symbol on every order/trade event.
- Compute cross-sectional (CS) factors continuously with very low latency.
- Let downstream Python processes read the latest ready snapshot without knowing anything about MVCC/version chains.
- Expose shared-memory layouts that are friendly to NumPy first, and optionally Arrow/Polars.
- Guarantee that readers never observe partial or overwritten snapshots.
- Periodically checkpoint factor snapshots to disk for replay and post-close analysis.

## 2. Core Decisions

1. Python must not trigger CS computation on demand. A dedicated CS worker must run continuously.
2. TS and CS state must be decoupled.
3. The CS worker must not read the mutable live TS table directly while computing a snapshot.
4. Published CS snapshots must be immutable after publication.
5. Snapshot correctness must be guaranteed on the producer/publisher side, not delegated to Python readers.
6. Slow readers may reduce freshness, but must never cause torn reads or slot overwrite.
7. If no publish slot is available, the system may skip intermediate publications, but must keep the newest completed snapshot pending.

## 3. High-Level Architecture

```text
KungFu journal (order/trade)
    -> Normalizer
    -> OrderStateStore
    -> TSState[symbol]
    -> BaseRow[symbol] (model-facing TS outputs)
    -> DeltaQueue (full-row deltas, in order)
    -> CrossSectionWorker
         -> CS local base matrix
         -> CS factor computation
         -> private scratch snapshot
         -> publish to N-slot shared-memory snapshot ring
              -> Python model readers
              -> CheckpointWriter
              -> Optional research readers
```

The most important design choice is the `DeltaQueue` plus `CS local base matrix`:

- The event engine owns the mutable live TS state.
- The CS worker owns a private, single-writer, logically consistent copy of the base factor table.
- The CS worker never scans the live TS table directly.

This removes the need for a full input snapshot ring on the TS side.

## 4. State Model

### 4.1 Event Engine State

- `OrderStateStore[(channel_id, order_id)]`
- `TSState[symbol]`
- `BaseRow[symbol]`

`BaseRow` is the boundary between TS and CS.

Suggested `BaseRow` properties:

- fixed symbol universe index
- fixed feature schema
- fixed-width numeric storage
- no Python objects
- no variable-length strings in the hot path

Suggested example:

```text
BaseRow[symbol_idx] = {
    symbol: int32,
    valid: uint8,
    ts_seq: uint64,
    asof_event_nano: uint64,
    features[F_base]: float32/float64
}
```

### 4.2 DeltaQueue

Every time a symbol's `BaseRow` changes, the event engine emits a delta to the CS worker.

For V1, the simplest and most cache-friendly choice is:

- send the full updated row, not a sparse patch
- fixed-width record
- ordered single-consumer queue

Example:

```text
BaseDelta = {
    global_seq: uint64,
    symbol_idx: uint32,
    asof_event_nano: uint64,
    valid: uint8,
    features[F_base]: float32/float64
}
```

Why full-row deltas are a good V1 choice:

- simple semantics
- no pointer chasing
- deterministic replay
- no need for the CS worker to touch live shared TS memory

### 4.3 CrossSectionWorker State

The CS worker keeps:

- `CSBaseMatrix[n_symbol][F_base]`
- `CSScratchMatrix[n_symbol][F_out]`
- optional incremental operator state for fast CS factors
- latest consumed `global_seq`

This local state is internally consistent because it is updated by one consumer in strict delta order.

## 5. Computation Flow

### 5.1 TS Update Path

For each order/trade event:

1. normalize SH/SZ semantics
2. update order/trade-related live state
3. update the affected symbol's `TSState`
4. materialize the symbol's latest `BaseRow`
5. emit one `BaseDelta` to the `DeltaQueue`

This path is fully event-driven per symbol.

### 5.2 CS Worker Loop

The CS worker runs continuously.

Suggested loop:

```text
loop forever:
    drain DeltaQueue up to latest available seq
    coalesce repeated updates for the same symbol if needed
    apply deltas to CSBaseMatrix
    compute CS factors from CSBaseMatrix into private scratch
    try_publish(private scratch)
```

Two practical notes:

1. The worker should compute from its private matrix, not from the live TS store.
2. The worker may coalesce multiple symbol updates before one CS recompute if backlog is non-zero.

This gives a natural `latest-wins` semantic:

- freshness stays high
- intermediate unpublished generations may be skipped
- correctness is preserved

## 6. How CS Factors Are Stored

Do not store CS output as a mutable per-symbol live table.

Instead:

- compute a complete CS snapshot in private scratch memory
- publish it as one immutable snapshot version
- let readers pin and read that immutable version

This is the key distinction:

- TS output is live and symbol-local
- CS output is snapshot-based and whole-market consistent

## 7. Published Snapshot Ring

### 7.1 Why AB Is Not Enough

AB buffering is unsafe if a reader can hold the previous ready version for too long.

Example:

- reader A still holds slot A
- writer publishes slot B
- writer wants to publish again
- if slot A is reused too early, reader A sees corruption

Therefore, use `N >= 4` slots, not just A/B.

### 7.2 Slot Semantics

Each slot is immutable after publication.

Slot states:

- `FREE`
- `WRITING`
- `READY`

Metadata per slot:

```text
SlotMeta = {
    generation: uint64,
    state: uint32,
    pin_count: uint32,
    input_seq_begin: uint64,
    input_seq_end: uint64,
    asof_event_nano: uint64,
    publish_nano: uint64,
    n_symbols: uint32,
    n_features: uint32,
    schema_hash: uint64
}
```

Control block:

```text
RingControl = {
    active_slot: uint32,
    active_generation: uint64,
    latest_completed_generation: uint64,
    skipped_publish_count: uint64,
    slot_meta[N],
    reader_handle_table[MAX_READERS]
}
```

### 7.3 Important Nuance: Seqlock vs Immutable Slots

For full-market CS snapshots, the critical safety mechanism is **not** a classic per-row seqlock.

The safer model is:

- immutable slot payload after publish
- generation/version check during acquire
- pin/unpin while reading

Seqlock/version check is still useful for control metadata, but the payload itself should not be mutable after publish.

## 8. No-Overwrite Guarantee

This directly answers the main concern.

If the CS worker computes very fast, it may finish more snapshots than Python can read. The system must guarantee that a reader-held slot is never overwritten.

The rule must be:

- a published slot can be reused only when `pin_count == 0`

This guarantee belongs on the producer/publisher side.

Python readers should cooperate by releasing quickly, but correctness must not depend on Python being fast.

### 8.1 What Happens If All Slots Are Busy

The publisher must not overwrite any in-use slot.

Instead:

1. keep the newest finished snapshot in a private `pending_latest` scratch buffer
2. increment a metric such as `skipped_publish_count`
3. continue computing newer snapshots if needed
4. as soon as one slot becomes reusable, publish only the newest pending snapshot

This yields the correct trade-off for trading systems:

- never corrupt a reader
- never block the main event path
- prefer newest data over preserving every intermediate generation

### 8.2 Consequence

If readers are too slow:

- they do not break correctness
- they reduce publication freshness
- some intermediate CS generations are dropped

This is acceptable for a `latest snapshot` service.

## 9. Reader Pinning and Reclamation

### 9.1 Reader API Contract

Readers must acquire and release snapshots explicitly.

Python should use:

```python
with client.acquire_latest(view="model", block=False) as snap:
    X = snap.matrix_numpy()
    symbols = snap.symbols_numpy()
    # compute signal fast
```

### 9.2 Why a Tiny Native Python Helper Is Needed

Pin/unpin and generation-checked acquire should not be implemented in pure Python against raw shared memory.

Provide a thin native helper (for example via pybind11 or a small C extension) that does:

- atomic acquire
- pin increment/decrement
- generation verification
- optional lease refresh

The Python strategy code should only see a high-level handle.

### 9.3 Long Reads

If a reader needs to hold data for a long time, it should:

1. acquire latest snapshot
2. copy out to private NumPy memory
3. release snapshot immediately
4. run long model logic on the private copy

So the policy is:

- short read: zero-copy shared memory
- long hold: explicit copy-out

### 9.4 Crashed Reader Cleanup

If a reader crashes without releasing, a fixed ring can leak slots forever unless cleanup exists.

Use a `reader_handle_table` with leases:

```text
ReaderHandle = {
    active: uint8,
    pid: uint32,
    slot: uint32,
    generation: uint64,
    lease_expire_nano: uint64
}
```

The publisher or a small control thread periodically reaps stale readers by:

- checking expired lease
- optionally checking whether `pid` is still alive
- decrementing the slot pin count if the handle is stale

## 10. Snapshot Acquire Protocol

Suggested logic:

1. read `active_generation` and `active_slot`
2. verify the slot is `READY` and the slot generation matches
3. allocate a reader handle
4. increment `pin_count`
5. re-check `active_generation` and slot generation
6. if unchanged, acquire succeeds
7. otherwise, undo pin and retry

This gives optimistic, low-latency acquire with ABA protection via generation numbers.

## 11. Snapshot Publish Protocol

Suggested logic:

1. choose a reusable slot (`READY` or `FREE`, but `pin_count == 0`, and not the current active slot)
2. if none exists, keep result in `pending_latest` and return
3. mark slot `WRITING`
4. copy snapshot payload into the slot
5. write metadata
6. release-store slot state as `READY`
7. atomically publish `active_slot` and `active_generation`

After step 7, the payload is immutable until reused later.

## 12. Memory Layout for Python

### 12.1 Model View (Hot Path)

For trading models, the default published format should be NumPy-friendly.

Use one contiguous dense matrix per slot:

```text
SnapshotSlot payload (model_view):
    header
    symbol[n_symbol]            int32
    valid[n_symbol]             uint8
    feature_matrix[n_symbol][F_out]   float32 (row-major)
```

Why this is good:

- zero-copy `numpy.ndarray((n_symbol, F_out), dtype=float32)`
- ideal for linear algebra and model inference
- easy to keep small and fast

### 12.2 Research View (Optional)

If zero-copy Polars/Arrow is required, publish a second optional view:

```text
SnapshotSlot payload (research_view):
    header
    symbol[n_symbol]            int32
    factor_0[n_symbol]          float32/float64
    factor_1[n_symbol]          float32/float64
    ...
```

This is a columnar layout.

Recommendation:

- `model_view`: always-on, highest frequency
- `research_view`: optional, same or lower frequency depending on cost budget

## 13. Why Python Must Read Ready Snapshots, Not Trigger Compute

If Python triggers CS computation synchronously:

- strategy latency becomes compute latency
- multiple readers compete for compute resources
- consistency and scheduling become messy

Instead:

- the CS worker computes continuously
- Python only acquires the latest ready snapshot

This keeps the strategy-side contract simple:

- "give me the latest ready consistent factor snapshot"

## 14. Adaptive Compute Policy

The CS worker should not blindly recompute after every single delta if that would reduce throughput.

Use an adaptive policy:

- if backlog is tiny, compute immediately
- if backlog is non-zero, coalesce a small burst of deltas first
- if compute is slower than updates, always chase the latest state

This is still event-driven, but avoids wasting CPU on snapshots that will never be published.

## 15. Periodic Checkpoint to Disk

Checkpointing must reuse the same published snapshot abstraction.

Recommended design:

- every 1 minute or 10 minutes, `CheckpointWriter` acquires the latest ready snapshot
- write it asynchronously to Parquet
- release the snapshot immediately after the write buffer is materialized

Benefits:

- on-disk files have exactly the same consistency semantics as Python readers
- no extra snapshot mechanism is needed
- post-close replay is simple

Suggested metadata columns:

- `trading_day`
- `generation`
- `input_seq_end`
- `asof_event_nano`
- `publish_nano`
- `schema_hash`

## 16. Recommended Initial Parameters

- `N = 8` slots for `model_view`
- `MAX_READERS = 16`
- `float32` for model-facing features unless precision requires `float64`
- one dedicated CS worker process first
- one checkpoint writer thread/process

If only one or two readers exist, `N = 4` may already work, but `N = 8` is a safer V1 default.

## 17. Metrics and Backpressure

Track at least:

- `delta_queue_depth`
- `delta_queue_lag_us`
- `cs_compute_us`
- `publish_copy_us`
- `publish_skipped_count`
- `slot_busy_count`
- `max_reader_hold_us`
- `stale_reader_reap_count`

These metrics tell you whether the latency bottleneck is:

- TS update
- CS compute
- publish copy
- slow Python readers

## 18. Final Answer to the Main Concern

Question:

- what if the CS process computes very fast and finishes a whole ring before Python finishes reading?

Answer:

- this must be prevented on the publisher side
- never overwrite a slot with `pin_count > 0`
- if all slots are pinned or otherwise unavailable, skip intermediate publications and keep only the latest pending snapshot

So the responsibility split is:

- producer/publisher side: hard correctness guarantee
- Python reader side: fast release for better freshness

Correctness must not depend on Python being fast.

## 19. Recommended V1 Boundary

V1 should implement:

- event-driven TS updates per symbol
- full-row delta queue from TS to CS
- one dedicated CS worker with private base matrix
- one immutable N-slot snapshot ring for `model_view`
- Python acquire/release client helper
- minute-level or 10-minute Parquet checkpoint

V1 should avoid:

- exposing version chains to Python
- letting Python trigger CS compute
- reusing A/B slots without pin tracking
- relying on pure-Python atomics against shared memory
