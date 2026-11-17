# UDF Target Architecture Blueprint

> Status: accepted architectural principles; implementation plan narrowed by repository review (2026-11-17).
> Scope: udf-frame as a reusable Rust framework; Xray is the first substantial integration/validation consumer.
> Compatibility: evolutionary migration. This document describes responsibilities and contracts; it is not an instruction to rename crates or rearrange the workspace now.

## 1. Product goal

UDF should make common application infrastructure safe, composable, observable, and easy to adopt. It is not intended to replace Tokio, the OS scheduler, allocators, databases, or application-specific architecture.

The product promise is **sensible defaults + opt-in capabilities + explicit resource/lifecycle semantics**. "Zero learning curve" is an aspiration for the happy path, not a literal guarantee: advanced users must still be able to configure execution, resource budgets, and failure policies.

### Core principles (retained)

1. Pay only for capabilities enabled/used.
2. Keep synchronous and asynchronous applications first-class.
3. Keep core contracts independent from concrete runtimes and I/O.
4. Make cancellation, shutdown, backpressure, and resource limits explicit.
5. Prefer safe Rust; isolate and document any unavoidable unsafe implementation, and require correctness evidence rather than treating comments as proof.
6. Never claim a performance win without reproducible benchmarks and correctness evidence.
7. Keep protocol/application hot paths outside the generic state-management runtime unless profiling proves otherwise.

These principles are accepted constraints. The decisions below narrow the implementation path; they do not weaken or replace the principles.

## 2. Architectural boundaries and current crate ownership

The architecture is defined first by **logical responsibility and dependency direction**, not by the number or names of Cargo crates.

    Application / Domain (Xray · Web · DB · CLI · Embedded)
       ├── Pure state semantics (Action · State · Fragment · Reducer · Effect · Store contract)
       ├── Synchronous execution (SyncStore · dispatch · sync task execution)
       ├── Async edges/adapters (Tokio tasks · async I/O · cancellation integration)
       └── Shared infrastructure (pools · budgets · journal · observability · diagnostics)
    
    All paths share runtime-neutral contracts; sync and async execution remain distinct.

### Current package mapping

| Logical responsibility | Current home / decision |
|---|---|
| Runtime-neutral base contracts, including Clock | udf-skeleton (skeleton). Keep the existing public contract home; ADR-0001 deliberately placed Clock here. |
| Standard utilities (cache, retry, scheduler, rate limit, IDs) | udf-std-ext (std-ext), depending on the small, zero-third-party udf-skeleton contract crate. |
| Store, effects, scopes, runtime integration and infrastructure | Existing udf-skeleton-infra modules, with internal logical boundaries kept explicit. |
| Tokio integration | Existing feature-gated runtime integration. Tokio must remain absent from sync-only dependency graphs. |
| Xray protocol, sockets, relay/data plane and concrete buffer allocator | Xray consumer repository; not moved into UDF. |

Do not create a second Clock abstraction or a parallel contracts crate. In particular, udf-contracts in an earlier diagram was a proposed logical responsibility, **not an approved new crate**. For the current workspace, skeleton remains the canonical home for its existing runtime-neutral contracts. If future evidence justifies a crate split, move the contracts in one coordinated migration and preserve one canonical API/home.

### Existing ADRs and relationship to this blueprint

The repository intentionally excludes `docs/adr/` from Git under the 2026-10-28 documentation policy (the repository keeps consumer-facing documentation in-tree, while decision/research archives remain local). This is a governance constraint, not evidence that the decisions do not exist. The following ADRs have direct architectural overlap with this blueprint and are inherited here:

| ADR | Decision relevant to this blueprint | Relationship |
|---|---|---|
| ADR-0001 | Clock / ConfigPort contract ownership belongs in `skeleton`. | **Accepted and preserved.** Keep one canonical contract home; do not introduce a duplicate Clock or parallel contracts crate. |
| ADR-0008 | Notification surface supports two coexisting tiers: sync and async. | **Accepted and inherited.** This is the existing precedent for two-tier coexistence; §11 evaluates extending the same model to Effect execution without replacing either mode. |
| ADR-0009 | Fragment / Reducer are distinct composition contracts: Reducer is the root contract; Fragment is the slice contract. Combining fragments as a root should fail at compile time (E0277), not at runtime by panic. | **Accepted and inherited.** The pure-state contract inventory includes both Fragment and Reducer; examples and trait bounds must preserve their root/slice distinction. |
| ADR-0010 | ObjectPool uses bounded degradation when the accounted pool universe is full: it may directly build an object outside that universe, does not count it in `alive`, and releases it rather than returning it to the pool. | **Accepted and inherited.** §5 explicitly distinguishes the accounted pool cap from the fallback production policy; `max` is not necessarily an absolute cap on objects returned to callers. |

ADR-0002–0007 are not cited as decision premises for the target-architecture choices in this blueprint. However, **ADR-0004 and ADR-0007 remain the authoritative basis for the current package layout described above** (the std-ext split and zero-third-party dependency posture); they are retained as current-state rationale, not re-decided here. ADR-0002, -0003, -0005, and -0006 are not cited as decision premises and have no material overlap with the decisions made in this blueprint. Their authoritative local copies remain governed by the existing `docs/adr/` exclusion policy. If a future blueprint change materially depends on one of them, summarize and cite that decision explicitly before proposing the change.

This arrangement preserves the established policy that ADR source files are not committed while making the four decisions that materially constrain this architecture visible to Git-based collaborators. It does not claim that a short index replaces the authoritative ADR records.

### Crate-splitting rule

Keep the current crate names, paths, and public APIs during the present implementation steps. Do not perform a workspace-wide rename, directory move, or create a facade/adapters/testing/resource crate merely to match a diagram.

Extract a new Cargo crate only when at least one concrete condition is demonstrated:

- an independent consumer needs to depend on that capability without pulling unrelated code;
- the dependency graph requires a boundary that cannot be enforced adequately with current module/feature boundaries;
- independent versioning, publication, ownership, or compilation isolation provides a measured benefit;
- a second structurally different consumer validates that the proposed public boundary is genuinely reusable.

A logical module boundary is useful even when it remains in the same Cargo package. The target tree is therefore intentionally not prescribed here.

## 3. Synchronous and asynchronous model

Sync and async are two first-class execution modes. They share semantics and runtime-neutral contracts, but they do not need identical APIs or one universal executor abstraction.

    SYNC: caller thread -> sync dispatch / SyncStore -> pure synchronous reducer -> sync effects / bounded sync executor
    ASYNC: Tokio task -> async ingress / bounded channel -> single-writer state owner -> async effects / Tokio tasks
    HYBRID: sync core / reducer / state -> async edge adapter -> async I/O / Tokio
            async completion -> explicit message/dispatch boundary -> sync core

Rules:

- Reducers remain synchronous, deterministic, and side-effect free in every topology. Async I/O belongs at Effects/adapters or application edges.
- A synchronous consumer must not pull Tokio into its dependency tree.
- An async consumer may use the Tokio adapter; UDF must not create a second general-purpose async scheduler.
- Do not hide blocking work inside async executor workers. Route it to an explicitly bounded blocking executor.
- Do not force async traits into the core solely to make APIs look uniform. Share semantics and value types; let adapters expose idiomatic sync/async APIs.
- In a sync-core/async-edge topology, the core must never block waiting for async completion. Async work reports completion through an explicit message/dispatch boundary.
- Queue bounds, overload behavior, ordering guarantees, and shutdown semantics must be documented for each execution path.

## 4. Lifecycle and cancellation model

    Running -> Closing (close gate / reject new work)
            -> Cancelling (signal cooperative cancellation)
            -> Draining (wait for owned work until deadline)
            -> Terminated (all owned work completed)
            -> TimedOut (deadline reached / report survivors)
    TimedOut -> Terminated (ownership released; survivors reported, possibly completing later)

Keep these operations distinct:

- close_gate: stop accepting new work; does not imply cancellation.
- cancel: signal cooperative cancellation to owned work and descendants.
- drain(deadline): wait for tracked work and collect outcomes.
- abort: runtime-specific best-effort task abort; not a universal guarantee for blocking threads.
- shutdown(deadline): orchestrate the above and return a structured report.

The semantic distinctions are invariants:

- close is not cancel;
- a cancellation signal is not proof that work stopped;
- timeout means the caller stopped waiting, not that work terminated;
- aborting an async task is not equivalent to terminating a synchronous thread.

The repository's CancelToken is a UDF-owned watch-based implementation (fx.rs:78-135), not tokio-util's CancellationToken. Parent-child propagation is a target capability, not an instruction to add tokio-util implicitly. The implementation choice is pending an explicit dependency decision: either extend the UDF-owned token with parent/child semantics, or evaluate tokio-util and accept its dependency only after a documented review of dependency cost, feature isolation, and behavior. Until that decision is made, do not add a per-child watcher task topology as an assumed requirement. Track this as deferred work in §11. The task registry remains separate: cancellation signals do not prove that work has stopped.

### Current gap and scope

- FxScope already distinguishes close() from cancellation and has shutdown/timeout/abort operations. Preserve those semantics with regression tests.
- Its current ShutdownReport is too small to distinguish actual termination from the caller timing out. Extend the report so at minimum it distinguishes still_running from timed_out, and represents completed/cancelled/panicked/failed outcomes without conflating them.
- A synchronous ThreadPool::drain(deadline) is deferred until a real sync consumer needs that contract. Do not add a speculative API without a consumer.
- Xray-specific ConnScope/ArmScope ownership, watcher-task elimination, and inbound lifecycle wiring belong in xray-core-rust, not this repository.

## 5. Resource model: admission, observation, and pool policy

Separate enforcement from observation:

- **Admission authority**: the synchronized counter, budget, or permit that decides whether a resource may be created/acquired. It must have a clear linearization point.
- **Metrics**: borrowed/idle/high-water/denied counters for observation. Metrics must not be the sole hard-limit authority.
- **Pool backend**: allocation/reuse strategy owned by the consumer or adapter.
- **Pool policy**: idle retention, eviction, size classes, and overflow behavior.

A hard cap on the **accounted pool universe** must use an atomic reservation/permit protocol or a lock-protected check-and-act operation. It must not use an unsynchronized check-then-act sequence.

### Three independent resource-accounting axes

The ObjectPool resource contract is not a single global memory cap and not merely a two-axis model. It has three independent axes with **different covered populations**:

| Axis | What it bounds / covers | Admission or accounting point | What is excluded / full-capacity behavior |
|---|---|---|---|
| **Alive / object-count axis (`max`)** | The accounted pool universe: normal objects that are idle **plus** normal objects currently borrowed. | `claim_growth` checks the per-shard `share(max, shards, idx)` and increments `shard.alive` under that shard's lock. Returning/pruning/dropping normal objects decrements the universe count as appropriate. | It does not include degraded direct-build objects. A full universe selects the degradation path; `borrowed / max` may therefore exceed 1.0 because degraded borrows are intentionally outside the universe. |
| **Idle-resident byte axis (`ByteBudget`)** | Only bytes retained by idle objects in the pool (or the aggregate idle population across pools sharing the same `Arc<ByteBudget>`). It is **not** a cap on all memory held by borrowed objects or by the process. | `weight(&T)` is measured and `ByteBudget::try_take` atomically claims bytes when an object is admitted to an idle slot (return/insert). **Pool destruction releases every idle-resident object's bytes back to the budget** (`impl Drop`; a shared budget must not stay permanently charged after a pool dies). | Borrowing an object gives its bytes back to the budget; borrowed objects consume alive capacity but **do not consume ByteBudget**. Over-budget return/insert is discarded rather than retained. |

Two load-bearing assumptions for the byte axis:
- **`weight(&T)` must be a pure function of the retained object.** Pool destruction recomputes each idle object's weight to release exactly what was claimed; a non-pure hook (returning different values over time) can make the released sum differ from the claimed sum, which surfaces as an accounting underrun (`give_back` fail-fast) rather than silent drift.
- **Destruction releases bytes even when a shard lock is poisoned** (`PoisonError::into_inner`): dropping a pool is best-effort accounting, and panicking during `Drop` would both skip the release and risk process abort during unwind. This is the one deliberate relaxation of the "fail-fast on accounting bugs" rule — data is already untrustworthy at that point, and refusing to release is strictly worse.
| **Degradation axis (bounded direct build)** | Objects constructed after the accounted alive universe is full. These objects are outside **both** the alive accounting and the idle-byte budget. | `pop_or_build` path ④ increments `degraded` and `constructed`, then directly calls the factory without `claim_growth` or `ByteBudget::try_take`. | The object is not pooled; return releases/destroys it and never inserts it into idle. Degradation can make the number of objects handed to callers exceed `max`; the idle-byte budget does not constrain these transient degraded objects either. |

Under ADR-0010, this bounded degradation is intentional, not an accounting bug. `max` is a hard bound on the **accounted alive universe**, not on every object temporarily handed to callers. Likewise, `ByteBudget::total` is a hard bound on **accounted idle-resident bytes**, not on borrowed-object memory or total process memory. These two hard bounds have different scopes.

Every pool configuration and API contract must declare all applicable axes separately:
- the population covered by the object-count cap (idle + normal borrowed);
- the population covered by the byte budget (idle resident objects only);
- the behavior when either limit is reached (including direct-build degradation, discard-on-return, and whether the object bypasses one or both ledgers).

Do not silently infer one axis from another. Expose degraded construction/release separately from normal admission where metrics are useful.

### Current implementation findings

The current ObjectPool implementation's concrete accounting points and coverage are:

| Mechanism / path | Linearization or accounting point | Coverage and consequence |
|---|---|---|
| `ObjectPool::claim_growth` | Under each shard lock, compare `shard.alive` with `share(max, shards, idx)`, then increment `alive`. | Counts normal idle + normal borrowed objects. Aggregate normal-object universe is bounded by `max` (subject to documented resize convergence semantics). |
| `ByteBudget::try_take` on idle admission | Single-word atomic CAS loop on `held`: claim only if `held + n <= total`, then `held += n`; called while admitting an object into an idle slot. | Counts idle-resident bytes only. Borrowing/prune/resize trimming returns the corresponding bytes; borrowed objects are outside this budget. |
| `pop_or_build` path ④ | When `claim_growth` returns `None`, increment `degraded` and `constructed`, then invoke the factory directly. | Bypasses both alive and ByteBudget accounting. The returned object is released on return and never becomes idle. |
| `PoolBase::note_borrow` / metrics | Updates observation counters; it does not reserve admission capacity. | Observability only; not a hard-limit authority and must not be treated as a third admission ledger. |

The review of the current repository did not find the alleged check-then-act race in the identified admission paths:
- MemoryPool::acquire checks and increments inner.created under the same mutex.
- ObjectPool::claim_growth checks and increments shard.alive under the shard lock; share() maintains the aggregate bound.
- ObjectPool byte quota uses ByteBudget::try_take with an atomic compare-and-swap check-and-act operation (no mutex; the old `Mutex<usize>` ledger was replaced under bd udf-frame-d5i because it cost ~2 µs/round-trip at 4 threads).

Therefore, do not introduce a generic ResourcePermit subsystem merely to fix a race that is not present here. ByteBudget already provides the idle-resident-byte reservation primitive. The remaining local documentation task is to preserve these exact coverage semantics when documenting other pools; do not describe ByteBudget as a total-memory budget.

If a corresponding race exists in Xray's concrete buffer pool, investigate and fix it in the Xray consumer repository, where that admission path lives.

Avoid per-acquire heap allocations in lease/drop paths unless benchmarks justify them. Prefer concrete lease types or enum dispatch over boxed FnOnce closures in known hot paths, while retaining generic extension points for uncommon backends.





## 6. Effect execution and backpressure

Effects should declare intent/policy rather than implicitly choosing an executor:

- Immediate: execute inline, bounded work only.
- Async: runtime-native async operation.
- Blocking: bounded blocking executor.
- CpuBound: optional CPU pool adapter.
- Deferred/Batch: explicit queue and flush policy.

Every queued execution path needs a capacity and overflow policy: wait, reject, drop-newest, drop-oldest, or caller-runs where semantically safe. Cancellation, deadline, retry budget, and shutdown drain behavior must be explicit.

Do not add a new executor abstraction that silently moves arbitrary work between sync and async contexts. The sync and async edges may have different mechanics while preserving shared lifecycle, failure, and backpressure semantics.

## 7. Error model

Infrastructure APIs must make failure classes and recovery semantics explicit. Avoid collapsing operational failures into panics or a single opaque error.

| Error class | Examples | Expected handling | Constructor-time invariant / policy |
|---|---|---|---|
| Configuration / construction | invalid limits, incompatible options, invalid weight/budget pairing | Unconditionally invalid constructor arguments are programmer errors: fail fast before publishing state. Environment-dependent failures discovered at runtime return typed errors. | **恒非法参数 = panic fail-fast**; do not bulk-convert established constructors to Result. This preserves the repository's current discipline (MemoryPool::new, ThreadPool::new, PoolBase::new, ByteBudget::new, ObjectPool construction). |
| Admission / overload | queue full, budget exhausted, task rejected | Return an observable typed rejection; do not silently drop work. Retry only under an explicit caller policy/deadline. | Runtime condition; may recover when capacity becomes available. |
| Execution | effect failure, worker task failure, I/O failure | Preserve operation context and surface the failure to its owner; isolation policy determines whether sibling work continues. | Operation-dependent; propagate or isolate according to declared policy. |
| Cancellation | cooperative cancellation requested | Represent as cancellation, not success and not an unexpected runtime failure. | Expected runtime control flow. |
| Timeout | deadline elapsed while waiting | Report timeout and any still-running work separately; never imply termination. | Runtime condition; timeout does not prove termination. |
| Runtime / infrastructure | OS thread spawn failure, executor unavailable, channel closed | Return a typed operational error; keep internal accounting consistent and the runtime usable where possible. | Environment/runtime failure; typed error, not constructor-argument panic. |
| Panic / invariant violation | task panic, internal state invariant broken | Isolate task panics where promised; invariant corruption must not be disguised as ordinary rejection or silently recovered. | Internal invariant violations remain fail-fast where continuing is unsound. |

Rules:

- Prefer typed errors for runtime-recoverable failures at public boundaries; preserve source errors and relevant operation/context information. This does not convert constructor-time, unconditionally invalid programmer arguments into Result: those remain fail-fast panics under the repository's established discipline.
- A rejection is a result visible to the submitter, not an invisible metric increment.
- Error reporting must distinguish the operation failing from the runtime itself becoming unavailable.
- Structured shutdown reports summarize lifecycle outcomes; they do not replace the original operation errors.
- Panics are not a substitute for ordinary error propagation. Reserve panic for violated internal invariants where continuing would be unsound.

## 8. Source organization and optional integrations

The earlier multi-crate tree (core/contracts/runtime/executors/resource/adapters/testing/facade) is withdrawn as an immediate construction plan. Its names may still be useful as **logical module labels**, but they do not authorize new packages or a workspace migration.

For now:

- preserve skeleton, std-ext, and skeleton-infra as the existing package boundaries;
- enforce dependency direction through module design, Cargo features, and CI checks;
- keep Tokio optional and absent from sync-only dependency graphs;
- keep test utilities and fault injection out of production dependency paths;
- add adapters only when an actual integration consumer needs them.

A facade/prelude may be considered after crate boundaries are stable and a second structurally different consumer demonstrates the ergonomic need. Profiles such as Balanced, LowLatency, HighThroughput, and BoundedResource are deferred until there is a facade and real consumer evidence. They are policy bundles, not universal optimizers, and should not become public API in advance of demonstrated use cases.

Cargo features are compile-time capability selection, not runtime dynamic plugin loading. If runtime loading is ever needed, it must be a separate plugin mechanism with an explicit security and ABI story.

## 9. Safety, correctness, and performance evidence

Before optimizing a primitive:

- define its linearization points and lifecycle invariants;
- add deterministic tests for races and failure paths;
- run stress tests and, where applicable, Loom/Miri;
- compare against a safe reference for the specific unsafe primitive or invariant being optimized;
- benchmark single-thread and contended workloads;
- report metrics that are meaningful for the workload and platform.

### Unsafe implementation evidence

A local SAFETY comment is necessary documentation, not proof of correctness. The evidence must exercise the invariant that makes the unsafe operation sound.

For the current DrainExclusive<T> unsafe concentration point:

- keep each unsafe block's local safety explanation, but make it explicit who establishes and maintains the external drain-exclusion invariant;
- build a debug/feature-gated Mutex-backed reference for DrainExclusive itself, not a second full implementation of SyncStore;
- add differential tests for observable dispatch behavior;
- add deterministic invariant tests covering concurrent dispatch with snapshot clone/drop and subscription register/unregister, proving no aliasing of effect_buf / notify_cache;
- test panic/unwind paths to ensure the drain flag remains consistent;
- run Miri only on the pure-std subset it can execute; record the exact test subset and result. A Loom model of the drain-flag exclusion may be added if its value justifies the dependency and maintenance cost.

The evidence deliverable must be explicit and owned by a tracked task. It must not be deferred to "whoever next touches the code." A full safe rewrite of SyncStore is not required to establish evidence for this one unsafe abstraction.

### Performance practice

Do not commit machine-specific benchmark numbers as universal baselines or impose unstable cross-platform thresholds. The repository's scheduled performance workflow uses rolling baselines and published artifacts/trends; keep that practice.

Keep separate gates for:

1. correctness and deterministic failure-path tests;
2. stress/concurrency and lifecycle/resource conformance;
3. benchmark compilation in CI;
4. scheduled performance tracking with environment-aware comparisons.

The concrete benchmark gap identified in review is the pool/permit dimension: acquire/release, saturation/rejection, and contention. Add it after the relevant correctness path is stable. A lifecycle/resource conformance suite must follow the runtime-neutral contract definition; it must not encode unspecified semantics. Collect p50/p95/p99, allocations, queue depth, RSS, or context-switch data only where the workload and platform make those measurements meaningful.

No lock-free queue, work-stealing scheduler, sharded JoinSet, or allocator rewrite should be accepted without a benchmark demonstrating a material improvement on representative workloads and correctness evidence for the associated invariants.

## 10. Xray integration boundary

Xray owns protocol semantics, socket I/O, buffer allocator choice, and proxy data-plane optimization. UDF owns reusable lifecycle, state-flow, generic execution contracts, and resource primitives that have demonstrated more than one use.

- Xray's network read/write/relay path stays on Tokio and protocol-native code.
- Xray's xray-buf remains the concrete buffer backend; UDF does not replace it with a speculative generic allocator.
- Xray should consume UDF through a narrow integration boundary rather than importing implementation internals throughout xray-core.
- Instance/Framework lifecycle ownership should converge on one coordinator and one structured shutdown report in the consumer repository.
- Dynamic inbound removal must release its scope registry ownership in the consumer repository.
- Do not route packet/read/write hot paths through generic Action/Reducer/Effect dispatch.

## 11. Migration and implementation order

This is an evidence-driven sequence within the **existing workspace layout**. It is not a crate-renaming schedule.

1. **P0 — ThreadPool spawn failure correctness**: make spawn fallible, define the worker-accounting publication point, propagate submit rejection/resize errors, and inject deterministic spawn failures. The live-worker count must match actual workers; failed creation must never leave join waiting for a phantom worker.
2. **P0 — DrainExclusive unsafe evidence**: produce the bounded Mutex-reference differential harness, aliasing/exclusion invariant tests, panic/unwind tests, and recorded Miri subset result. SAFETY comments alone do not close this task.
3. **P1 — Lifecycle report contract**: extend ShutdownReport to distinguish timed-out waiting from still-running work and represent terminal outcomes; pin existing close/cancel semantics with idempotency tests.
4. **P1 — Remaining error taxonomy**: align the remaining public runtime-recoverable errors and rejection/reporting paths with §7. Thread-creation typed errors and submit/resize propagation are delivered in P0; do not duplicate that work or convert constructor-time invariant violations from panic to Result.
5. **P1 — Architecture regression checks**: keep the sync-only dependency graph free of Tokio with a CI cargo tree assertion; preserve existing feature isolation and public Store contract.
6. **P1 — Effect execution two-tier parity**: carry forward ADR-0008's already implemented two-tier sync/async notification model as an explicit precedent, then evaluate and document how the same two-tier coexistence applies to effect execution. Do not force one mode to replace the other; preserve the existing notification contract while defining the effect-side boundary and tests.
7. **Deferred — CancelToken parent/child design**: make and record an explicit dependency decision between extending the UDF-owned watch-based CancelToken and adopting tokio-util. No implicit tokio-util dependency or watcher-per-child implementation before that decision.
8. **P2 — Resource semantics documentation**: document the exact universe bounded by each pool maximum. Investigate any check-then-act race in Xray's buffer pool in the Xray repository, not by adding an unused UDF subsystem.
9. **P2 — Performance coverage**: add pool acquire/release, saturation/rejection, and contention benchmarks after the corresponding correctness changes. Add lifecycle/resource conformance tests only after their contracts are specified.
10. **Deferred — consumer-driven extraction**: facade, profiles, independent adapter/testing/resource crates, and workspace renames wait for stable boundaries and a second structurally different consumer. Existing quickstarts and tutorials should be retained; do not rewrite them merely to match a proposed future facade. The architecture owner/maintainer must review this deferred set at each release cut (and at least once every 90 days), using concrete signals: a second structurally different consumer, dependency-graph pressure, repeated integration duplication, or a demonstrated publication/versioning need. Record the review outcome in #25 or a linked decision issue; keep the item explicitly deferred with the next review date if no trigger has occurred.
11. **Consumer-repository work**: Xray ConnScope/ArmScope, watcher task topology, buffer-pool admission, and protocol/data-plane integration are tracked and implemented in xray-core-rust.

## 12. Non-goals

- Replacing Tokio or implementing a new general-purpose async runtime.
- Forcing every operation through Action/Reducer/Effect.
- Turning every network packet/read/write into a UDF dispatch.
- Claiming zero allocation or zero contention for all workloads.
- One executor/pool policy that is optimal for every application.
- Creating Cargo crates to mirror conceptual boxes before a real dependency/consumer boundary exists.
- Runtime dynamic loading via Cargo features.
- Shipping policy profiles without a concrete consumer and measured need.

## 13. Acceptance criteria: current state versus remaining gap

| Criterion | Current status | Required action |
|---|---|---|
| Sync-only consumer has no Tokio in its dependency tree | **Satisfied and pinned** for the reviewed graphs | CI step *Sync-only dependency tree (no Tokio)* asserts three graphs (`udf-skeleton-infra` default, `udf-skeleton-infra --no-default-features --features runtime`, `udf-std-ext`), fail-closed on both tokio detection and `cargo tree` failure; reverse control (`--features tokio`, `--all-features`) verified to fail. Extend the assertion list when a new sync-only graph appears. |
| Core Store contract can be implemented independently | **Satisfied but unpinned**: public skeleton::Store contract; SyncStore is an implementation | Preserve the public contract and avoid coupling it to runtime internals. |
| Optional Tokio integration is feature-gated | **Satisfied but unpinned** in the reviewed dependency graph | Keep the regression check. |
| Existing quickstart/tutorial material exists | **Satisfied but unpinned**: quickstart, driver tour, end-to-end example, and tutorial | Keep compiling; re-home only if a future facade is justified. |
| Close and cancellation are distinct | **Satisfied and pinned** | `tests/fx_lifecycle.rs` pins close-gate not cancelling in-flight work, repeated `close`/`cancel`/`shutdown` idempotency, and bucket exhaustion. |
| Shutdown report distinguishes timeout from actual termination | **Satisfied and pinned** | `ShutdownReport` carries six buckets (`completed`/`cancelled`/`panicked`/`failed`+`errors`/`timed_out`/`still_running`), with `timed_out` sampled at the deadline and `still_running` after the reaping pass. Pinned by `tests/fx_lifecycle.rs` + `tests/conformance_lifecycle.rs`; both fail in ~0.06 s under mutation instead of hanging. |
| Thread creation failure cannot corrupt worker accounting or hang join | **Satisfied and pinned** | Accounting publication point moved behind successful spawn with rollback; `ThreadSpawnError` + `RejectCause::SpawnFailed` + `spawn_failures` ledger; `resize` returns `PoolResizeError::SpawnFailed{kind,target_core,workers}` (partial-effect state in the error). Pinned by 6 injection tests in `tests/thread_pool.rs` (30 total) plus bounded waits with metrics snapshots. |
| DrainExclusive unsafe invariant has executable evidence | **Satisfied and pinned** | `tests/unsafe_evidence.rs` (6 tests: concurrent dispatch × snapshot × subscription interleaving, reducer/subscriber panic, reentrancy, production-vs-Mutex differential over 8 observable surfaces); mutation verification (bypassing drain-flag mutual exclusion fails 3 tests in ~1 s, surfacing the real aliasing write); Miri on the pure-std single-threaded subset (no claim over concurrency). `unsafe-evidence` is off by default and default-build zero cost is proved at symbol level (`MutexExclusive` absent from the default rlib). |
| Resource maxima have documented semantics | **Satisfied** | `docs/reference.md` records, per pool, the covered population, full-capacity behaviour, legal over-subscription states and metric read semantics (7 surfaces including the three axes of `ObjectPool`). Verified by an external probe crate asserting every documented claim (`ALL DOC CLAIMS HOLD`). No generic permit layer introduced. |
| Pool admission/permit performance is measured | **Satisfied** | `benches/infra.rs` gained pool-dimension groups (ObjectPool acquire/drop, MemoryPool idle-hit vs empty-create, ThreadPool saturation rejection, shared `ByteBudget` 2×2 contention). Data only, no thresholds — per §9. The shared `ByteBudget` mutex ledger measured ~2.15 µs/round-trip at 4 threads; bd udf-frame-d5i replaced it with a single-word atomic CAS ledger (**958-1024 ns at 4 threads across 3 runs, 89-91 ns single-thread**, was 103-105 ns), i.e. ~2.1×. Residual ~10× vs single-thread is *not* attributable to ledger contention (isolated probes: 16-107 ns per contended pair; a sharded-mutex ledger measured 696 ns / 105 ns single-thread), so the next step is attribution inside `ObjectPool`'s borrow/return path — see `verify/2026-11-17-p2-bytebudget-contention/`. |
| Lifecycle/resource conformance semantics are specified | **Satisfied** (sync-side drain still deferred) | `tests/conformance_lifecycle.rs` encodes the six-bucket lifecycle semantics and the three resource axes as cross-implementation assertions. `ThreadPool::drain(deadline)` remains deferred pending a real sync consumer. |
| Facade and policy profiles are needed by multiple consumers | **Not yet demonstrated** | Defer until a second structurally different consumer provides evidence. |
| A second non-proxy application passes conformance | **Future validation target** | Use this as a trigger for extracting reusable crates and broadening public API, not as a reason to pre-build them. |

The acceptance criteria describe observable properties. They do not imply that every property requires a new crate or a new abstraction.
