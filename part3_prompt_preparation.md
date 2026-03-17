# Part 3: Prompt Preparation Document

## Selected PR: PR #190 – Async ConsumerRebalanceListener Support
**Repository:** `aio-libs/aiokafka`
**PR Link:** https://github.com/aio-libs/aiokafka/pull/190

---

## 3.1.1 Repository Context (200–300 words)

aiokafka is a Python library that gives developers an asynchronous interface for working with Apache Kafka — the widely used distributed message streaming platform. The library wraps the lower-level `kafka-python` package and replaces its blocking, thread-based I/O model with Python's native `asyncio` system, making it possible to send and receive Kafka messages within async Python applications without blocking the event loop.

The intended users of this library are Python backend engineers who are building real-time data pipelines, event-driven microservices, or distributed systems that require high-throughput message passing. These users are typically already working in an `asyncio`-based stack — for example, using `aiohttp`, FastAPI, or similar frameworks — and need a Kafka client that fits naturally into that model. They want to `await` Kafka sends and receives just like any other coroutine in their application.

The problem domain this library addresses is the friction that arises when you try to use a synchronous, thread-blocking Kafka client inside an async Python program. Blocking calls inside the event loop cause the whole loop to stall, degrading performance and potentially causing timeouts. aiokafka solves this by implementing the Kafka protocol entirely on top of `asyncio`, so every network operation is non-blocking and cooperative with the event loop.

The repository provides two main classes: `AIOKafkaConsumer` for reading messages from topics (with full consumer group support, offset management, and rebalance coordination), and `AIOKafkaProducer` for writing messages (with batching, compression, and delivery guarantees). It is actively maintained, widely used in production async Python services, and is a member of the `aio-libs` ecosystem — a curated collection of production-grade async Python libraries.

---

## 3.1.2 Pull Request Description (200–300 words)

This PR changes how aiokafka handles callbacks registered with `ConsumerRebalanceListener`. A `ConsumerRebalanceListener` is an object that users can attach to a consumer to receive notifications when Kafka reassigns topic partitions between consumers in a group — a process called "rebalancing." These notifications let applications do cleanup (like committing offsets for the old partition set) before the rebalance finalizes, and setup (like initializing state for new partitions) after it completes.

Before this PR, the two callback methods on the listener — `on_partitions_revoked` and `on_partitions_assigned` — were required to be ordinary synchronous Python functions. This created a significant gap: because aiokafka is an async library, most things you'd want to do in these callbacks (for example, committing offsets via `await consumer.commit()`, flushing to a database, or calling an external service) are async operations. You cannot await inside a plain function. As a result, users either had to skip important cleanup during rebalances or work around it with thread-based hacks, which defeats the purpose of using an async library.

The new behavior introduced by this PR is that the `GroupCoordinator` — the internal component responsible for managing group membership — now checks at callback dispatch time whether each listener method is a coroutine function. If it is, the coordinator awaits it properly inside its async coordination loop. If it is a plain function, it is still called synchronously as before.

This change is backward compatible: existing code that uses synchronous listeners requires no modification. New code can now write fully `async def` rebalance callbacks and use any `await`-able operation inside them. The previous behavior was that async-defined callbacks would be called but their resulting coroutine would not be awaited, silently producing no effect — this is now corrected.

---

## 3.1.3 Acceptance Criteria (Minimum 5 criteria)

✓ When a user registers an `async def on_partitions_revoked(partitions)` callback on a `ConsumerRebalanceListener` and attaches it to an `AIOKafkaConsumer`, the callback must be properly awaited by the `GroupCoordinator` during a rebalance event.

✓ When a user registers an `async def on_partitions_assigned(partitions)` callback, it must be properly awaited after the group rebalance completes and new partitions are assigned.

✓ The implementation should handle plain synchronous callbacks (ordinary `def` functions) exactly as before, with no change in behavior, ensuring full backward compatibility.

✓ When an async callback is awaited and it raises an exception, the exception must propagate correctly and not be silently swallowed — the consumer's error handling behavior for rebalance failures must remain intact.

✓ The implementation should correctly detect whether a callback is a coroutine function using `asyncio.iscoroutinefunction()` (or equivalent), not by duck-typing or try/except, to avoid false positives.

✓ Integration tests must demonstrate that within an async rebalance callback, it is possible to successfully `await consumer.commit()` (or equivalent async operation) — proving that the callback is genuinely running in the async context, not in a synchronous wrapper.

✓ The feature must work correctly under concurrent consumer group scenarios — for example, when multiple consumers in the same group trigger simultaneous rebalances, each consumer's async callback must be independently and correctly awaited.

---

## 3.1.4 Edge Cases (Minimum 3 cases)

**Edge Case 1: Callback raises an exception during rebalance**
If the async `on_partitions_revoked` callback raises an exception (for example, a network timeout when committing offsets to an external store), the `GroupCoordinator` must handle this gracefully. The question is whether the exception should abort the rebalance, be logged and swallowed, or propagate to the consumer's main loop. The implementation must define and test this behavior explicitly — unhandled exceptions in rebalance callbacks have historically caused the consumer to silently stop working.

**Edge Case 2: Callback is a coroutine function but raises immediately without doing any I/O**
A callback like `async def on_partitions_revoked(partitions): raise ValueError("bad partition")` is technically a valid coroutine function. The detection logic using `asyncio.iscoroutinefunction()` will mark it as async, and the coordinator will attempt to `await` it. This edge case verifies that the awaiting mechanism handles both fast-raising and slow-returning coroutines correctly, without hanging or masking the error.

**Edge Case 3: Callback is a lambda or callable object (not a plain function or coroutine)**
Users might pass a callable class instance or a lambda instead of a `def` function. `asyncio.iscoroutinefunction()` returns `False` for lambdas even if they wrap coroutines. This edge case tests whether the detection correctly handles all common Python callable patterns and whether the documentation clearly communicates what types of callables are supported as async callbacks.

**Edge Case 4 (Bonus): Callback takes longer than the rebalance timeout**
Kafka has a `max.poll.interval.ms` timeout — if the consumer does not poll within that window, the broker considers it dead and triggers another rebalance. If an async rebalance callback is slow (e.g., doing a slow DB flush), it could cause the consumer to miss the poll deadline, triggering an unintended cascading rebalance. This scenario should be documented as a usage concern, and ideally tested or warned about in the implementation.

---

## 3.1.5 Initial Prompt (300–500 words)

You are implementing a feature in the `aio-libs/aiokafka` repository — a Python asyncio client for Apache Kafka. Your task is to modify the consumer's rebalance listener system to support **async (coroutine) callbacks** in addition to the existing synchronous callback support.

**Background:**
`AIOKafkaConsumer` allows users to register a `ConsumerRebalanceListener` with two callback methods: `on_partitions_revoked(partitions)` (called before old partitions are removed) and `on_partitions_assigned(partitions)` (called after new partitions are assigned). Currently, these callbacks must be plain synchronous functions. This is a problem because aiokafka is an async library — users who need to perform async operations (like committing offsets with `await consumer.commit()`) during rebalances cannot do so.

**What you need to implement:**
In `aiokafka/consumer/group_coordinator.py`, locate the points where `on_partitions_revoked` and `on_partitions_assigned` are called on the registered listener. Modify these call sites so that:

1. The code checks whether the callback is a coroutine function using `asyncio.iscoroutinefunction(callback)`.
2. If it **is** a coroutine function, the code **awaits** it: `await callback(partitions)`.
3. If it is a **plain function**, the code calls it synchronously as before: `callback(partitions)`.

This change must be fully backward compatible — existing code using synchronous listeners must not require any changes.

**Acceptance criteria to satisfy:**
- Async `on_partitions_revoked` callbacks must be properly awaited during rebalance events.
- Async `on_partitions_assigned` callbacks must be properly awaited after partition assignment.
- Synchronous callbacks must continue to work exactly as before.
- Exceptions raised by async callbacks must propagate correctly and not be silently swallowed.
- `asyncio.iscoroutinefunction()` must be used for detection (not duck-typing or try/except).

**Edge cases to handle:**
- A callback that raises an exception immediately without doing I/O (fast-raising coroutine).
- A callback that is a coroutine function returning `None` (most common case).
- A plain function passed where an async one is expected — must not break.
- Document (in a code comment or docstring) that slow async callbacks could interfere with Kafka's `max.poll.interval.ms` timeout.

**Testing requirements:**
- Add at least one unit test in `tests/test_consumer.py` or `tests/test_coordinator.py` that verifies an async `on_partitions_revoked` callback is awaited during a simulated rebalance.
- Add at least one integration test (using a real or mock Kafka setup) that demonstrates `await consumer.commit()` working successfully inside an async rebalance callback.
- Ensure all existing tests for `ConsumerRebalanceListener` still pass.

**Files most likely to be modified:**
- `aiokafka/consumer/group_coordinator.py` — primary implementation change
- `tests/test_consumer.py` or `tests/test_coordinator.py` — tests
- `docs/consumer.rst` or similar — documentation update
- `CHANGES.rst` — add a changelog entry under the current version

Please write clean, idiomatic Python that follows the existing code style in the repository. Use type hints where the surrounding code uses them, and add a docstring update to `ConsumerRebalanceListener` explaining that its methods now support both sync and async callables.
