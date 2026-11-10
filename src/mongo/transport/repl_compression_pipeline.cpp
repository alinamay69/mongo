// Copyright (c) MongoDB, Inc.
// SPDX-License-Identifier: SSPL-1.0

#include "mongo/transport/repl_compression_pipeline.h"

#include "mongo/base/counter.h"
#include "mongo/base/init.h"  // IWYU pragma: keep
#include "mongo/base/initializer.h"
#include "mongo/db/commands/server_status/server_status_metric.h"
#include "mongo/logv2/log.h"
#include "mongo/transport/message_compressor_manager.h"
#include "mongo/transport/repl_compression_pipeline_gen.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/processinfo.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/timer.h"

#include <algorithm>
#include <atomic>
#include <cstring>
#include <memory>

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kNetwork

namespace mongo::transport {
namespace {

MONGO_FAIL_POINT_DEFINE(replPipelineCompressionThrows);

// Used when internalReplPipelineCompressionMaxPendingBytes is 0.
constexpr long long kDefaultMaxPendingBytes = 256LL * 1024 * 1024;

// Bytes reserved by all pipelines, checked against the soft cap.
AtomicWord<long long> gGlobalPendingBytes{0};

// serverStatus metrics.network.replCompressionPipeline.*. All are monotonic counters except
// rawQueueDepth.

auto& gBatchesSubmitted =
    *MetricBuilder<Counter64>("network.replCompressionPipeline.batchesSubmitted");

// Batches handed to the connection thread for sending.
auto& gBatchesSent = *MetricBuilder<Counter64>("network.replCompressionPipeline.batchesSent");

// Batches submitted but not yet sent or dropped, across all sessions.
auto& gRawQueueDepth = *MetricBuilder<Counter64>("network.replCompressionPipeline.rawQueueDepth");

// rawBytes includes batches that were later dropped; compressedBytes counts successful ones only.
auto& gRawBytes = *MetricBuilder<Counter64>("network.replCompressionPipeline.rawBytes");
auto& gCompressedBytes =
    *MetricBuilder<Counter64>("network.replCompressionPipeline.compressedBytes");

// Total and count of compression time in microseconds; mean = total / count.
auto& gCompressLatencyMicrosTotal =
    *MetricBuilder<Counter64>("network.replCompressionPipeline.compressLatencyMicrosTotal");
auto& gCompressLatencyCount =
    *MetricBuilder<Counter64>("network.replCompressionPipeline.compressLatencyCount");

// Time the sender spent waiting in takeNextReady(). High values mean the pool can't keep up.
auto& gSenderBlockedMicrosTotal =
    *MetricBuilder<Counter64>("network.replCompressionPipeline.senderBlockedMicrosTotal");

// Failed batches seen by the sender. Each one ends the session.
auto& gCompressFailures =
    *MetricBuilder<Counter64>("network.replCompressionPipeline.compressFailures");

// awaitDataWakes: wake hook calls, on completion or on registerActiveOpCtx().
// awaitDataWakeMissedNoTarget: head batch finished while no getMore was registered.
auto& gAwaitDataWakes = *MetricBuilder<Counter64>("network.replCompressionPipeline.awaitDataWakes");
auto& gAwaitDataWakeMissedNoTarget =
    *MetricBuilder<Counter64>("network.replCompressionPipeline.awaitDataWakeMissedNoTarget");

// Installed by the db layer at startup; null in unit tests.
std::atomic<ReplPipelineAwaitDataWakeFn> gAwaitDataWakeFn{nullptr};  // NOLINT
std::atomic<ReplPipelineArmOpCtxFn> gArmOpCtxFn{nullptr};            // NOLINT

long long resolveMaxPendingBytes() {
    auto v = gReplPipelineCompressionMaxPendingBytes.load();
    return v > 0 ? v : kDefaultMaxPendingBytes;
}

// Touch every metric at startup so it is registered on the main thread before the metric tree is
// frozen. Otherwise the subtree is missing from serverStatus until the pipeline is first used.
MONGO_INITIALIZER(RegisterReplCompressionPipelineMetrics)(InitializerContext*) {
    (void)gBatchesSubmitted;
    (void)gBatchesSent;
    (void)gRawQueueDepth;
    (void)gRawBytes;
    (void)gCompressedBytes;
    (void)gCompressLatencyMicrosTotal;
    (void)gCompressLatencyCount;
    (void)gSenderBlockedMicrosTotal;
    (void)gCompressFailures;
    (void)gAwaitDataWakes;
    (void)gAwaitDataWakeMissedNoTarget;
}

}  // namespace

void setReplPipelineAwaitDataWakeFn(ReplPipelineAwaitDataWakeFn fn) {
    gAwaitDataWakeFn.store(fn, std::memory_order_release);
}

void setReplPipelineArmOpCtxFn(ReplPipelineArmOpCtxFn fn) {
    gArmOpCtxFn.store(fn, std::memory_order_release);
}

// ---------------------------------------------------------------------------
// ReplCompressionThreadPool
// ---------------------------------------------------------------------------

ReplCompressionThreadPool& ReplCompressionThreadPool::get() {
    static StaticImmortal<ReplCompressionThreadPool> pool;
    return *pool;
}

size_t ReplCompressionThreadPool::_resolvePoolSize() {
    int configured = gReplPipelineCompressionThreadPoolSize.load();
    if (configured > 0) {
        return static_cast<size_t>(configured);
    }
    // auto: max(1, min(4, numCores/2))
    unsigned cores = ProcessInfo::getNumAvailableCores();
    size_t n = std::max<size_t>(1, std::min<size_t>(4, cores / 2));
    return n;
}

void ReplCompressionThreadPool::ensureStarted() {
    std::lock_guard<std::mutex> lk(_mutex);
    if (_shutdown.load() || _pool) {
        // Already started, or shut down for good.
        return;
    }
    const size_t desired = _resolvePoolSize();
    ThreadPool::Options options;
    options.poolName = "ReplComprPool";
    options.threadNamePrefix = "ReplCompr-";
    options.minThreads = 0;
    options.maxThreads = desired;
    _pool = std::make_unique<ThreadPool>(options);
    _pool->startup();
}

void ReplCompressionThreadPool::schedule(unique_function<void(Status)> task) {
    // The task may be reachable from two places (the pool queue and the inline fallback below);
    // `claimed` guarantees it runs exactly once.
    struct SharedTask {
        std::atomic<bool> claimed{false};  // NOLINT
        unique_function<void(Status)> fn;
    };
    auto shared = std::make_shared<SharedTask>();
    shared->fn = std::move(task);
    auto runOnce = [shared](Status status) {
        if (!shared->claimed.exchange(true)) {
            auto fn = std::move(shared->fn);
            fn(std::move(status));
        }
    };

    std::shared_ptr<ThreadPool> pool;
    {
        std::lock_guard<std::mutex> lk(_mutex);
        if (!_shutdown.load()) {
            invariant(_pool);
            pool = _pool;
            ++_activeSchedulers;
        }
    }
    if (!pool) {
        runOnce(
            Status(ErrorCodes::ShutdownInProgress, "Replication compression pool is shut down"));
        return;
    }
    ON_BLOCK_EXIT([&] {
        std::lock_guard<std::mutex> lk(_mutex);
        --_activeSchedulers;
        _cvShutdown.notify_all();
    });

    pool->schedule(runOnce);
    // If the first worker could not be created, claim the task inline. shutdown() also waits for
    // this path before joining, since an inline task is not covered by ThreadPool::join().
    if (pool->getStats().numThreads == 0) {
        runOnce(Status::OK());
    }
}

void ReplCompressionThreadPool::shutdown() {
    std::unique_lock<std::mutex> lk(_mutex);
    if (_shutdown.load()) {
        _cvShutdown.wait(lk, [&] { return _shutdownComplete; });
        return;
    }
    _shutdown.store(true);
    auto pool = _pool;
    lk.unlock();
    if (pool) {
        pool->shutdown();
    }
    lk.lock();
    _cvShutdown.wait(lk, [&] { return _activeSchedulers == 0; });
    lk.unlock();
    if (pool) {
        pool->join();
    }
    lk.lock();
    _pool.reset();
    _shutdownComplete = true;
    _cvShutdown.notify_all();
}

// ---------------------------------------------------------------------------
// ReplyCompressionPipeline
// ---------------------------------------------------------------------------

ReplyCompressionPipeline::~ReplyCompressionPipeline() {
    shutdown();
}

size_t ReplyCompressionPipeline::effectiveQueueDepth() {
    if (!gReplPipelineCompressionEnabled.load() || ReplCompressionThreadPool::get().isShutdown()) {
        return 0;
    }
    int depth = gReplPipelineCompressionQueueDepth.load();
    if (depth <= 0) {
        return 0;
    }
    // Over the cap, new responses are compressed inline. This is a soft cap: the check and the
    // reservation in trySubmit() aren't atomic, so concurrent submits can overshoot it.
    if (gGlobalPendingBytes.load() >= resolveMaxPendingBytes()) {
        return 0;
    }
    return static_cast<size_t>(depth);
}

long long ReplyCompressionPipeline::globalPendingBytes() {
    return gGlobalPendingBytes.load();
}

bool ReplyCompressionPipeline::trySubmit(Message& raw,
                                         MessageCompressorId compressorId,
                                         boost::optional<Message> trafficRecorderRequest) {
    auto self = shared_from_this();
    const size_t rawSize = static_cast<size_t>(raw.size());

    std::unique_lock<std::mutex> lk(_mutex);

    const size_t depth = effectiveQueueDepth();
    if (_shutdown || depth == 0 || _batches.size() >= depth) {
        return false;
    }

    const uint64_t seq = _nextSeq++;
    auto batch = std::make_shared<ReplPipelineBatch>();
    batch->seq = seq;
    batch->compressorId = compressorId;
    batch->rawSize = rawSize;
    batch->raw = std::move(raw);
    batch->trafficRecorderRequest = std::move(trafficRecorderRequest);
    _batches.emplace(seq, batch);

    // The task owns this reservation until _onCompressed() swaps it or the task releases it.
    gGlobalPendingBytes.fetchAndAdd(static_cast<long long>(rawSize));

    gBatchesSubmitted.increment();
    gRawBytes.increment(static_cast<long long>(rawSize));
    gRawQueueDepth.increment();

    lk.unlock();

    // The worker only gets a weak_ptr and copies; it never touches the session or opCtx.
    std::weak_ptr<ReplyCompressionPipeline> weak = self;
    Message rawForWorker = batch->raw;
    MessageCompressorId cid = compressorId;
    ReplCompressionThreadPool::get().schedule(
        [weak, seq, rawForWorker, cid, rawSize](Status dispatchStatus) mutable {
            // Nobody will consume the result: skip compression, drop raw, then release.
            bool cancelled;
            {
                auto pipeline = weak.lock();
                cancelled = !pipeline || pipeline->isShutdown();
            }
            if (cancelled) {
                rawForWorker = Message{};
                gGlobalPendingBytes.fetchAndSubtract(static_cast<long long>(rawSize));
                return;
            }

            Message compressed;
            Status status = Status::OK();
            size_t wireSize = 0;

            if (!dispatchStatus.isOK()) {
                // Pool is shut down. Still publish a result so the sender doesn't wait forever.
                status = dispatchStatus;
            } else {
                try {
                    MessageCompressorManager mgr;
                    Timer compressTimer;
                    replPipelineCompressionThrows.execute([](const BSONObj&) {
                        uasserted(ErrorCodes::InternalError,
                                  "Injected replication pipeline compression failure");
                    });
                    auto swm = mgr.compressMessage(rawForWorker, &cid);
                    gCompressLatencyMicrosTotal.increment(compressTimer.micros());
                    gCompressLatencyCount.increment();
                    if (swm.isOK()) {
                        compressed = std::move(swm.getValue());
                        wireSize = static_cast<size_t>(compressed.size());
                        // Compact the worst-case compression allocation before queueing the result.
                        if (compressed.operation() == dbCompressed &&
                            compressed.capacity() > static_cast<size_t>(compressed.size())) {
                            auto compact = SharedBuffer::allocate(wireSize);
                            std::memcpy(compact.get(), compressed.buf(), wireSize);
                            compressed = Message(std::move(compact));
                        }
                    } else {
                        status = swm.getStatus();
                    }
                } catch (const DBException& ex) {
                    status = ex.toStatus();
                    compressed = Message{};
                    wireSize = 0;
                }
            }
            rawForWorker = Message{};

            if (auto pipeline = weak.lock()) {
                pipeline->_onCompressed(seq, std::move(compressed), status, wireSize, rawSize);
            } else {
                // Session is gone.
                compressed = Message{};
                gGlobalPendingBytes.fetchAndSubtract(static_cast<long long>(rawSize));
            }
        });

    return true;
}

void ReplyCompressionPipeline::_onCompressed(
    uint64_t seq, Message compressed, Status status, size_t wireSize, size_t rawSize) {
    {
        std::lock_guard<std::mutex> lk(_mutex);
        auto it = _batches.find(seq);
        if (it == _batches.end() || _shutdown) {
            // Dropped by shutdown(), which leaves this task's reservation for us to release.
            compressed = Message{};
            gGlobalPendingBytes.fetchAndSubtract(static_cast<long long>(rawSize));
            return;
        }
        auto& batch = it->second;
        batch->compressed = std::move(compressed);
        batch->status = status;
        batch->compressedSize = wireSize;
        batch->compressedDone = true;

        // Swap the reservation to the result's capacity and drop raw. A passthrough result shares
        // raw's buffer, which stays alive through `compressed`.
        batch->accountedBytes =
            status.isOK() ? std::max<size_t>(batch->compressed.capacity(), wireSize) : 0;
        const long long delta =
            static_cast<long long>(batch->accountedBytes) - static_cast<long long>(batch->rawSize);
        gGlobalPendingBytes.fetchAndAdd(delta);
        batch->raw = Message{};

        if (status.isOK()) {
            gCompressedBytes.increment(static_cast<long long>(wireSize));
        }

        _cvReady.notify_all();

        // The head batch is done and its getMore may be parked in awaitData, so wake it. Called
        // under _mutex so clearActiveOpCtx() can't retire the opCtx during the call; the hook
        // never calls back into the pipeline.
        if (!_shutdown && seq == _nextSeqToSend && !_wakeSignalled &&
            !ReplCompressionThreadPool::get().isShutdown() &&
            gReplPipelineCompressionWakeAwaitData.load()) {
            if (_activeOpCtx) {
                if (auto fn = gAwaitDataWakeFn.load(std::memory_order_acquire)) {
                    _wakeSignalled = true;
                    gAwaitDataWakes.increment();
                    fn(_activeOpCtx);
                }
            } else {
                // No getMore registered yet; registerActiveOpCtx() will fire the wake.
                gAwaitDataWakeMissedNoTarget.increment();
            }
        }
    }
}

bool ReplyCompressionPipeline::_extractReadyBatchLocked(
    std::unique_lock<std::mutex>& lk, Message* out, boost::optional<Message>* outRecorderRequest) {
    auto it = _batches.find(_nextSeqToSend);
    invariant(it != _batches.end());
    auto batch = it->second;

    // Release before a possible throw below.
    invariant(batch->compressedDone);
    const long long freed = static_cast<long long>(batch->accountedBytes);
    gGlobalPendingBytes.fetchAndSubtract(freed);

    _batches.erase(it);
    ++_nextSeqToSend;
    gRawQueueDepth.decrement();

    // Allow the next head batch to wake again.
    _wakeSignalled = false;

    Status status = batch->status;
    Message compressed = std::move(batch->compressed);
    boost::optional<Message> recorderReq = std::move(batch->trafficRecorderRequest);

    lk.unlock();

    // Compression failed; the caller tears down the session.
    if (!status.isOK()) {
        gCompressFailures.increment();
        uassertStatusOK(status);
    }

    gBatchesSent.increment();

    *out = std::move(compressed);
    if (outRecorderRequest) {
        *outRecorderRequest = std::move(recorderReq);
    }
    return true;
}

bool ReplyCompressionPipeline::takeNextReady(Message* out,
                                             boost::optional<Message>* outRecorderRequest) {
    std::unique_lock<std::mutex> lk(_mutex);

    // Only count time when we actually have to wait.
    auto headReady = [&] {
        if (_shutdown) {
            return true;
        }
        auto it = _batches.find(_nextSeqToSend);
        return it != _batches.end() && it->second->compressedDone;
    };
    if (!headReady()) {
        Timer blockTimer;
        _cvReady.wait(lk, headReady);
        gSenderBlockedMicrosTotal.increment(blockTimer.micros());
    }

    if (_shutdown) {
        return false;
    }

    return _extractReadyBatchLocked(lk, out, outRecorderRequest);
}

bool ReplyCompressionPipeline::tryTakeNextReady(Message* out,
                                                boost::optional<Message>* outRecorderRequest) {
    std::unique_lock<std::mutex> lk(_mutex);
    if (_shutdown) {
        return false;
    }
    auto it = _batches.find(_nextSeqToSend);
    if (it == _batches.end() || !it->second->compressedDone) {
        return false;
    }
    return _extractReadyBatchLocked(lk, out, outRecorderRequest);
}

size_t ReplyCompressionPipeline::inFlight() const {
    std::lock_guard<std::mutex> lk(_mutex);
    return _batches.size();
}

bool ReplyCompressionPipeline::hasPending() const {
    std::lock_guard<std::mutex> lk(_mutex);
    return !_batches.empty();
}

void ReplyCompressionPipeline::shutdown() {
    std::lock_guard<std::mutex> lk(_mutex);
    if (_shutdown) {
        return;
    }
    _shutdown = true;
    // Release only batches that finished compressing. Unfinished tasks still hold raw and release
    // their own reservation.
    long long freed = 0;
    for (auto& [seq, batch] : _batches) {
        if (batch->compressedDone) {
            freed += static_cast<long long>(batch->accountedBytes);
        }
    }
    if (freed > 0) {
        gGlobalPendingBytes.fetchAndSubtract(freed);
    }
    if (!_batches.empty()) {
        gRawQueueDepth.decrement(static_cast<uint64_t>(_batches.size()));
    }
    _batches.clear();
    _cvReady.notify_all();
}

bool ReplyCompressionPipeline::isShutdown() const {
    std::lock_guard<std::mutex> lk(_mutex);
    return _shutdown;
}

void ReplyCompressionPipeline::registerActiveOpCtx(void* opCtxTag) {
    // Arm before the getMore runs so its awaitData wait installs a waker.
    if (opCtxTag) {
        if (auto arm = gArmOpCtxFn.load(std::memory_order_acquire)) {
            arm(opCtxTag);
        }
    }
    std::lock_guard<std::mutex> lk(_mutex);
    _activeOpCtx = opCtxTag;
    _wakeSignalled = false;

    // The head batch may have finished before this getMore registered; wake it now.
    if (!_shutdown && opCtxTag && !ReplCompressionThreadPool::get().isShutdown() &&
        gReplPipelineCompressionWakeAwaitData.load()) {
        auto it = _batches.find(_nextSeqToSend);
        const bool headReady = it != _batches.end() && it->second->compressedDone;
        if (headReady) {
            if (auto fn = gAwaitDataWakeFn.load(std::memory_order_acquire)) {
                _wakeSignalled = true;
                gAwaitDataWakes.increment();
                fn(opCtxTag);
            }
        }
    }
}

void ReplyCompressionPipeline::clearActiveOpCtx() {
    std::lock_guard<std::mutex> lk(_mutex);
    _activeOpCtx = nullptr;
}

}  // namespace mongo::transport
