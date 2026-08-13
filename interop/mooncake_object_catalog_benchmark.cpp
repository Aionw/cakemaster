#include "master_service.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdint>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "types.h"

DEFINE_uint64(num_objects, 100000, "Number of completed memory objects");
DEFINE_double(evict_ratio_target, 0.50, "BatchEvict target eviction ratio");
DEFINE_double(evict_ratio_lowerbound, 0.25,
              "BatchEvict lower-bound eviction ratio");
DEFINE_uint64(lookup_threads, 8, "Concurrent direct GetReplicaList threads");
DEFINE_uint64(hot_objects, 2048, "Keys sampled by lookup threads");
DEFINE_uint64(warmup_ms, 10, "Lookup warmup before BatchEvict");
DEFINE_string(mode, "evict", "Benchmark mode: evict, mixed, or watermark");
DEFINE_uint64(mixed_threads, 8, "Worker threads in mixed mode");
DEFINE_uint64(operations_per_thread, 50000,
              "Alternating put/get operations per mixed worker");
DEFINE_uint64(num_segments, 8, "Memory segments in mixed mode");
DEFINE_double(initial_used_ratio, 0.89,
              "Initial allocator usage in watermark mode");
DEFINE_double(high_watermark_ratio, 0.90, "Automatic eviction high watermark");
DEFINE_double(watermark_eviction_ratio, 0.05,
              "Minimum automatic eviction ratio");
DEFINE_uint64(monitor_interval_ms, 1,
              "Usage sampling interval in watermark mode");
DEFINE_uint64(settle_timeout_ms, 5000,
              "Maximum post-workload eviction settling time");

namespace mooncake::benchmarks {

struct LookupWorkerResult {
    std::vector<uint64_t> during_latencies_ns;
    uint64_t operations{0};
    uint64_t failures{0};
};

struct RunResult {
    uint64_t evict_us{0};
    size_t evicted_objects{0};
    uint64_t freed_bytes{0};
    std::vector<uint64_t> lookup_latencies_ns;
    uint64_t lookup_operations{0};
    uint64_t lookup_failures{0};
};

struct MixedWorkerResult {
    std::vector<uint64_t> put_latencies_ns;
    std::vector<uint64_t> get_latencies_ns;
    uint64_t get_hits{0};
    uint64_t failures{0};
};

struct WatermarkWorkerResult {
    std::vector<uint64_t> before_put_latencies_ns;
    std::vector<uint64_t> before_get_latencies_ns;
    std::vector<uint64_t> pressure_put_latencies_ns;
    std::vector<uint64_t> pressure_put_success_latencies_ns;
    std::vector<uint64_t> pressure_put_failure_latencies_ns;
    std::vector<uint64_t> pressure_get_latencies_ns;
    uint64_t put_successes{0};
    uint64_t put_start_failures{0};
    uint64_t put_end_failures{0};
    uint64_t get_hits{0};
    uint64_t get_failures{0};
};

struct WatermarkMonitorResult {
    double maximum_used_ratio{0.0};
    uint64_t observed_drops{0};
};

class BatchEvictBench {
   public:
    static bool Run() {
        MasterService service(MakeConfig());
        const UUID client_id = generate_uuid();
        if (!MountSegment(service, client_id) ||
            !CreateObjects(service, client_id)) {
            return false;
        }
        ExpireAllLeases(service);
        if (!WarmHotObjects(service)) {
            return false;
        }

        RunResult result;
        if (!RunConcurrentEviction(service, result)) {
            return false;
        }
        std::sort(result.lookup_latencies_ns.begin(),
                  result.lookup_latencies_ns.end());
        std::cout
            << "num_objects,total_us,evicted_count,freed_bytes,lookup_threads,"
               "lookup_samples,lookup_p50_ns,lookup_p99_ns,lookup_max_ns,"
               "lookup_operations,lookup_failures"
            << std::endl;
        std::cout << FLAGS_num_objects << "," << result.evict_us << ","
                  << result.evicted_objects << "," << result.freed_bytes << ","
                  << FLAGS_lookup_threads << ","
                  << result.lookup_latencies_ns.size() << ","
                  << Percentile(result.lookup_latencies_ns, 50) << ","
                  << Percentile(result.lookup_latencies_ns, 99) << ","
                  << (result.lookup_latencies_ns.empty()
                          ? 0
                          : result.lookup_latencies_ns.back())
                  << "," << result.lookup_operations << ","
                  << result.lookup_failures << std::endl;
        return true;
    }

    static bool RunMixed() {
        MasterService service(MakeConfig());
        const UUID client_id = generate_uuid();
        if (!MountMixedSegments(service, client_id)) {
            return false;
        }
        std::vector<std::string> hot_keys;
        hot_keys.reserve(FLAGS_hot_objects);
        for (size_t index = 0; index < FLAGS_hot_objects; ++index) {
            hot_keys.push_back(MixedHotKey(index));
            if (!CreateMixedObject(service, client_id, hot_keys.back(),
                                   index % FLAGS_num_segments)) {
                return false;
            }
        }

        std::vector<std::vector<std::string>> put_keys(FLAGS_mixed_threads);
        for (size_t worker = 0; worker < FLAGS_mixed_threads; ++worker) {
            auto& keys = put_keys[worker];
            keys.reserve((FLAGS_operations_per_thread + 1) / 2);
            for (size_t sequence = 0;
                 sequence < (FLAGS_operations_per_thread + 1) / 2; ++sequence) {
                keys.push_back(MixedPutKey(worker, sequence));
            }
        }

        std::atomic<size_t> ready{0};
        std::atomic<bool> start{false};
        std::vector<MixedWorkerResult> worker_results(FLAGS_mixed_threads);
        std::vector<std::thread> workers;
        workers.reserve(FLAGS_mixed_threads);
        for (size_t worker = 0; worker < FLAGS_mixed_threads; ++worker) {
            workers.emplace_back([&, worker] {
                worker_results[worker] =
                    RunMixedWorker(service, client_id, worker, hot_keys,
                                   put_keys[worker], ready, start);
            });
        }
        while (ready.load(std::memory_order_acquire) != FLAGS_mixed_threads) {
            std::this_thread::yield();
        }
        const auto begin = std::chrono::steady_clock::now();
        start.store(true, std::memory_order_release);
        for (auto& worker : workers) {
            worker.join();
        }
        const auto elapsed = std::chrono::steady_clock::now() - begin;

        std::vector<uint64_t> puts;
        std::vector<uint64_t> gets;
        uint64_t hits = 0;
        uint64_t failures = 0;
        for (auto& worker : worker_results) {
            puts.insert(
                puts.end(),
                std::make_move_iterator(worker.put_latencies_ns.begin()),
                std::make_move_iterator(worker.put_latencies_ns.end()));
            gets.insert(
                gets.end(),
                std::make_move_iterator(worker.get_latencies_ns.begin()),
                std::make_move_iterator(worker.get_latencies_ns.end()));
            hits += worker.get_hits;
            failures += worker.failures;
        }
        std::sort(puts.begin(), puts.end());
        std::sort(gets.begin(), gets.end());
        const auto elapsed_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed)
                .count();
        const uint64_t operations =
            FLAGS_mixed_threads * FLAGS_operations_per_thread;
        const double operations_per_second = static_cast<double>(operations) *
                                             1e9 /
                                             static_cast<double>(elapsed_ns);
        std::cout << "threads,segments,operations,ops_per_sec,put_p50_ns,"
                     "put_p99_ns,put_max_ns,get_p50_ns,get_p99_ns,get_max_ns,"
                     "get_hits,failures"
                  << std::endl;
        std::cout << FLAGS_mixed_threads << "," << FLAGS_num_segments << ","
                  << operations << ","
                  << static_cast<uint64_t>(operations_per_second) << ","
                  << Percentile(puts, 50) << "," << Percentile(puts, 99) << ","
                  << (puts.empty() ? 0 : puts.back()) << ","
                  << Percentile(gets, 50) << "," << Percentile(gets, 99) << ","
                  << (gets.empty() ? 0 : gets.back()) << "," << hits << ","
                  << failures << std::endl;
        return failures == 0 && hits == gets.size();
    }

    static bool RunWatermark() {
        MasterService service(MakeWatermarkConfig());
        const UUID client_id = generate_uuid();
        if (!MountWatermarkSegment(service, client_id) ||
            !CreateObjects(service, client_id)) {
            return false;
        }
        ExpireAllLeases(service);
        if (!WarmHotObjects(service)) {
            return false;
        }

        const auto initial_usage = service.QuerySegments(kSegmentName);
        if (!initial_usage.has_value()) {
            return false;
        }
        const double observed_initial_ratio =
            static_cast<double>(initial_usage->first) /
            static_cast<double>(initial_usage->second);
        if (observed_initial_ratio >= FLAGS_high_watermark_ratio) {
            LOG(ERROR) << "initial usage already crossed the high watermark: "
                       << observed_initial_ratio;
            return false;
        }

        std::vector<std::string> hot_keys;
        hot_keys.reserve(FLAGS_hot_objects);
        for (size_t index = 0; index < FLAGS_hot_objects; ++index) {
            hot_keys.push_back(Key(index));
        }
        std::vector<std::vector<std::string>> put_keys(FLAGS_mixed_threads);
        for (size_t worker = 0; worker < FLAGS_mixed_threads; ++worker) {
            auto& keys = put_keys[worker];
            keys.reserve((FLAGS_operations_per_thread + 1) / 2);
            for (size_t sequence = 0;
                 sequence < (FLAGS_operations_per_thread + 1) / 2; ++sequence) {
                keys.push_back(WatermarkPutKey(worker, sequence));
            }
        }

        std::atomic<size_t> ready{0};
        std::atomic<bool> start{false};
        std::atomic<bool> workload_running{true};
        std::atomic<bool> pressure_started{false};
        std::vector<WatermarkWorkerResult> worker_results(FLAGS_mixed_threads);
        WatermarkMonitorResult monitor_result;
        std::thread monitor([&] {
            monitor_result = RunWatermarkMonitor(
                service, start, workload_running, pressure_started);
        });
        std::vector<std::thread> workers;
        workers.reserve(FLAGS_mixed_threads);
        for (size_t worker = 0; worker < FLAGS_mixed_threads; ++worker) {
            workers.emplace_back([&, worker] {
                worker_results[worker] = RunWatermarkWorker(
                    service, client_id, worker, hot_keys, put_keys[worker],
                    ready, start, pressure_started);
            });
        }
        while (ready.load(std::memory_order_acquire) != FLAGS_mixed_threads) {
            std::this_thread::yield();
        }
        const size_t objects_before = service.GetKeyCount();
        const auto begin = std::chrono::steady_clock::now();
        start.store(true, std::memory_order_release);
        for (auto& worker : workers) {
            worker.join();
        }
        const auto elapsed = std::chrono::steady_clock::now() - begin;
        workload_running.store(false, std::memory_order_release);
        monitor.join();

        uint64_t put_successes = 0;
        uint64_t put_start_failures = 0;
        uint64_t put_end_failures = 0;
        uint64_t get_hits = 0;
        uint64_t get_failures = 0;
        std::vector<uint64_t> before_puts;
        std::vector<uint64_t> before_gets;
        std::vector<uint64_t> pressure_puts;
        std::vector<uint64_t> pressure_put_successes;
        std::vector<uint64_t> pressure_put_failures;
        std::vector<uint64_t> pressure_gets;
        for (auto& worker : worker_results) {
            put_successes += worker.put_successes;
            put_start_failures += worker.put_start_failures;
            put_end_failures += worker.put_end_failures;
            get_hits += worker.get_hits;
            get_failures += worker.get_failures;
            MoveAppend(before_puts, worker.before_put_latencies_ns);
            MoveAppend(before_gets, worker.before_get_latencies_ns);
            MoveAppend(pressure_puts, worker.pressure_put_latencies_ns);
            MoveAppend(pressure_put_successes,
                       worker.pressure_put_success_latencies_ns);
            MoveAppend(pressure_put_failures,
                       worker.pressure_put_failure_latencies_ns);
            MoveAppend(pressure_gets, worker.pressure_get_latencies_ns);
        }
        SortLatencies(before_puts, before_gets, pressure_puts, pressure_gets);
        std::sort(pressure_put_successes.begin(), pressure_put_successes.end());
        std::sort(pressure_put_failures.begin(), pressure_put_failures.end());

        const size_t objects_after = service.GetKeyCount();
        const uint64_t expected_without_eviction =
            objects_before + put_successes;
        if (objects_after > expected_without_eviction) {
            LOG(ERROR) << "object count grew beyond successful puts";
            return false;
        }
        const uint64_t evicted_objects =
            expected_without_eviction - objects_after;
        const auto final_usage = service.QuerySegments(kSegmentName);
        if (!final_usage.has_value()) {
            return false;
        }
        const double final_used_ratio =
            static_cast<double>(final_usage->first) /
            static_cast<double>(final_usage->second);
        const uint64_t operations =
            FLAGS_mixed_threads * FLAGS_operations_per_thread;
        const auto elapsed_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed)
                .count();
        const uint64_t operations_per_second =
            static_cast<uint64_t>(static_cast<double>(operations) * 1e9 /
                                  static_cast<double>(elapsed_ns));

        std::cout
            << "objects_before,initial_ratio,max_ratio,final_ratio,operations,"
               "ops_per_sec,put_success,put_start_fail,put_end_fail,get_hits,"
               "get_fail,evicted_objects,observed_drops,before_put_samples,"
               "before_put_p99_ns,before_get_samples,before_get_p99_ns,"
               "pressure_put_samples,pressure_put_p99_ns,pressure_put_p999_ns,"
               "pressure_put_max_ns,pressure_put_success_samples,"
               "pressure_put_success_p99_ns,pressure_put_success_p999_ns,"
               "pressure_put_success_max_ns,pressure_put_failure_p99_ns,"
               "pressure_get_samples,pressure_get_p99_ns,"
               "pressure_get_p999_ns,pressure_get_max_ns"
            << std::endl;
        std::cout << objects_before << "," << observed_initial_ratio << ","
                  << monitor_result.maximum_used_ratio << ","
                  << final_used_ratio << "," << operations << ","
                  << operations_per_second << "," << put_successes << ","
                  << put_start_failures << "," << put_end_failures << ","
                  << get_hits << "," << get_failures << "," << evicted_objects
                  << "," << monitor_result.observed_drops << ","
                  << before_puts.size() << "," << Percentile(before_puts, 99)
                  << "," << before_gets.size() << ","
                  << Percentile(before_gets, 99) << "," << pressure_puts.size()
                  << "," << Percentile(pressure_puts, 99) << ","
                  << PercentileFraction(pressure_puts, 999, 1000) << ","
                  << Maximum(pressure_puts) << ","
                  << pressure_put_successes.size() << ","
                  << Percentile(pressure_put_successes, 99) << ","
                  << PercentileFraction(pressure_put_successes, 999, 1000)
                  << "," << Maximum(pressure_put_successes) << ","
                  << Percentile(pressure_put_failures, 99) << ","
                  << pressure_gets.size() << ","
                  << Percentile(pressure_gets, 99) << ","
                  << PercentileFraction(pressure_gets, 999, 1000) << ","
                  << Maximum(pressure_gets) << std::endl;
        return pressure_started.load(std::memory_order_acquire) &&
               evicted_objects > 0 && !pressure_puts.empty() &&
               !pressure_gets.empty();
    }

   private:
    static constexpr const char* kSegmentName =
        "object_catalog_concurrent_bench_segment";
    static constexpr uintptr_t kSegmentBase = 0x300000000ULL;
    static constexpr uint64_t kObjectSize = 1024;
    static constexpr uint64_t kMixedObjectSize = 4096;
    static constexpr uint64_t kMixedSegmentSize = UINT64_C(8) << 30;
    static constexpr uintptr_t kMixedSegmentBase = UINT64_C(0x1000000000);

    static MasterServiceConfig MakeConfig() {
        return MasterServiceConfig::builder()
            .set_memory_allocator(BufferAllocatorType::OFFSET)
            .set_eviction_ratio(0.0)
            .set_eviction_high_watermark_ratio(1.0)
            .set_client_live_ttl_sec(3600)
            .build();
    }

    static MasterServiceConfig MakeWatermarkConfig() {
        return MasterServiceConfig::builder()
            .set_memory_allocator(BufferAllocatorType::OFFSET)
            .set_eviction_ratio(FLAGS_watermark_eviction_ratio)
            .set_eviction_high_watermark_ratio(FLAGS_high_watermark_ratio)
            .set_client_live_ttl_sec(3600)
            .build();
    }

    static size_t SegmentSize() {
        constexpr size_t kMinSegmentSize = 16 * 1024 * 1024;
        const size_t needed = FLAGS_num_objects * kObjectSize;
        return std::max(kMinSegmentSize,
                        needed + needed / 8 + 1024 * kObjectSize);
    }

    static size_t WatermarkSegmentSize() {
        const double required =
            static_cast<double>(FLAGS_num_objects * kObjectSize) /
            FLAGS_initial_used_ratio;
        return static_cast<size_t>(std::ceil(required));
    }

    static std::string Key(size_t index) {
        return "batch_evict_bench_key_" + std::to_string(index);
    }

    static std::string MixedSegmentName(size_t index) {
        return "catalog-memory-" + std::to_string(index);
    }

    static std::string MixedHotKey(size_t index) {
        char key[32];
        std::snprintf(key, sizeof(key), "hot-%08zx", index);
        return key;
    }

    static std::string MixedPutKey(size_t worker, size_t sequence) {
        char key[64];
        std::snprintf(key, sizeof(key), "put-%04zx-%016zx", worker, sequence);
        return key;
    }

    static std::string WatermarkPutKey(size_t worker, size_t sequence) {
        char key[64];
        std::snprintf(key, sizeof(key), "watermark-%04zx-%016zx", worker,
                      sequence);
        return key;
    }

    static bool MountWatermarkSegment(MasterService& service,
                                      const UUID& client_id) {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = kSegmentName;
        segment.base = kSegmentBase;
        segment.size = WatermarkSegmentSize();
        segment.te_endpoint = segment.name;
        const auto result = service.MountSegment(segment, client_id);
        if (!result.has_value()) {
            LOG(ERROR) << "watermark MountSegment failed: "
                       << toString(result.error());
            return false;
        }
        return true;
    }

    static bool MountMixedSegments(MasterService& service,
                                   const UUID& client_id) {
        for (size_t index = 0; index < FLAGS_num_segments; ++index) {
            Segment segment;
            segment.id = generate_uuid();
            segment.name = MixedSegmentName(index);
            segment.base =
                kMixedSegmentBase +
                index * static_cast<uintptr_t>(kMixedSegmentSize * 2);
            segment.size = kMixedSegmentSize;
            segment.te_endpoint = segment.name;
            const auto result = service.MountSegment(segment, client_id);
            if (!result.has_value()) {
                LOG(ERROR) << "mixed MountSegment failed at index=" << index;
                return false;
            }
        }
        return true;
    }

    static bool CreateMixedObject(MasterService& service, const UUID& client_id,
                                  const std::string& key,
                                  size_t segment_index) {
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segment = MixedSegmentName(segment_index);
        const auto started = service.PutStart(
            client_id, key, TenantId::Default(), kMixedObjectSize, config);
        if (!started.has_value() || started->size() != 1) {
            return false;
        }
        return service
            .PutEnd(client_id, key, TenantId::Default(), ReplicaType::MEMORY)
            .has_value();
    }

    static MixedWorkerResult RunMixedWorker(
        MasterService& service, const UUID& client_id, size_t worker,
        const std::vector<std::string>& hot_keys,
        const std::vector<std::string>& put_keys, std::atomic<size_t>& ready,
        std::atomic<bool>& start) {
        MixedWorkerResult result;
        result.put_latencies_ns.reserve((FLAGS_operations_per_thread + 1) / 2);
        result.get_latencies_ns.reserve(FLAGS_operations_per_thread / 2);
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segment =
            MixedSegmentName(worker % FLAGS_num_segments);
        uint64_t random = (worker + 1) * UINT64_C(0x9e3779b97f4a7c15);
        size_t put_index = 0;
        ready.fetch_add(1, std::memory_order_release);
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        for (size_t operation = 0; operation < FLAGS_operations_per_thread;
             ++operation) {
            const auto begin = std::chrono::steady_clock::now();
            if ((operation & 1) == 0) {
                const auto& key = put_keys[put_index++];
                const auto started =
                    service.PutStart(client_id, key, TenantId::Default(),
                                     kMixedObjectSize, config);
                bool succeeded = started.has_value() && started->size() == 1;
                if (succeeded) {
                    succeeded = service
                                    .PutEnd(client_id, key, TenantId::Default(),
                                            ReplicaType::MEMORY)
                                    .has_value();
                }
                if (!succeeded) {
                    ++result.failures;
                }
                result.put_latencies_ns.push_back(ElapsedNanos(begin));
            } else {
                random ^= random << 13;
                random ^= random >> 7;
                random ^= random << 17;
                if (service
                        .GetReplicaList(hot_keys[random % hot_keys.size()],
                                        TenantId::Default())
                        .has_value()) {
                    ++result.get_hits;
                } else {
                    ++result.failures;
                }
                result.get_latencies_ns.push_back(ElapsedNanos(begin));
            }
        }
        return result;
    }

    static WatermarkWorkerResult RunWatermarkWorker(
        MasterService& service, const UUID& client_id, size_t worker,
        const std::vector<std::string>& hot_keys,
        const std::vector<std::string>& put_keys, std::atomic<size_t>& ready,
        std::atomic<bool>& start, std::atomic<bool>& pressure_started) {
        WatermarkWorkerResult result;
        const size_t put_count = (FLAGS_operations_per_thread + 1) / 2;
        const size_t get_count = FLAGS_operations_per_thread / 2;
        result.before_put_latencies_ns.reserve(put_count);
        result.before_get_latencies_ns.reserve(get_count);
        result.pressure_put_latencies_ns.reserve(put_count);
        result.pressure_put_success_latencies_ns.reserve(put_count);
        result.pressure_put_failure_latencies_ns.reserve(put_count);
        result.pressure_get_latencies_ns.reserve(get_count);
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segment = kSegmentName;
        uint64_t random = (worker + 1) * UINT64_C(0x9e3779b97f4a7c15);
        size_t put_index = 0;
        ready.fetch_add(1, std::memory_order_release);
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        for (size_t operation = 0; operation < FLAGS_operations_per_thread;
             ++operation) {
            const bool pressure =
                pressure_started.load(std::memory_order_acquire);
            const auto begin = std::chrono::steady_clock::now();
            if ((operation & 1) == 0) {
                const auto& key = put_keys[put_index++];
                const auto started = service.PutStart(
                    client_id, key, TenantId::Default(), kObjectSize, config);
                bool succeeded = false;
                if (!started.has_value() || started->size() != 1) {
                    ++result.put_start_failures;
                } else if (!service
                                .PutEnd(client_id, key, TenantId::Default(),
                                        ReplicaType::MEMORY)
                                .has_value()) {
                    ++result.put_end_failures;
                } else {
                    ++result.put_successes;
                    succeeded = true;
                }
                const uint64_t latency = ElapsedNanos(begin);
                auto& latencies = pressure ? result.pressure_put_latencies_ns
                                           : result.before_put_latencies_ns;
                latencies.push_back(latency);
                if (pressure) {
                    auto& outcome_latencies =
                        succeeded ? result.pressure_put_success_latencies_ns
                                  : result.pressure_put_failure_latencies_ns;
                    outcome_latencies.push_back(latency);
                }
            } else {
                random ^= random << 13;
                random ^= random >> 7;
                random ^= random << 17;
                if (service
                        .GetReplicaList(hot_keys[random % hot_keys.size()],
                                        TenantId::Default())
                        .has_value()) {
                    ++result.get_hits;
                } else {
                    ++result.get_failures;
                }
                auto& latencies = pressure ? result.pressure_get_latencies_ns
                                           : result.before_get_latencies_ns;
                latencies.push_back(ElapsedNanos(begin));
            }
        }
        return result;
    }

    static WatermarkMonitorResult RunWatermarkMonitor(
        MasterService& service, std::atomic<bool>& start,
        std::atomic<bool>& workload_running,
        std::atomic<bool>& pressure_started) {
        WatermarkMonitorResult result;
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        size_t previous_used = 0;
        auto settle_deadline = std::chrono::steady_clock::time_point::max();
        while (true) {
            const auto usage = service.QuerySegments(kSegmentName);
            if (!usage.has_value()) {
                break;
            }
            const size_t used = usage->first;
            const double ratio =
                static_cast<double>(used) / static_cast<double>(usage->second);
            result.maximum_used_ratio =
                std::max(result.maximum_used_ratio, ratio);
            if (ratio > FLAGS_high_watermark_ratio) {
                pressure_started.store(true, std::memory_order_release);
            }
            if (previous_used > used + kObjectSize) {
                ++result.observed_drops;
            }
            previous_used = used;

            if (!workload_running.load(std::memory_order_acquire)) {
                if (settle_deadline ==
                    std::chrono::steady_clock::time_point::max()) {
                    settle_deadline =
                        std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(FLAGS_settle_timeout_ms);
                }
                if (!pressure_started.load(std::memory_order_acquire) ||
                    ratio <= FLAGS_high_watermark_ratio ||
                    std::chrono::steady_clock::now() >= settle_deadline) {
                    break;
                }
            }
            std::this_thread::sleep_for(
                std::chrono::milliseconds(FLAGS_monitor_interval_ms));
        }
        return result;
    }

    static uint64_t ElapsedNanos(std::chrono::steady_clock::time_point begin) {
        return static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - begin)
                .count());
    }

    static bool MountSegment(MasterService& service, const UUID& client_id) {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = kSegmentName;
        segment.base = kSegmentBase;
        segment.size = SegmentSize();
        segment.te_endpoint = segment.name;
        const auto result = service.MountSegment(segment, client_id);
        if (!result.has_value()) {
            LOG(ERROR) << "MountSegment failed: " << toString(result.error());
            return false;
        }
        return true;
    }

    static bool CreateObjects(MasterService& service, const UUID& client_id) {
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segment = kSegmentName;
        for (size_t index = 0; index < FLAGS_num_objects; ++index) {
            const auto key = Key(index);
            const auto started = service.PutStart(
                client_id, key, TenantId::Default(), kObjectSize, config);
            if (!started.has_value() || started->size() != 1) {
                LOG(ERROR) << "PutStart failed at index=" << index;
                return false;
            }
            const auto completed = service.PutEnd(
                client_id, key, TenantId::Default(), ReplicaType::MEMORY);
            if (!completed.has_value()) {
                LOG(ERROR) << "PutEnd failed at index=" << index;
                return false;
            }
        }
        return true;
    }

    static void ExpireAllLeases(MasterService& service) {
        const auto base_expiration =
            std::chrono::system_clock::now() - std::chrono::hours(1);
        size_t ordinal = 0;
        for (size_t shard_index = 0; shard_index < MasterService::kNumShards;
             ++shard_index) {
            MasterService::MetadataShardAccessorRW shard(&service, shard_index);
            for (auto& [tenant_id, tenant_state] : shard->tenants) {
                if (tenant_id != TenantId::Default()) {
                    continue;
                }
                for (auto& [key, metadata] : tenant_state.metadata) {
                    (void)key;
                    SpinLocker locker(&metadata.lock);
                    metadata.lease_timeout =
                        base_expiration + std::chrono::nanoseconds(ordinal++);
                }
            }
        }
    }

    static bool WarmHotObjects(MasterService& service) {
        for (size_t index = 0; index < FLAGS_hot_objects; ++index) {
            if (!service.GetReplicaList(Key(index), TenantId::Default())
                     .has_value()) {
                LOG(ERROR) << "hot lookup failed at index=" << index;
                return false;
            }
        }
        return true;
    }

    static LookupWorkerResult RunLookupWorker(
        MasterService& service, const std::vector<std::string>& hot_keys,
        size_t worker_index, std::atomic<size_t>& ready,
        std::atomic<bool>& start, std::atomic<bool>& evicting,
        std::atomic<bool>& stop) {
        LookupWorkerResult result;
        uint64_t random = (worker_index + 1) * UINT64_C(0x9e3779b97f4a7c15);
        ready.fetch_add(1, std::memory_order_release);
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        while (!stop.load(std::memory_order_acquire)) {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            const bool sample = evicting.load(std::memory_order_acquire);
            const auto begin = std::chrono::steady_clock::now();
            const auto lookup = service.GetReplicaList(
                hot_keys[random % hot_keys.size()], TenantId::Default());
            const auto elapsed =
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - begin)
                    .count();
            ++result.operations;
            if (!lookup.has_value()) {
                ++result.failures;
            }
            if (sample) {
                result.during_latencies_ns.push_back(
                    static_cast<uint64_t>(elapsed));
            }
        }
        return result;
    }

    static bool RunConcurrentEviction(MasterService& service,
                                      RunResult& result) {
        const size_t objects_before = service.GetKeyCount();
        uint64_t used_before = 0;
        const auto usage_before = service.QuerySegments(kSegmentName);
        if (!usage_before.has_value()) {
            return false;
        }
        used_before = usage_before->first;

        std::atomic<size_t> ready{0};
        std::atomic<bool> start{false};
        std::atomic<bool> evicting{false};
        std::atomic<bool> stop{false};
        std::vector<std::thread> workers;
        std::vector<LookupWorkerResult> worker_results(FLAGS_lookup_threads);
        std::vector<std::string> hot_keys;
        hot_keys.reserve(FLAGS_hot_objects);
        for (size_t index = 0; index < FLAGS_hot_objects; ++index) {
            hot_keys.push_back(Key(index));
        }
        workers.reserve(FLAGS_lookup_threads);
        for (size_t worker = 0; worker < FLAGS_lookup_threads; ++worker) {
            workers.emplace_back([&, worker] {
                worker_results[worker] = RunLookupWorker(
                    service, hot_keys, worker, ready, start, evicting, stop);
            });
        }
        while (ready.load(std::memory_order_acquire) != FLAGS_lookup_threads) {
            std::this_thread::yield();
        }
        start.store(true, std::memory_order_release);
        std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_warmup_ms));

        evicting.store(true, std::memory_order_release);
        const auto evict_begin = std::chrono::steady_clock::now();
        service.BatchEvict(FLAGS_evict_ratio_target,
                           FLAGS_evict_ratio_lowerbound);
        result.evict_us = std::chrono::duration_cast<std::chrono::microseconds>(
                              std::chrono::steady_clock::now() - evict_begin)
                              .count();
        evicting.store(false, std::memory_order_release);
        stop.store(true, std::memory_order_release);
        for (auto& worker : workers) {
            worker.join();
        }

        const size_t objects_after = service.GetKeyCount();
        const auto usage_after = service.QuerySegments(kSegmentName);
        if (!usage_after.has_value()) {
            return false;
        }
        result.evicted_objects = objects_before - objects_after;
        result.freed_bytes = used_before - usage_after->first;
        for (auto& worker : worker_results) {
            result.lookup_operations += worker.operations;
            result.lookup_failures += worker.failures;
            result.lookup_latencies_ns.insert(
                result.lookup_latencies_ns.end(),
                std::make_move_iterator(worker.during_latencies_ns.begin()),
                std::make_move_iterator(worker.during_latencies_ns.end()));
        }

        const size_t expected_evictions = static_cast<size_t>(
            std::ceil(objects_before * FLAGS_evict_ratio_target));
        if (result.evicted_objects != expected_evictions ||
            result.freed_bytes != result.evicted_objects * kObjectSize) {
            LOG(ERROR) << "unexpected eviction result: objects="
                       << result.evicted_objects
                       << ", bytes=" << result.freed_bytes;
            return false;
        }
        return true;
    }

    static uint64_t Percentile(const std::vector<uint64_t>& sorted,
                               size_t percentile) {
        return PercentileFraction(sorted, percentile, 100);
    }

    static uint64_t PercentileFraction(const std::vector<uint64_t>& sorted,
                                       size_t numerator, size_t denominator) {
        if (sorted.empty()) {
            return 0;
        }
        const size_t rank =
            (sorted.size() * numerator + denominator - 1) / denominator;
        return sorted[std::min(rank - 1, sorted.size() - 1)];
    }

    static uint64_t Maximum(const std::vector<uint64_t>& sorted) {
        return sorted.empty() ? 0 : sorted.back();
    }

    static void MoveAppend(std::vector<uint64_t>& destination,
                           std::vector<uint64_t>& source) {
        destination.insert(destination.end(),
                           std::make_move_iterator(source.begin()),
                           std::make_move_iterator(source.end()));
    }

    static void SortLatencies(std::vector<uint64_t>& first,
                              std::vector<uint64_t>& second,
                              std::vector<uint64_t>& third,
                              std::vector<uint64_t>& fourth) {
        std::sort(first.begin(), first.end());
        std::sort(second.begin(), second.end());
        std::sort(third.begin(), third.end());
        std::sort(fourth.begin(), fourth.end());
    }
};

}  // namespace mooncake::benchmarks

int main(int argc, char** argv) {
    google::InitGoogleLogging("MooncakeObjectCatalogBenchmark");
    FLAGS_logtostderr = true;
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    bool ok = false;
    if (FLAGS_mode == "evict") {
        if (FLAGS_num_objects == 0 || FLAGS_lookup_threads == 0 ||
            FLAGS_hot_objects == 0 || FLAGS_hot_objects >= FLAGS_num_objects ||
            !(FLAGS_evict_ratio_lowerbound > 0.0 &&
              FLAGS_evict_ratio_lowerbound <= FLAGS_evict_ratio_target &&
              FLAGS_evict_ratio_target <= 1.0)) {
            LOG(ERROR) << "invalid evict benchmark arguments";
        } else {
            ok = mooncake::benchmarks::BatchEvictBench::Run();
        }
    } else if (FLAGS_mode == "mixed") {
        if (FLAGS_mixed_threads == 0 || FLAGS_operations_per_thread < 2 ||
            FLAGS_num_segments == 0 || FLAGS_hot_objects == 0) {
            LOG(ERROR) << "invalid mixed benchmark arguments";
        } else {
            ok = mooncake::benchmarks::BatchEvictBench::RunMixed();
        }
    } else if (FLAGS_mode == "watermark") {
        if (FLAGS_num_objects == 0 || FLAGS_mixed_threads == 0 ||
            FLAGS_operations_per_thread < 2 || FLAGS_hot_objects == 0 ||
            FLAGS_hot_objects >= FLAGS_num_objects ||
            FLAGS_initial_used_ratio <= 0.0 ||
            FLAGS_initial_used_ratio >= FLAGS_high_watermark_ratio ||
            FLAGS_high_watermark_ratio >= 1.0 ||
            FLAGS_watermark_eviction_ratio <= 0.0 ||
            FLAGS_watermark_eviction_ratio >= 1.0 ||
            FLAGS_monitor_interval_ms == 0) {
            LOG(ERROR) << "invalid watermark benchmark arguments";
        } else {
            ok = mooncake::benchmarks::BatchEvictBench::RunWatermark();
        }
    } else {
        LOG(ERROR) << "unknown benchmark mode: " << FLAGS_mode;
    }
    google::ShutdownGoogleLogging();
    return ok ? 0 : 1;
}
