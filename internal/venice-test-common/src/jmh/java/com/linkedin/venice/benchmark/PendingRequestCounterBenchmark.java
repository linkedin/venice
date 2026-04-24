package com.linkedin.venice.benchmark;

import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/**
 * Benchmarks two implementations of the pending request counter in InstanceHealthMonitor:
 *
 * 1. CURRENT:  Map<String, Integer> + ConcurrentHashMap.compute()
 *    - Takes a segment lock on every increment and decrement
 *    - Boxes/unboxes Integer on every update (heap allocation if value > 127)
 *
 * 2. PROPOSED: Map<String, AtomicInteger> + AtomicInteger.incrementAndGet()
 *    - Lock-free CAS for increment/decrement once the AtomicInteger is in the map
 *    - No per-update heap allocation
 *
 * Each benchmark iteration models one full request lifecycle:
 *   increment (on send) → decrement (on response)
 *
 * @param instanceCount simulates the number of distinct server instances
 *                      (Venice typically has ~30 per store)
 *
 * Thread counts mirror realistic Venice client concurrency:
 *   1  → single-threaded baseline
 *   4  → light load
 *   16 → moderate load
 *   32 → heavy load / hotspot scenario
 *
 * Run with:
 *   ./gradlew :internal:venice-test-common:jmh
 *   or to include GC allocation data:
 *   add -prof gc to the OptionsBuilder below
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
public class PendingRequestCounterBenchmark {
  /**
   * Number of server instances. Controls contention level:
   *   1  → extreme contention (all threads hit one instance)
   *   10 → moderate (approximates a skewed hotspot)
   *   30 → realistic Venice topology (spread load)
   */
  @Param({ "1", "10", "30" })
  int instanceCount;

  // ---- shared maps (Scope.Benchmark = one instance shared across all threads) ----
  Map<String, Integer> boxedMap;
  Map<String, AtomicInteger> atomicMap;

  // Pre-built instance key array to avoid String allocation on the hot path
  String[] instanceKeys;

  @Setup(Level.Trial)
  public void setUp() {
    boxedMap = new VeniceConcurrentHashMap<>();
    atomicMap = new VeniceConcurrentHashMap<>();
    instanceKeys = new String[instanceCount];

    for (int i = 0; i < instanceCount; i++) {
      String key = "https://venice-server-" + i + ":1234";
      instanceKeys[i] = key;
      boxedMap.put(key, 0);
      atomicMap.put(key, new AtomicInteger(0));
    }
  }

  // ---- per-thread state to pick a pseudo-random instance without ThreadLocalRandom overhead ----
  @State(Scope.Thread)
  public static class ThreadState {
    // Simple counter cycling through instances — avoids ThreadLocalRandom on the hot path
    // while still spreading load across instances
    int idx = 0;
  }

  // ========== CURRENT IMPLEMENTATION ==========

  /**
   * Models the current InstanceHealthMonitor behaviour:
   *   increment via compute() on send, decrement via compute() on completion.
   */
  @Benchmark
  @Threads(1)
  public void boxedCompute_t01(ThreadState ts) {
    requestLifecycleBoxed(ts);
  }

  @Benchmark
  @Threads(4)
  public void boxedCompute_t04(ThreadState ts) {
    requestLifecycleBoxed(ts);
  }

  @Benchmark
  @Threads(16)
  public void boxedCompute_t16(ThreadState ts) {
    requestLifecycleBoxed(ts);
  }

  @Benchmark
  @Threads(32)
  public void boxedCompute_t32(ThreadState ts) {
    requestLifecycleBoxed(ts);
  }

  // ========== PROPOSED IMPLEMENTATION ==========

  /**
   * Models the proposed AtomicInteger behaviour.
   * Uses getAndUpdate(v -> Math.max(0, v - 1)) to match decrementPendingCounter() exactly.
   */
  @Benchmark
  @Threads(1)
  public void atomicIncDec_t01(ThreadState ts) {
    requestLifecycleAtomic(ts);
  }

  @Benchmark
  @Threads(4)
  public void atomicIncDec_t04(ThreadState ts) {
    requestLifecycleAtomic(ts);
  }

  @Benchmark
  @Threads(16)
  public void atomicIncDec_t16(ThreadState ts) {
    requestLifecycleAtomic(ts);
  }

  @Benchmark
  @Threads(32)
  public void atomicIncDec_t32(ThreadState ts) {
    requestLifecycleAtomic(ts);
  }

  // ========== HELPERS ==========

  private String pickInstance(ThreadState ts) {
    // Cycle through keys: simulates round-robin style dispatch across instances
    String key = instanceKeys[ts.idx % instanceCount];
    ts.idx++;
    return key;
  }

  /**
   * Mirrors the current InstanceHealthMonitor increment + decrement pair for one request lifecycle.
   * Both operations use the SAME instance, matching production behaviour where a single request
   * increments on send and decrements on completion for the same server.
   * Mirrors InstanceHealthMonitor.java trackHealthBasedOnRequestToInstance() (increment)
   * and decrementPendingCounter() (decrement).
   */
  private void requestLifecycleBoxed(ThreadState ts) {
    String instance = pickInstance(ts); // one instance for both inc and dec
    boxedMap.compute(instance, (k, v) -> {
      if (v == null) {
        return 1;
      }
      return v + 1;
    });
    boxedMap.compute(instance, (k, v) -> {
      if (v == null || v == 0) {
        return 0;
      }
      return v - 1;
    });
  }

  /**
   * Mirrors the proposed AtomicInteger increment + decrement pair for one request lifecycle.
   * Uses the SAME instance for both operations, and uses getAndUpdate(v -> Math.max(0, v - 1))
   * to match decrementPendingCounter() in InstanceHealthMonitor exactly.
   */
  private void requestLifecycleAtomic(ThreadState ts) {
    String instance = pickInstance(ts); // one instance for both inc and dec
    atomicMap.get(instance).incrementAndGet();
    atomicMap.get(instance).getAndUpdate(v -> Math.max(0, v - 1));
  }

  public static void main(String[] args) throws RunnerException {
    org.openjdk.jmh.runner.options.Options opt =
        new OptionsBuilder().include(PendingRequestCounterBenchmark.class.getSimpleName())
            .addProfiler(GCProfiler.class) // shows allocations/op — key signal for the boxing issue
            .build();
    new Runner(opt).run();
  }
}
