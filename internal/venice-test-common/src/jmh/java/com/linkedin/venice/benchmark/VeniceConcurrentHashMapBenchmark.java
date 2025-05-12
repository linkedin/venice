package com.linkedin.venice.benchmark;

import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 3)
public class VeniceConcurrentHashMapBenchmark {
  private final int mapEntryKeyRange = 400000;
  private final Random random = new Random();
  VeniceConcurrentHashMap<Integer, Long> concurrentHashMap;
  DescriptiveStatistics aggregatedAvgStats = new DescriptiveStatistics();
  DescriptiveStatistics aggregatedMaxStats = new DescriptiveStatistics();
  DescriptiveStatistics aggregatedP99Stats = new DescriptiveStatistics();

  @Setup
  public void setUp() {
    concurrentHashMap = new VeniceConcurrentHashMap<>();
    for (int i = 0; i < mapEntryKeyRange; i++) {
      concurrentHashMap.put(i, 0L);
    }
  }

  @TearDown
  public void cleanUp() {
    System.out.println(
        "Aggregated stats: Avg: " + aggregatedAvgStats.getMean() + "; Max avg: " + aggregatedAvgStats.getMax()
            + "; Avg P99: " + aggregatedP99Stats.getMean() + "; Max P99: " + aggregatedP99Stats.getMax() + "; Max: "
            + aggregatedMaxStats.getMax());
  }

  @State(Scope.Thread)
  public static class ThreadContext {
    Time time = SystemTime.INSTANCE;
    DescriptiveStatistics stats = new DescriptiveStatistics();

    @TearDown
    public void end(VeniceConcurrentHashMapBenchmark benchmark) {
      benchmark.aggregatedAvgStats.addValue(stats.getMean());
      benchmark.aggregatedMaxStats.addValue(stats.getMax());
      benchmark.aggregatedP99Stats.addValue(stats.getPercentile(99));
    }
  }

  public static void main(String[] args) throws RunnerException {
    org.openjdk.jmh.runner.options.Options opt =
        new OptionsBuilder().include(VeniceConcurrentHashMapBenchmark.class.getSimpleName())
            // .addProfiler(GCProfiler.class)
            .build();
    new Runner(opt).run();
  }

  @Benchmark
  @Threads(80)
  public void veniceConcurrentHashMapWithThreadCount_80(ThreadContext threadContext, Blackhole bh) {
    test(threadContext, bh);
  }

  private void test(ThreadContext threadContext, Blackhole bh) {
    int key = random.nextInt(mapEntryKeyRange);
    long startTime = threadContext.time.getNanoseconds();
    concurrentHashMap.put(key, startTime);
    double latencyInMs = LatencyUtils.getElapsedTimeFromNSToMS(startTime);
    threadContext.stats.addValue(latencyInMs);
    bh.consume(latencyInMs);
  }
}
