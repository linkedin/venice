package com.linkedin.venice.benchmark;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/**
 * Benchmark test avro vs fast-avro, ZipLine floatVectors and flatbuffers ser-deser performance.
 * We are using JMH platform to the do the testing. You can specify adjust the benchmark run using command-line options
 * Example: https://github.com/Valloric/jmh-playground
 *
 * To run the test, build the project and run the following commands:
 * ligradle jmh
 * If above command throws an error, you can try run `ligradle jmh --debug` first to clean up all the caches, then retry
 * `ligradle jmh` again to run the results.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 2)
@Warmup(iterations = 1)
@Measurement(iterations = 3)
public class SerdeBenchmark {
  private final static int numQueries = 1000_000;
  private final static int _numThreads = 2;

  /**
   * Testing with different value size
   */
  @Param({"2500", "5000"})
  protected String valueSize;

  @Setup
  public void setup() {
  }

  @TearDown
  public void teardown() {
  }

  public static void main(String[] args) throws RunnerException {
    org.openjdk.jmh.runner.options.Options opt = new OptionsBuilder()
        .addProfiler(GCProfiler.class)
        .build();
    new Runner(opt).run();
  }

  @Benchmark
  public void avroSerdeBenchmarkTest(Blackhole bh) {
      Map<String, Float> metricsDelta = BenchmarkUtils.runWithMetrics(
          () -> BenchmarkUtils.avroBenchmark(false, Integer.parseInt(valueSize), numQueries), "avro_" + valueSize , numQueries, _numThreads);
  }

  @Benchmark
  public void fastAvroSerdeBenchmarkTest(Blackhole bh) {
    Map<String, Float> metricsDelta = BenchmarkUtils.runWithMetrics(
        () -> BenchmarkUtils.avroBenchmark(true, Integer.parseInt(valueSize), numQueries), "fastAvro_" + valueSize, numQueries,  _numThreads);
  }

  @Benchmark
  public void flatbuffersBenchmarkTest(Blackhole bh) {
    Map<String, Float> metricsDelta = BenchmarkUtils.runWithMetrics(
        () -> BenchmarkUtils.flatBuffersBenchmark(Integer.parseInt(valueSize), numQueries), "flatbuffers_" + valueSize , numQueries,  _numThreads);
  }

  @Benchmark
  public void floatVectorBenchmarkTest(Blackhole bh) {
    Map<String, Float> metricsDelta = BenchmarkUtils.runWithMetrics(
        () -> BenchmarkUtils.floatVectorBenchmark(Integer.parseInt(valueSize), numQueries), "floatVectors_" + valueSize , numQueries,  _numThreads);
  }
}
