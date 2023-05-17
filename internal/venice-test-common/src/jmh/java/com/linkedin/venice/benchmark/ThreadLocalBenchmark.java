package com.linkedin.venice.benchmark;

import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;


@Fork(value = 1)
@Warmup(iterations = 1)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Threads(Threads.MAX)
public class ThreadLocalBenchmark {
  private static final ThreadLocal<Object> threadLocal = ThreadLocal.withInitial(String::new);

  public static void main(String[] args) throws Exception {
    Options options = new OptionsBuilder().include(ThreadLocalBenchmark.class.getSimpleName()).build();
    new Runner(options).run();
  }

  @Benchmark
  public void load(Blackhole blackhole) {
    blackhole.consume(threadLocal.get());
  }

  @Benchmark
  public void noop(Blackhole blackhole) {
    blackhole.consume(0);
  }
}
