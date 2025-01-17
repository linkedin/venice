package com.linkedin.venice.benchmark;

import static java.util.concurrent.TimeUnit.SECONDS;

import com.linkedin.venice.throttle.TokenBucket;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.time.Clock;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/**
 * To run the test, build the project and run the following commands:
 * ligradle jmh
 * If above command throws an error, you can try run `ligradle jmh --debug` first to clean up all the caches, then retry
 * `ligradle jmh` again to run the results.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 3)
public class TokenBucketBenchmark {
  private final int enforcementIntervalSeconds = 10; // TokenBucket refill interval
  private final int enforcementCapacityMultiple = 5; // Token bucket capacity is refill amount times this multiplier

  /**
   * Testing with different capacities: 1T (basically unlimited), 1M
   */
  @Param({ "1000000000000", "1000000" })
  protected long rcuPerSecond;

  /**
   * Testing with different tokensToConsume: 1 (single get), 100 (batch get)
   */
  @Param({ "1", "100" })
  protected long tokensToConsume;

  TokenBucket tokenBucket;

  AtomicLong approvedTotal = new AtomicLong(0);
  AtomicLong deniedTotal = new AtomicLong(0);

  @State(Scope.Thread)
  public static class ThreadContext {
    long approved;
    long denied;

    @TearDown
    public void end(TokenBucketBenchmark benchmark) {
      benchmark.approvedTotal.addAndGet(approved);
      benchmark.deniedTotal.addAndGet(denied);
    }
  }

  /**
   * Copied from {@link com.linkedin.venice.listener.ReadQuotaEnforcementHandler}
   */
  private TokenBucket tokenBucketFromRcuPerSecond(long totalRcuPerSecond, double thisBucketProportionOfTotalRcu) {
    long totalRefillAmount = totalRcuPerSecond * enforcementIntervalSeconds;
    long totalCapacity = totalRefillAmount * enforcementCapacityMultiple;
    long thisRefillAmount = calculateRefillAmount(totalRcuPerSecond, thisBucketProportionOfTotalRcu);
    long thisCapacity = (long) Math.ceil(totalCapacity * thisBucketProportionOfTotalRcu);
    return new TokenBucket(thisCapacity, thisRefillAmount, enforcementIntervalSeconds, SECONDS, Clock.systemUTC());
  }

  /**
   * Copied from {@link com.linkedin.venice.listener.ReadQuotaEnforcementHandler}
   */
  private long calculateRefillAmount(long totalRcuPerSecond, double thisBucketProportionOfTotalRcu) {
    long totalRefillAmount = totalRcuPerSecond * enforcementIntervalSeconds;
    return (long) Math.ceil(totalRefillAmount * thisBucketProportionOfTotalRcu);
  }

  @Setup
  public void setUp() {
    this.tokenBucket = tokenBucketFromRcuPerSecond(rcuPerSecond, 1);
  }

  @TearDown
  public void cleanUp() {
    long approved = approvedTotal.get();
    long denied = deniedTotal.get();
    double approvalRatio = (double) approved / ((double) approved + (double) denied);
    NumberFormat formatter = new DecimalFormat("#0.00");
    String approvalRatioStr = formatter.format(approvalRatio);

    System.out.println();
    System.out.println(
        "RCU/sec: " + rcuPerSecond + "; Tokens to consume: " + tokensToConsume + "; Approved: " + approved
            + "; Denied: " + denied + "; Approval ratio: " + approvalRatioStr);
  }

  public static void main(String[] args) throws RunnerException {
    org.openjdk.jmh.runner.options.Options opt =
        new OptionsBuilder().include(TokenBucketBenchmark.class.getSimpleName())
            // .addProfiler(GCProfiler.class)
            .build();
    new Runner(opt).run();
  }

  @Benchmark
  @Threads(1)
  public void tokenBucketWithThreadCount_01(ThreadContext threadContext, Blackhole bh) {
    test(threadContext, bh);
  }

  @Benchmark
  @Threads(2)
  public void tokenBucketWithThreadCount_02(ThreadContext threadContext, Blackhole bh) {
    test(threadContext, bh);
  }

  @Benchmark
  @Threads(4)
  public void tokenBucketWithThreadCount_04(ThreadContext threadContext, Blackhole bh) {
    test(threadContext, bh);
  }

  @Benchmark
  @Threads(8)
  public void tokenBucketWithThreadCount_08(ThreadContext threadContext, Blackhole bh) {
    test(threadContext, bh);
  }

  @Benchmark
  @Threads(16)
  public void tokenBucketWithThreadCount_16(ThreadContext threadContext, Blackhole bh) {
    test(threadContext, bh);
  }

  @Benchmark
  @Threads(32)
  public void tokenBucketWithThreadCount_32(ThreadContext threadContext, Blackhole bh) {
    test(threadContext, bh);
  }

  @Benchmark
  @Threads(64)
  public void tokenBucketWithThreadCount_64(ThreadContext threadContext, Blackhole bh) {
    test(threadContext, bh);
  }

  private void test(ThreadContext threadContext, Blackhole bh) {
    if (this.tokenBucket.tryConsume(tokensToConsume)) {
      bh.consume(threadContext.approved++);
    } else {
      bh.consume(threadContext.denied++);
    }
  }
}
