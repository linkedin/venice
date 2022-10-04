package com.linkedin.alpini.base.statistics;

import com.linkedin.alpini.base.concurrency.ConcurrentAccumulator;
import com.linkedin.alpini.base.misc.Msg;
import com.linkedin.alpini.base.misc.Time;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class TestQuantileEstimation {
  private static final Logger LOG = LogManager.getLogger(TestQuantileEstimation.class);

  final int windowSize = 100000;
  final double epsilon = 0.001;

  final Long[] values = new Long[windowSize];

  static final ConcurrentAccumulator.Mode DEFAULT_MODE = ConcurrentAccumulator.defaultMode;

  @BeforeClass(groups = "unit")
  public void setupLog4j() {
    // if (!org.apache.log4j.Logger.getRootLogger().getAllAppenders().hasMoreElements()) {
    // org.apache.log4j.BasicConfigurator.configure();
    // }

    Arrays.setAll(values, Long::valueOf);

    Random rand = new Random(0xDEADBEEF);
    Collections.shuffle(Arrays.asList(values), rand);
  }

  @AfterTest(groups = { "unit", "functional" })
  public void resetMode() {
    ConcurrentAccumulator.defaultMode = DEFAULT_MODE;
  }

  private static final Object[][] ACCUMULATOR_MODES =
      { { ConcurrentAccumulator.Mode.COMPLEX }, { ConcurrentAccumulator.Mode.THREADED }, };

  @DataProvider
  public Object[][] accumulatorModes() {
    return ACCUMULATOR_MODES;
  }

  @Test(groups = "unit", dataProvider = "accumulatorModes")
  public void testGenericSinpleQuantileEstimation(ConcurrentAccumulator.Mode mode) {
    ConcurrentAccumulator.defaultMode = mode;

    GenericQuantileEstimation<Long> estimator =
        new GenericQuantileEstimation<>(epsilon, 1000, Comparator.<Long>naturalOrder());

    for (int i = 10; i > 0; i--) {
      Long value = ThreadLocalRandom.current().nextLong(0, Integer.MAX_VALUE);
      long startNanos = Time.nanoTime();
      estimator.accept(value);
      long endNanos = Time.nanoTime();
      LOG.warn(
          "accepted {} samples in {} nanoseconds ({} samples per second)",
          1,
          (endNanos - startNanos),
          TimeUnit.SECONDS.toNanos(1) / (endNanos - startNanos));
    }
  }

  @Test(groups = "unit", dataProvider = "accumulatorModes")
  public void testGenericQuantileEstimation(ConcurrentAccumulator.Mode mode) {
    ConcurrentAccumulator.defaultMode = mode;

    GenericQuantileEstimation<Long> estimator =
        new GenericQuantileEstimation<>(epsilon, 1000, Comparator.<Long>naturalOrder());

    long startNanos = Time.nanoTime();
    Arrays.asList(values).forEach(estimator);
    long endNanos = Time.nanoTime();
    LOG.error(
        "accepted {} samples in {} nanoseconds ({} samples per second)",
        windowSize,
        (endNanos - startNanos),
        windowSize * TimeUnit.SECONDS.toNanos(1) / (endNanos - startNanos));

    double[] quantiles = { 0.5, 0.9, 0.95, 0.99, 1.0 };

    for (double q: quantiles) {
      Long estimate = estimator.query(q);
      long actual = (long) ((q) * (windowSize - 1));

      LOG.error("Estimated {} quantile as {} (actually {})", q, estimate, actual);
    }

    for (Long v: Arrays.asList(1000L, 20000L, 75000L, 99995L, 1000000L)) {
      LOG.error("computed quantile for {} = {}", v, estimator.computeQuantile(v));
    }

    LOG.error("# of samples: {}", estimator.getNumberOfSamples());
  }

  @Test(groups = "unit", dataProvider = "accumulatorModes")
  public void testDoubleQuantileEstimation(ConcurrentAccumulator.Mode mode) {
    ConcurrentAccumulator.defaultMode = mode;

    DoubleQuantileEstimation estimator = new DoubleQuantileEstimation(epsilon, 1000);

    long startNanos = Time.nanoTime();
    Arrays.asList(values).forEach(estimator::accept);
    long endNanos = Time.nanoTime();
    LOG.error(
        "accepted {} samples in {} nanoseconds ({} samples per second)",
        windowSize,
        (endNanos - startNanos),
        windowSize * TimeUnit.SECONDS.toNanos(1) / (endNanos - startNanos));

    double[] quantiles = { 0.5, 0.9, 0.95, 0.99, 1.0 };

    for (double q: quantiles) {
      double estimate = estimator.query(q);
      long actual = (long) ((q) * (windowSize - 1));

      LOG.error("Estimated {} quantile as {} (actually {})", q, estimate, actual);
    }
    LOG.error("# of samples: {}", estimator.getNumberOfSamples());
  }

  @Test(groups = "unit", dataProvider = "accumulatorModes")
  public void testLongQuantileEstimation(ConcurrentAccumulator.Mode mode) {
    ConcurrentAccumulator.defaultMode = mode;

    LongQuantileEstimation estimator = new LongQuantileEstimation(epsilon, 1000);

    long startNanos = Time.nanoTime();
    Arrays.asList(values).forEach(estimator::accept);
    long endNanos = Time.nanoTime();
    LOG.error(
        "accepted {} samples in {} nanoseconds ({} samples per second)",
        windowSize,
        (endNanos - startNanos),
        windowSize * TimeUnit.SECONDS.toNanos(1) / (endNanos - startNanos));

    double[] quantiles = { 0.5, 0.9, 0.95, 0.99, 1.0 };

    for (double q: quantiles) {
      long estimate = estimator.query(q);
      long actual = (long) ((q) * (windowSize - 1));

      LOG.error("Estimated {} quantile as {} (actually {})", q, estimate, actual);
    }
    LOG.error("# of samples: {}", estimator.getNumberOfSamples());
  }

  @Test(groups = "unit", dataProvider = "accumulatorModes")
  public void testLongQuantileArrayEstimation(ConcurrentAccumulator.Mode mode) {
    ConcurrentAccumulator.defaultMode = mode;

    LongQuantileArrayEstimation estimator = new LongQuantileArrayEstimation(epsilon, 1000);

    long startNanos = Time.nanoTime();
    Arrays.asList(values).forEach(estimator::accept);
    long endNanos = Time.nanoTime();
    LOG.error(
        "accepted {} samples in {} nanoseconds ({} samples per second)",
        windowSize,
        (endNanos - startNanos),
        windowSize * TimeUnit.SECONDS.toNanos(1) / (endNanos - startNanos));

    double[] quantiles = { 0.5, 0.9, 0.95, 0.99, 1.0 };

    for (double q: quantiles) {
      long estimate = estimator.query(q);
      long actual = (long) ((q) * (windowSize - 1));

      LOG.error("Estimated {} quantile as {} (actually {})", q, estimate, actual);
    }
    LOG.error("# of samples: {}", estimator.getNumberOfSamples());
  }

  @Test(groups = "unit", dataProvider = "accumulatorModes")
  public void testRealQuantileEstimation(ConcurrentAccumulator.Mode mode) {
    ConcurrentAccumulator.defaultMode = mode;

    DoubleQuantileEstimation estimator = new DoubleQuantileEstimation(epsilon, 1000);

    long startNanos = Time.nanoTime();
    Arrays.asList(values).forEach(estimator::accept);
    long endNanos = Time.nanoTime();
    LOG.error(
        "accepted {} samples in {} nanoseconds ({} samples per second)",
        windowSize,
        (endNanos - startNanos),
        windowSize * TimeUnit.SECONDS.toNanos(1) / (endNanos - startNanos));

    double[] quantiles = { 0.5, 0.9, 0.95, 0.99, 1.0 };

    for (double q: quantiles) {
      double estimate = estimator.query(q);
      long actual = (long) ((q) * (windowSize - 1));

      LOG.error("Estimated {} quantile as {} (actually {})", q, estimate, actual);
    }
    LOG.error("# of samples: {}", estimator.getNumberOfSamples());
  }

  @Test(groups = "unit", dataProvider = "accumulatorModes")
  public void testStatsQuantileEstimation(ConcurrentAccumulator.Mode mode) {
    ConcurrentAccumulator.defaultMode = mode;

    LongStatsAggregator estimator = new LongStatsAggregator(epsilon / 2, 2000);

    long startNanos = Time.nanoTime();
    Arrays.asList(values).forEach(estimator::accept);
    long endNanos = Time.nanoTime();
    LOG.error(
        "accepted {} samples in {} nanoseconds ({} samples per second)",
        windowSize,
        (endNanos - startNanos),
        windowSize * TimeUnit.SECONDS.toNanos(1) / (endNanos - startNanos));

    LOG.error("# of samples: {}", estimator.getNumberOfSamples());

    LongStats stats = estimator.getLongStats();

    LOG.error("Stats: {}", stats.toString());
  }

  @Test(groups = "unit", dataProvider = "accumulatorModes")
  public void testStatsLowQuantileEstimation(ConcurrentAccumulator.Mode mode) {
    ConcurrentAccumulator.defaultMode = mode;

    LongStatsAggregatorLowQuantile estimator = new LongStatsAggregatorLowQuantile(epsilon / 2, 2000);

    long startNanos = Time.nanoTime();
    Arrays.asList(values).forEach(estimator::accept);
    long endNanos = Time.nanoTime();
    LOG.error(
        "accepted {} samples in {} nanoseconds ({} samples per second)",
        windowSize,
        (endNanos - startNanos),
        windowSize * TimeUnit.SECONDS.toNanos(1) / (endNanos - startNanos));

    LOG.error("# of samples: {}", estimator.getNumberOfSamples());

    LongStatsLowQuantile stats = estimator.getLongStatsLowQuantile();

    LOG.error("Stats: {}", stats.toString());
  }

  static class MinMax {
    final long min;
    final long max;

    MinMax(long min, long max) {
      this.min = min;
      this.max = max;
    }

    MinMax apply(long value) {
      return new MinMax(Math.min(min, value), Math.max(max, value));
    }
  }

  @Test(groups = "functional", dataProvider = "accumulatorModes")
  public void testQuantileEstimationMemoryLeak(ConcurrentAccumulator.Mode mode) throws InterruptedException {
    ConcurrentAccumulator.defaultMode = mode;

    final int compactSize = 2000;

    LongAdder count = new LongAdder();
    LongStatsAggregator estimator = new LongStatsAggregator(epsilon / 2, compactSize);

    ForkJoinPool executorService = new ForkJoinPool();
    AtomicReference<MinMax> time = new AtomicReference<>(new MinMax(Long.MAX_VALUE, 0));
    ConcurrentAccumulator<Long, Welfords.LongWelford, Welfords.Result> welfords =
        new ConcurrentAccumulator<>(Welfords.LongWelford.COLLECTOR);
    try {

      // We have to use ForkJoinPool/RecursiveAction because using simpler ThreadPool/Runnable
      // is unable to max out all the cores due to overhead in the ThreadPoolExecutorService.

      class Task extends RecursiveAction {
        private final ThreadLocalRandom rnd = ThreadLocalRandom.current();

        @Override
        protected void compute() {
          run();
          executorService.submit(new Task());
        }

        public void run() {
          long value = rnd.nextLong(100000000);
          long startTime = Time.nanoTime();
          estimator.accept(value);
          long delta = Time.nanoTime() - startTime;
          count.increment();

          MinMax prev = time.get();
          if (prev.max < delta || prev.min > delta) {
            time.updateAndGet(old -> old.apply(delta));
          }

          welfords.accept(delta);
        }
      }

      for (int i = 96; i >= 0; i--) {
        executorService.submit(new Task());
      }

      long lastCount = 0;
      int foo = 0;
      for (long endTime = Time.currentTimeMillis() + TimeUnit.MINUTES.toMillis(5); endTime > Time
          .currentTimeMillis(); Time.sleep(1000)) {
        LOG.error("gather");
        long thisCount = count.longValue();
        MinMax minMax = time.getAndSet(new MinMax(Long.MAX_VALUE, 0));
        long max = minMax.max;
        long min = minMax.min;
        Welfords.LongWelford.Result std = welfords.getThenReset();
        LOG.error(
            "# of samples: {}, calls: {}, delta: {}, max: {}ns, min: {}ns, (count: {}, avg: {}ns, std: {}ns)",
            estimator.getNumberOfSamples(),
            thisCount,
            thisCount - lastCount,
            max,
            min,
            Msg.makeNullable(std, Welfords.Result::getCount),
            Msg.makeNullable(std, s -> (long) s.getAverage()),
            Msg.makeNullable(std, s -> (long) s.getStandardDeviation()));

        LongStats stats = estimator.getLongStats();
        LOG.error("Stats: {}", stats);
        if ((++foo) % 60 == 0) {
          estimator.reset();
        }
        lastCount = thisCount;
      }

    } finally {
      executorService.shutdownNow();
    }

    int numberOfSamples = estimator.getNumberOfSamples();
    LOG.error("# of samples: {}, calls: {}", numberOfSamples, count);

    Assert.assertTrue(numberOfSamples < (count.longValue() / 100));

    LongStats stats = estimator.getLongStats();

    LOG.error("Stats: {}", stats);
  }

  @Test(groups = "functional", dataProvider = "accumulatorModes")
  public void testQuantileArrayEstimationMemoryLeak(ConcurrentAccumulator.Mode mode) throws InterruptedException {
    ConcurrentAccumulator.defaultMode = mode;

    final int compactSize = 2000;

    LongAdder count = new LongAdder();
    LongStatsArrayAggregator estimator = new LongStatsArrayAggregator(epsilon / 2, compactSize);
    LongStatsAggregatorLowQuantile estimatorLQ = new LongStatsAggregatorLowQuantile(epsilon / 2, compactSize);

    ForkJoinPool executorService = new ForkJoinPool();
    AtomicReference<MinMax> time = new AtomicReference<>(new MinMax(Long.MAX_VALUE, 0));
    ConcurrentAccumulator<Double, Welfords.DoubleWelford, Welfords.Result> welfords =
        new ConcurrentAccumulator<>(Welfords.DoubleWelford.COLLECTOR);
    try {

      // We have to use ForkJoinPool/RecursiveAction because using simpler ThreadPool/Runnable
      // is unable to max out all the cores due to overhead in the ThreadPoolExecutorService.

      class Task extends RecursiveAction {
        private final ThreadLocalRandom rnd = ThreadLocalRandom.current();

        @Override
        protected void compute() {
          run();
          executorService.submit(new Task());
        }

        public void run() {
          long value = rnd.nextLong(100000000);
          long startTime = Time.nanoTime();
          estimator.accept(value);
          estimatorLQ.accept(value);
          long delta = Time.nanoTime() - startTime;
          count.increment();

          MinMax prev = time.get();
          if (prev.max < delta || prev.min > delta) {
            time.updateAndGet(old -> old.apply(delta));
          }

          welfords.accept((double) delta);
        }
      }

      for (int i = 96; i >= 0; i--) {
        executorService.submit(new Task());
      }

      long lastCount = 0;
      int foo = 0;
      for (long endTime = Time.currentTimeMillis() + TimeUnit.MINUTES.toMillis(5); endTime > Time
          .currentTimeMillis(); Time.sleep(1000)) {
        LOG.error("gather");
        long thisCount = count.longValue();
        MinMax minMax = time.getAndSet(new MinMax(Long.MAX_VALUE, 0));
        long max = minMax.max;
        long min = minMax.min;
        Welfords.DoubleWelford.Result std = welfords.getThenReset();
        LOG.error(
            "# of samples: estimator: {}, estimatorLQ: {}, calls: {}, delta: {}, max: {}ns, min: {}ns, (count: {}, avg: {}ns, std: {}ns)",
            estimator.getNumberOfSamples(),
            estimatorLQ.getNumberOfSamples(),
            thisCount,
            thisCount - lastCount,
            max,
            min,
            Msg.makeNullable(std, Welfords.Result::getCount),
            Msg.makeNullable(std, s -> (long) s.getAverage()),
            Msg.makeNullable(std, s -> (long) s.getStandardDeviation()));

        LongStats stats = estimator.getLongStats();
        LongStatsLowQuantile statsLQ = estimatorLQ.getLongStatsLowQuantile();
        LOG.error("Stats: {}", stats);
        LOG.error("StatsLQ: {}", statsLQ);
        if ((++foo) % 60 == 0) {
          LOG.error("resetting estimators...");
          estimator.reset();
          estimatorLQ.reset();
        }
        lastCount = thisCount;
      }

    } finally {
      executorService.shutdownNow();
    }

    int numberOfSamples = estimator.getNumberOfSamples();
    int numberOfSamples2 = estimatorLQ.getNumberOfSamples();
    LOG.error("# of samples: estimator: {}, estimatorLQ: {}, calls: {}", numberOfSamples, numberOfSamples2, count);

    Assert.assertTrue(numberOfSamples < (count.longValue() / 100));
    Assert.assertTrue(numberOfSamples2 < (count.longValue() / 100));

    LongStats stats = estimator.getLongStats();
    LongStatsLowQuantile statsLQ = estimatorLQ.getLongStatsLowQuantile();

    LOG.error("Stats: {}", stats);
    LOG.error("StatsLQ: {}", statsLQ);
  }
}
