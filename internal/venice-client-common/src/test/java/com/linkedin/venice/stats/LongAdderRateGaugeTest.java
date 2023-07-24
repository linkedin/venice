package com.linkedin.venice.stats;

import static org.testng.Assert.assertEquals;

import com.linkedin.venice.utils.TestMockTime;
import com.linkedin.venice.utils.Time;
import io.tehuti.metrics.MetricConfig;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class LongAdderRateGaugeTest {
  private static final TestMockTime TIME = new TestMockTime();
  private static final MetricConfig METRIC_CONFIG = new MetricConfig();

  static class ObjectWithToString {
    @Override
    public String toString() {
      return this.getClass().getSimpleName();
    }
  }

  static class RateExtractor1 extends ObjectWithToString implements Function<LongAdderRateGauge, Double> {
    @Override
    public Double apply(LongAdderRateGauge longAdderRateGauge) {
      return longAdderRateGauge.getRate();
    }
  }

  static class RateExtractor2 extends ObjectWithToString implements Function<LongAdderRateGauge, Double> {
    @Override
    public Double apply(LongAdderRateGauge longAdderRateGauge) {
      return longAdderRateGauge.measure(METRIC_CONFIG, TIME.getMilliseconds());
    }
  }

  static class RateRecorder1 extends ObjectWithToString implements Consumer<LongAdderRateGauge> {
    @Override
    public void accept(LongAdderRateGauge longAdderRateGauge) {
      longAdderRateGauge.record();
    }
  }

  static class RateRecorder2 extends ObjectWithToString implements Consumer<LongAdderRateGauge> {
    @Override
    public void accept(LongAdderRateGauge longAdderRateGauge) {
      longAdderRateGauge.record(1);
    }
  }

  /**
   * There are two APIs to write and two to read, so we test all permutations...
   */
  @DataProvider(name = "Rate-Extractors")
  public static Object[][] trueAndFalseProvider() {
    return new Object[][] { { new RateExtractor1(), new RateRecorder1() },
        { new RateExtractor1(), new RateRecorder2() }, { new RateExtractor2(), new RateRecorder1() },
        { new RateExtractor2(), new RateRecorder2() } };
  }

  @Test(dataProvider = "Rate-Extractors")
  public void test(Function<LongAdderRateGauge, Double> rateExtractor, Consumer<LongAdderRateGauge> rateRecorder)
      throws InterruptedException {
    LongAdderRateGauge larg = new LongAdderRateGauge(TIME);
    assertEquals(rateExtractor.apply(larg), 0.0);
    rateRecorder.accept(larg);
    TIME.addMilliseconds(Time.MS_PER_SECOND * LongAdderRateGauge.RATE_GAUGE_CACHE_DURATION_IN_SECONDS);
    assertEquals(rateExtractor.apply(larg), 1.0 / LongAdderRateGauge.RATE_GAUGE_CACHE_DURATION_IN_SECONDS);
    // Test that the rate is reset or not after the first call
    assertEquals(rateExtractor.apply(larg), 1.0 / LongAdderRateGauge.RATE_GAUGE_CACHE_DURATION_IN_SECONDS);
    int numberOfRunnables = 8;
    Runnable[] runnables = new Runnable[numberOfRunnables];
    int recordCallsPerRunnable = 100;
    for (int i = 0; i < numberOfRunnables; i++) {
      runnables[i] = () -> {
        for (int j = 0; j < recordCallsPerRunnable; j++) {
          rateRecorder.accept(larg);
        }
      };
    }
    ExecutorService executor = Executors.newFixedThreadPool(numberOfRunnables);
    for (int i = 0; i < numberOfRunnables; i++) {
      executor.execute(runnables[i]);
    }
    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.SECONDS);
    TIME.addMilliseconds(Time.MS_PER_SECOND * LongAdderRateGauge.RATE_GAUGE_CACHE_DURATION_IN_SECONDS);
    double expectedRate = numberOfRunnables * recordCallsPerRunnable;
    assertEquals(rateExtractor.apply(larg), expectedRate / LongAdderRateGauge.RATE_GAUGE_CACHE_DURATION_IN_SECONDS);
    TIME.addMilliseconds(Time.MS_PER_SECOND * LongAdderRateGauge.RATE_GAUGE_CACHE_DURATION_IN_SECONDS);
    assertEquals(rateExtractor.apply(larg), 0.0);
    TIME.addMilliseconds(-1);
    assertEquals(rateExtractor.apply(larg), 0.0);
  }
}
