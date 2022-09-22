package com.linkedin.alpini.base.concurrency;

import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.base.statistics.Welfords;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


@Test(groups = "unit")
public class TestConcurrentAccumulator {
  private static final Object[][] ACCUMULATOR_MODES =
      { { ConcurrentAccumulator.Mode.COMPLEX }, { ConcurrentAccumulator.Mode.THREADED }, };

  @DataProvider
  public Object[][] accumulatorModes() {
    return ACCUMULATOR_MODES;
  }

  private void assertThreadMap(ConcurrentAccumulator<?, ?, ?> accumulator, int expect) {
    ThreadedAccumulator threaded = accumulator.unwrap(ThreadedAccumulator.class);
    if (threaded != null) {
      Assert.assertEquals(threaded.threadMapSize(), expect);
    }
  }

  @Test(dataProvider = "accumulatorModes")
  public void basicTest(ConcurrentAccumulator.Mode mode) throws InterruptedException {
    ConcurrentAccumulator<Double, Welfords.DoubleWelford, Welfords.Result> accumulator =
        new ConcurrentAccumulator<>(mode, Welfords.DoubleWelford.COLLECTOR);

    for (int j = 0; j < 2; j++) {
      ForkJoinPool pool = new ForkJoinPool(8);

      try {
        assertThreadMap(accumulator, 1);

        accumulator.reset();

        pool.submit(() -> {
          IntStream.range(1, 5000000).parallel().forEach(i -> {
            accumulator.accept(ThreadLocalRandom.current().nextDouble(100));
          });
        }).join();

        assertThreadMap(accumulator, 9);
        System.out.println(accumulator.getThenReset());
        pool.submit(accumulator::pack).join();

        pool.submit(() -> {
          IntStream.range(1, 5000000).parallel().forEach(i -> {
            accumulator.accept(ThreadLocalRandom.current().nextDouble(100));
          });
        }).join();

        assertThreadMap(accumulator, 9);
        System.out.println(accumulator);
      } finally {
        pool.shutdownNow();
        Assert.assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
      }

      System.gc();
      Time.sleepUninterruptably(1000);
      System.gc();

      System.out.println(accumulator);
      assertThreadMap(accumulator, 1);
    }
  }
}
