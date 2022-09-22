package com.linkedin.alpini.base.monitoring;

import com.linkedin.alpini.base.concurrency.ConcurrentAccumulator;
import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.base.statistics.Welfords;
import java.util.Arrays;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class TestCallTracker {
  private static final Logger LOG = LogManager.getLogger(TestCallTracker.class);
  static final ConcurrentAccumulator.Mode DEFAULT_MODE = ConcurrentAccumulator.defaultMode;

  @AfterTest(groups = { "unit", "functional" })
  public void resetMode() {
    ConcurrentAccumulator.defaultMode = DEFAULT_MODE;
    CallTrackerImpl.defaultMode = CallTrackerImpl.Mode.SKIP_LIST;
  }

  private static final Object[][] ACCUMULATOR_MODES =
      { { ConcurrentAccumulator.Mode.COMPLEX, CallTrackerImpl.Mode.SKIP_LIST },
          { ConcurrentAccumulator.Mode.THREADED, CallTrackerImpl.Mode.SKIP_LIST },
          { ConcurrentAccumulator.Mode.COMPLEX, CallTrackerImpl.Mode.ARRAY_LIST },
          { ConcurrentAccumulator.Mode.THREADED, CallTrackerImpl.Mode.ARRAY_LIST }, };

  @DataProvider
  public Object[][] accumulatorModes() {
    return ACCUMULATOR_MODES;
  }

  @Test(groups = { "unit", "NoCoverage" }, dataProvider = "accumulatorModes")
  public void testBasic(ConcurrentAccumulator.Mode mode, CallTrackerImpl.Mode stats) {
    testBasic(mode, stats, 30);
  }

  @Test(groups = { "unit", "Coverage" }, dataProvider = "accumulatorModes")
  public void testBasicCoverage(ConcurrentAccumulator.Mode mode, CallTrackerImpl.Mode stats) {
    testBasic(mode, stats, 3);
  }

  private void testBasic(ConcurrentAccumulator.Mode mode, CallTrackerImpl.Mode statsMode, int testDuration) {
    ConcurrentAccumulator.defaultMode = mode;
    CallTrackerImpl.defaultMode = statsMode;

    CallTracker tracker = CallTracker.create();
    Assert.assertEquals(tracker.getTimeSinceLastStartCall(), 0);

    long endTime = Time.nanoTime() + TimeUnit.SECONDS.toNanos(testDuration);

    do {

      for (int i = 0; i < 100000; i++) {
        try (CallCompletion cc = tracker.startCall()) {
          if (i % 97 == 42) {
            cc.closeWithError();
          }
        }
      }

      CallTracker.CallStats stats = tracker.getCallStats();

      System.out.println(stats);

    } while (Time.nanoTime() < endTime);
  }

  @Test(groups = { "unit", "NoCoverage" }, dataProvider = "accumulatorModes")
  public void testBasicDelay(ConcurrentAccumulator.Mode mode, CallTrackerImpl.Mode stats) throws InterruptedException {
    testBasicDelay(mode, stats, 30);
  }

  @Test(groups = { "unit", "Coverage" }, dataProvider = "accumulatorModes")
  public void testBasicDelayCoverage(ConcurrentAccumulator.Mode mode, CallTrackerImpl.Mode stats)
      throws InterruptedException {
    testBasicDelay(mode, stats, 3);
  }

  private void testBasicDelay(ConcurrentAccumulator.Mode mode, CallTrackerImpl.Mode statsMode, int testDuration)
      throws InterruptedException {
    ConcurrentAccumulator.defaultMode = mode;
    CallTrackerImpl.defaultMode = statsMode;

    CallTracker tracker = CallTracker.create();

    long endTime = Time.nanoTime() + TimeUnit.SECONDS.toNanos(testDuration);

    do {

      for (int i = 0; i < 1000; i++) {
        try (CallCompletion cc = tracker.startCall()) {
          Thread.sleep(1);
          if (i % 97 == 42) {
            cc.closeWithError();
          }
        }
      }

      CallTracker.CallStats stats = tracker.getCallStats();

      System.out.println(stats);

    } while (Time.nanoTime() < endTime);
  }

  /*static class MinMax {
    final long min;
    final long max;
  
    MinMax(long min, long max) {
      this.min = min;
      this.max = max;
    }
  
    MinMax apply(long value) {
      return new MinMax(Math.min(min, value), Math.max(max, value));
    }
  }*/

  @Test(groups = "unit", singleThreaded = true, dataProvider = "accumulatorModes")
  public void testConcurrency(ConcurrentAccumulator.Mode mode, CallTrackerImpl.Mode statsMode) {
    ConcurrentAccumulator.defaultMode = mode;
    CallTrackerImpl.defaultMode = statsMode;

    CallTracker tracker = CallTracker.create();

    try {
      Time.freeze();
      tracker.reset();

      Assert.assertEquals(tracker.getMaxConcurrency()[0], 0);
      Assert.assertEquals(tracker.getCurrentStartCountTotal(), 0);

      CallCompletion cc = tracker.startCall();

      Assert.assertEquals(tracker.getCurrentConcurrency(), 1);
      Assert.assertEquals(tracker.getCurrentStartCountTotal(), 1);
      Assert.assertEquals(tracker.getCurrentCallCountTotal(), 0);
      Assert.assertEquals(tracker.getLastResetTime(), Time.currentTimeMillis());

      Time.advance(1, TimeUnit.SECONDS);
      Assert.assertEquals(tracker.getStartFrequency()[0], 1);
      Assert.assertEquals(tracker.getStartCount()[0], 1);

      Time.advance(58, TimeUnit.SECONDS);

      Assert.assertEquals(tracker.getCurrentConcurrency(), 1);
      Assert.assertEquals(tracker.getMaxConcurrency()[0], 1);

      Assert.assertEquals(tracker.getCallStats().getAverageConcurrency1min(), 1.0, 0.1);

      cc.closeCompletion(null, null);

      Assert.assertEquals(tracker.getCurrentConcurrency(), 0);
      Assert.assertEquals(tracker.getCurrentStartCountTotal(), 1);
      Assert.assertEquals(tracker.getCurrentCallCountTotal(), 1);
      Assert.assertEquals(tracker.getCurrentErrorCountTotal(), 0);
      Assert.assertEquals(tracker.getAverageConcurrency()[0], 1.0, 0.1);

      Time.advance(1, TimeUnit.MINUTES);

      Assert.assertEquals(tracker.getAverageConcurrency()[0], 0.0, 0.1);
      Assert.assertEquals(tracker.getAverageConcurrency()[1], 0.2, 0.01);
      Assert.assertEquals(tracker.getStartFrequency()[0], 0);
      Assert.assertEquals(tracker.getStartCount()[0], 0);
      Assert.assertEquals(tracker.getErrorFrequency()[0], 0);
      Assert.assertEquals(tracker.getErrorCount()[0], 0);

      Time.advance(13, TimeUnit.MINUTES);

      Assert.assertEquals(tracker.getMaxConcurrency()[2], 1); // Max concurrency during past 15 minutes

      Time.advance(1, TimeUnit.MINUTES);

      Assert.assertEquals(tracker.getCallStats().getAverageConcurrency5min(), 0.0, 0.001);
      Assert.assertEquals(tracker.getCallStats().getAverageConcurrency15min(), 0.0, 0.001);
      Assert.assertEquals(tracker.getMaxConcurrency()[2], 0);

    } finally {
      Time.restore();
    }
  }

  @Test(groups = "functional", dataProvider = "accumulatorModes")
  public void testHighConcurrency(ConcurrentAccumulator.Mode mode, CallTrackerImpl.Mode statsMode)
      throws InterruptedException {
    ConcurrentAccumulator.defaultMode = mode;
    CallTrackerImpl.defaultMode = statsMode;

    LongAdder count = new LongAdder();
    CallTracker[] callTracker = new CallTracker[] { CallTracker.create(), CallTracker.create(), CallTracker.create(),
        CallTracker.create(), CallTracker.create(), CallTracker.create(), CallTracker.create(), CallTracker.create() };

    ForkJoinPool executorService = new ForkJoinPool();
    // AtomicReference<MinMax> closeTime = new AtomicReference<>(new MinMax(Long.MAX_VALUE, 0));
    ConcurrentAccumulator<Long, Welfords.LongWelford, Welfords.Result> closeWelfords =
        new ConcurrentAccumulator<>(Welfords.LongWelford.COLLECTOR);

    // AtomicReference<MinMax> startTime = new AtomicReference<>(new MinMax(Long.MAX_VALUE, 0));
    ConcurrentAccumulator<Long, Welfords.LongWelford, Welfords.Result> startWelfords =
        new ConcurrentAccumulator<>(Welfords.LongWelford.COLLECTOR);
    try {

      // We have to use ForkJoinPool/RecursiveAction because using simpler ThreadPool/Runnable
      // is unable to max out all the cores due to overhead in the ThreadPoolExecutorService.

      class CloseTask extends RecursiveAction {
        private final CallCompletion _cc;
        private final RecursiveAction _nextStart;

        CloseTask(CallCompletion cc, RecursiveAction nextStart) {
          _cc = cc;
          _nextStart = nextStart;
        }

        @Override
        protected void compute() {
          long time = Time.nanoTime();
          int rnd = ThreadLocalRandom.current().nextInt(97);

          if (rnd == 42) {
            _cc.closeWithError();
          } else {
            _cc.close();
          }
          long closeDelta = Time.nanoTime() - time;
          count.increment();

          // MinMax test = closeTime.get();
          // if (test.max < closeDelta || test.min > closeDelta) {
          // closeTime.updateAndGet(minMax -> minMax.apply(closeDelta));
          // }
          closeWelfords.accept(closeDelta);

          executorService.submit(_nextStart);
        }
      }

      class StartTask extends RecursiveAction {
        @Override
        protected void compute() {
          long time = Time.nanoTime();
          CallCompletion cc = callTracker[ThreadLocalRandom.current().nextInt(callTracker.length)].startCall();
          long startDelta = Time.nanoTime() - time;

          // MinMax test = startTime.get();
          // if (test.max < startDelta || test.min > startDelta) {
          // startTime.updateAndGet(minMax -> minMax.apply(startDelta));
          // }
          startWelfords.accept(startDelta);

          RecursiveAction nextStart = new StartTask();

          executorService.submit(new CloseTask(cc, nextStart));
        }
      }

      final int NUMBER_OF_TASKS = 4800;

      for (int i = NUMBER_OF_TASKS; i > 0; i--) {
        executorService.submit(new StartTask());
      }

      long lastCount = 0;
      int foo = 0;
      for (long endTime = Time.currentTimeMillis() + TimeUnit.MINUTES.toMillis(5); endTime > Time
          .currentTimeMillis(); Time.sleep(10000)) {
        LOG.error("gather");
        long thisCount = count.longValue();
        // MinMax close = closeTime.getAndSet(new MinMax(Long.MAX_VALUE, 0));
        // MinMax start = startTime.getAndSet(new MinMax(Long.MAX_VALUE, 0));
        // long closeMax = close.max;
        // long closeMin = close.min;
        // long startMax = start.max;
        // long startMin = start.min;
        Welfords.LongWelford.Result std = closeWelfords.getThenReset();
        Welfords.LongWelford.Result std2 = startWelfords.getThenReset();
        LOG.error(
            "calls: {}, delta: {}, \r\n" + "close {}\r\n" + "start {}",
            thisCount,
            thisCount - lastCount,
            std,
            std2);

        for (CallTracker ct: callTracker) {
          CallTracker.CallStats stats = ct.getCallStats();
          LOG.error("Stats: {}", stats);
        }
        if ((++foo) % 60 == 0) {
          for (CallTracker ct: callTracker) {
            ct.reset();
          }
        }
        lastCount = thisCount;

        Assert.assertTrue(
            Arrays.stream(callTracker).mapToInt(CallTracker::getCurrentConcurrency).sum() < NUMBER_OF_TASKS);

      }

    } finally {
      executorService.shutdownNow();
    }

    LOG.error("calls: {}", count);

    for (CallTracker ct: callTracker) {
      CallTracker.CallStats stats = ct.getCallStats();
      LOG.error("Stats: {}", stats);
    }
  }

}
