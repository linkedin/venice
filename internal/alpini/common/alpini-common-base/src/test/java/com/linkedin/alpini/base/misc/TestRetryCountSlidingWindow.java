package com.linkedin.alpini.base.misc;

import com.linkedin.alpini.base.concurrency.Executors;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * @author solu
 * Date: 4/27/21
 */
@Test(groups = "unit")
public class TestRetryCountSlidingWindow {
  static final Logger LOG = LogManager.getLogger(TestRetryCountSlidingWindow.class);
  private final int _ioWorkersCount = 64;
  private final double _error = 0.005;
  private final int _ioWorkerUpdateIntervalMs = 20;
  // Each ioWorker would put 1000 requests in every 20 ms == 100 * (1000/20) = 5K/s for one ioWorker.
  // The total QPS would be 5 K * 64 = 320,000 /s.
  final int _totalCount = 100;
  final int _initRetryCut = 20;
  final int _secondRetryCut = 40;

  int _windowLen = 5;
  int _runningSeconds = 20;
  ScheduledExecutorService _shutdown;

  @BeforeClass
  public void beforeClass() {
    _shutdown = Executors.newSingleThreadScheduledExecutor();
  }

  @AfterClass
  public void afterClass() {
    _shutdown.shutdownNow();
  }

  public void testAtSteadyRate() throws InterruptedException {
    ScheduledExecutorService updater = Executors.newScheduledThreadPool(1);
    RetryCountSlidingWindow w = new RetryCountSlidingWindow(1000, _windowLen, updater);
    updater.shutdown();

    ScheduledExecutorService ioWorkers = Executors.newScheduledThreadPool(_ioWorkersCount);
    ScheduledExecutorService resultCollector = Executors.newScheduledThreadPool(1);
    AtomicInteger cut = new AtomicInteger(_initRetryCut);

    for (int i = 0; i < _ioWorkersCount; i++) {
      ioWorkers.scheduleAtFixedRate(() -> {
        // every time putting 10 request with 2 being retried
        // so the retryRatio should be 20%
        CounterQueue<RetryCounter> q = w.getQueue();
        // LOG.error("Fill the queue by {}", Thread.currentThread().getName());
        for (int j = 0; j < _totalCount; j++) {
          q.increaseCount(j < cut.get());
        }
      }, 0, _ioWorkerUpdateIntervalMs, TimeUnit.MILLISECONDS);
    }

    List<Result> results = new LinkedList<>();
    retrieveRatioDuringRun(w, cut, _totalCount, resultCollector, results);
    scheduleShutdown(ioWorkers, resultCollector, _runningSeconds);

    for (Result r: results) {
      Assert.assertEquals(
          r.actual,
          r.expected,
          _error,
          String.format("ActualRatio is %f is too much away from the expected %f", r.actual, r.expected));
    }

    updater.shutdownNow();
  }

  public void testAtVariantRate() throws InterruptedException {
    ScheduledExecutorService updater = Executors.newScheduledThreadPool(1);
    RetryCountSlidingWindow w = new RetryCountSlidingWindow(1000, _windowLen, updater);

    ScheduledExecutorService ioWorkers = Executors.newScheduledThreadPool(_ioWorkersCount);
    ScheduledExecutorService resultCollector = Executors.newScheduledThreadPool(1);
    AtomicInteger cut = new AtomicInteger(_initRetryCut);

    final long startSecond = RetryCounter.getCurrentSecond();

    for (int i = 0; i < _ioWorkersCount; i++) {
      ioWorkers.scheduleAtFixedRate(() -> {
        // every time putting 10 request with 2 being retried
        // so the retryRatio should be 20%
        CounterQueue<RetryCounter> q = w.getQueue();
        if (RetryCounter.getCurrentSecond() - startSecond >= _runningSeconds / 2) {
          // change the cut in the half way
          cut.set(_secondRetryCut);
        }
        // LOG.error("Fill the queue by {}", Thread.currentThread().getName());
        for (int j = 0; j < _totalCount; j++) {
          q.increaseCount(j < cut.get());
        }
      }, 0, _ioWorkerUpdateIntervalMs, TimeUnit.MILLISECONDS);
    }

    List<Result> results = new LinkedList<>();
    retrieveRatioDuringRun(w, cut, _totalCount, resultCollector, results);
    scheduleShutdown(ioWorkers, resultCollector, _runningSeconds);

    for (Result r: results) {
      double initExpectedRatio = (double) _initRetryCut / _totalCount;
      if (Math.abs(r.expected - initExpectedRatio) < _error) {
        Assert.assertEquals(
            r.actual,
            r.expected,
            _error,
            String.format("ActualRatio is %f is too much away from the expected %f", r.actual, r.expected));
      } else {
        // now the ratio should be different and gradually increased from 0.2 to 0.4 or less.
        Assert.assertTrue(
            r.actual >= initExpectedRatio - _error && r.actual <= r.expected + _error,
            String.format(
                "ActualRatio is %f is not between expected range: %f --> %f",
                r.actual,
                initExpectedRatio,
                r.expected));
      }
    }
    updater.shutdownNow();
  }

  private void scheduleShutdown(
      ScheduledExecutorService ioWorkers,
      ScheduledExecutorService collector,
      int runningSeconds) throws InterruptedException {
    _shutdown.schedule(() -> {
      ioWorkers.shutdownNow();
      collector.shutdownNow();

    }, runningSeconds, TimeUnit.SECONDS);

    ioWorkers.awaitTermination(runningSeconds, TimeUnit.SECONDS);
    collector.awaitTermination(runningSeconds, TimeUnit.SECONDS);

  }

  private void retrieveRatioDuringRun(
      RetryCountSlidingWindow w,
      AtomicInteger cut,
      int totalCount,
      ScheduledExecutorService collector,
      List<Result> results) {
    collector.scheduleWithFixedDelay(() -> {
      double expected = (double) cut.get() / (double) totalCount;
      String s = String.format(
          "Expected retry ratio: %f and actual %f, retryCount: %d, total: %d",
          expected,
          w.getRetryRatio(),
          w.getRetryCount(),
          w.getTotalCount());
      // LOG.error(s);
      if (w.getTotalCount() > 0) {
        results.add(new Result(s, expected, w.getRetryRatio()));
      }

    }, 100, 1000, TimeUnit.MILLISECONDS);
  }

  private static class Result {
    String s;
    double expected;
    double actual;

    public Result(String s, double expected, double actual) {
      this.s = s;
      this.expected = expected;
      this.actual = actual;
    }
  }

}
