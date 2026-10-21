package com.linkedin.venice.listener;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.metrics.MetricsRepositoryUtils;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class ServerNettyEventLoopMonitorTest {
  private MetricsRepository metricsRepository;
  private ScheduledExecutorService sampler;
  private SingleThreadEventExecutor firstLoop;
  private SingleThreadEventExecutor secondLoop;
  private List<Runnable> firstProbes;
  private List<Runnable> secondProbes;
  private AtomicLong nanoTime;
  private ServerNettyEventLoopMonitor monitor;

  @BeforeMethod
  public void setUp() {
    metricsRepository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    sampler = mock(ScheduledExecutorService.class);
    firstLoop = mock(SingleThreadEventExecutor.class);
    secondLoop = mock(SingleThreadEventExecutor.class);
    firstProbes = captureProbes(firstLoop);
    secondProbes = captureProbes(secondLoop);
    nanoTime = new AtomicLong();
    monitor = new ServerNettyEventLoopMonitor(group(firstLoop, secondLoop), metricsRepository, sampler, nanoTime::get);
  }

  @AfterMethod
  public void tearDown() {
    monitor.close();
    metricsRepository.close();
  }

  @DataProvider
  public Object[][] backlogs() {
    return new Object[][] { { 3, 7 }, { Integer.MAX_VALUE, Integer.MAX_VALUE } };
  }

  @Test(dataProvider = "backlogs")
  public void testSamplesBacklogAndDelayWithBoundedProbes(int firstPending, int secondPending) {
    when(firstLoop.pendingTasks()).thenReturn(firstPending);
    when(secondLoop.pendingTasks()).thenReturn(secondPending);
    monitor.sample();

    assertEquals(metric("max_thread_number.LambdaStat"), 2.0);
    assertEquals(metric("queued_task_count_gauge.LambdaStat"), (double) firstPending + secondPending);
    assertEquals(metric("max_queued_task_count_gauge.LambdaStat"), (double) Math.max(firstPending, secondPending));
    assertTrue(Double.isNaN(metric("event_loop_scheduling_delay.Avg")));
    for (int i = 0; i < 100; i++) {
      monitor.sample();
    }
    assertEquals(firstProbes.size(), 1);
    assertEquals(secondProbes.size(), 1);
    nanoTime.set(2_500_000);
    firstProbes.get(0).run();
    monitor.sample();
    assertEquals(firstProbes.size(), 2);
    assertEquals(secondProbes.size(), 1);
    nanoTime.set(4 * Time.NS_PER_MS);
    secondProbes.get(0).run();
    assertEquals(metric("event_loop_scheduling_delay.Avg"), 3.25);
    assertEquals(metric("event_loop_scheduling_delay.Max"), 4.0);

    when(firstLoop.pendingTasks()).thenReturn(0);
    when(secondLoop.pendingTasks()).thenReturn(0);
    monitor.sample();
    assertEquals(metric("queued_task_count_gauge.LambdaStat"), 0.0);
    assertEquals(metric("max_queued_task_count_gauge.LambdaStat"), 0.0);
  }

  @Test
  public void testStartAndCloseManageSamplingTask() {
    ScheduledFuture<?> task = mock(ScheduledFuture.class);
    doAnswer(invocation -> task).when(sampler)
        .scheduleWithFixedDelay(any(Runnable.class), eq(0L), eq(1L), eq(TimeUnit.SECONDS));
    monitor.start();
    monitor.start();
    ArgumentCaptor<Runnable> sample = ArgumentCaptor.forClass(Runnable.class);
    verify(sampler).scheduleWithFixedDelay(sample.capture(), eq(0L), eq(1L), eq(TimeUnit.SECONDS));
    sample.getValue().run();
    assertEquals(firstProbes.size(), 1);

    monitor.close();
    monitor.close();
    verify(task).cancel(false);
    verify(sampler).shutdownNow();
    sample.getValue().run();
    firstProbes.get(0).run();
    secondProbes.get(0).run();
    assertEquals(firstProbes.size(), 1);
    assertTrue(Double.isNaN(metric("event_loop_scheduling_delay.Avg")));
    assertThrows(IllegalStateException.class, monitor::start);
  }

  @DataProvider
  public Object[][] shutdownStates() {
    return new Object[][] { { false, false, 2 }, { true, true, 0 }, { false, true, 1 } };
  }

  @Test(dataProvider = "shutdownStates")
  public void testRejectionAndShutdownDoNotBlockOtherLoops(
      boolean shuttingDownBeforeSubmission,
      boolean shuttingDownAfterRejection,
      int expectedAttempts) {
    when(firstLoop.isShuttingDown()).thenReturn(shuttingDownBeforeSubmission, shuttingDownAfterRejection);
    doThrow(new RejectedExecutionException("queue rejected probe")).when(firstLoop).execute(any(Runnable.class));
    monitor.sample();
    monitor.sample();
    verify(firstLoop, times(expectedAttempts)).execute(any(Runnable.class));
    assertTrue(firstProbes.isEmpty());
    assertEquals(secondProbes.size(), 1);
  }

  @Test
  public void testUnsupportedExecutorFailsExplicitly() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new ServerNettyEventLoopMonitor(
            group(mock(EventExecutor.class)),
            metricsRepository,
            sampler,
            nanoTime::get));
  }

  @Test(timeOut = 30000)
  public void testDefaultWorkerGroupSamplesBlockedLoopWithoutAccumulatingProbes() throws Exception {
    NioEventLoopGroup workers = new NioEventLoopGroup(0);
    EventLoop blockedLoop = workers.next();
    MetricsRepository repository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    try (ServerNettyEventLoopMonitor realMonitor =
        new ServerNettyEventLoopMonitor(workers, repository, mock(ScheduledExecutorService.class), nanoTime::get)) {
      assertEquals(
          repository.getMetric(".netty_worker_thread_pool--max_thread_number.LambdaStat").value(),
          (double) workers.executorCount());
      assertTrue(workers.executorCount() > 0);
      blockedLoop.submit(() -> {
        entered.countDown();
        assertTrue(release.await(10, TimeUnit.SECONDS));
        return null;
      });
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      blockedLoop.execute(() -> {});
      blockedLoop.execute(() -> {});
      realMonitor.sample();
      assertEquals(
          repository.getMetric(".netty_worker_thread_pool--max_queued_task_count_gauge.LambdaStat").value(),
          2.0);
      for (int i = 0; i < 10; i++) {
        realMonitor.sample();
        assertEquals(
            repository.getMetric(".netty_worker_thread_pool--max_queued_task_count_gauge.LambdaStat").value(),
            3.0);
      }
      nanoTime.set(5 * Time.NS_PER_MS);
      release.countDown();
      blockedLoop.submit(() -> {}).get(5, TimeUnit.SECONDS);
      assertEquals(repository.getMetric(".netty_worker_thread_pool--event_loop_scheduling_delay.Max").value(), 5.0);
    } finally {
      release.countDown();
      workers.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
      repository.close();
    }
  }

  private List<Runnable> captureProbes(SingleThreadEventExecutor eventLoop) {
    List<Runnable> probes = new ArrayList<>();
    doAnswer(invocation -> {
      probes.add(invocation.getArgument(0));
      return null;
    }).when(eventLoop).execute(any(Runnable.class));
    return probes;
  }

  private EventLoopGroup group(EventExecutor... executors) {
    EventLoopGroup group = mock(EventLoopGroup.class);
    when(group.iterator()).thenAnswer(invocation -> Arrays.asList(executors).iterator());
    return group;
  }

  private double metric(String name) {
    return metricsRepository.getMetric(".netty_worker_thread_pool--" + name).value();
  }
}
