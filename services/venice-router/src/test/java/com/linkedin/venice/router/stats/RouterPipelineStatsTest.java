package com.linkedin.venice.router.stats;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.tehuti.MockTehutiReporter;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import io.tehuti.metrics.MetricsRepository;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class RouterPipelineStatsTest {
  private MetricsRepository metricsRepository;
  private MockTehutiReporter reporter;
  private MultithreadEventLoopGroup mockEventLoopGroup;
  private SingleThreadEventExecutor mockExecutor1;
  private SingleThreadEventExecutor mockExecutor2;
  private AtomicInteger unwritableCount;

  @BeforeMethod
  public void setUp() {
    metricsRepository = new MetricsRepository();
    reporter = new MockTehutiReporter();
    metricsRepository.addReporter(reporter);

    mockExecutor1 = mock(SingleThreadEventExecutor.class);
    mockExecutor2 = mock(SingleThreadEventExecutor.class);
    mockEventLoopGroup = mock(MultithreadEventLoopGroup.class);
    when(mockEventLoopGroup.iterator())
        .thenAnswer(inv -> Arrays.<EventExecutor>asList(mockExecutor1, mockExecutor2).iterator());

    unwritableCount = new AtomicInteger(0);
  }

  @Test
  public void testPreHandlerLatencyPercentiles() {
    RouterPipelineStats stats =
        new RouterPipelineStats(metricsRepository, "router_pipeline", mockEventLoopGroup, unwritableCount::get);

    for (int i = 1; i <= 100; i++) {
      stats.recordPreHandlerLatency(i);
    }

    Assert.assertEquals((int) reporter.query(".router_pipeline--pre_handler_latency.50thPercentile").value(), 50);
    Assert.assertEquals((int) reporter.query(".router_pipeline--pre_handler_latency.95thPercentile").value(), 95);
    Assert.assertEquals((int) reporter.query(".router_pipeline--pre_handler_latency.99thPercentile").value(), 99);
  }

  @Test
  public void testHandlerChainLatencyPercentiles() {
    RouterPipelineStats stats =
        new RouterPipelineStats(metricsRepository, "router_pipeline", mockEventLoopGroup, unwritableCount::get);

    for (int i = 1; i <= 100; i++) {
      stats.recordHandlerChainLatency(i);
    }

    Assert.assertEquals((int) reporter.query(".router_pipeline--handler_chain_latency.50thPercentile").value(), 50);
    Assert.assertEquals((int) reporter.query(".router_pipeline--handler_chain_latency.95thPercentile").value(), 95);
    Assert.assertEquals((int) reporter.query(".router_pipeline--handler_chain_latency.99thPercentile").value(), 99);
  }

  @Test
  public void testEventLoopPendingTasksGauges() {
    when(mockExecutor1.pendingTasks()).thenReturn(10);
    when(mockExecutor2.pendingTasks()).thenReturn(20);

    RouterPipelineStats stats =
        new RouterPipelineStats(metricsRepository, "router_pipeline", mockEventLoopGroup, unwritableCount::get);

    // AsyncGauges are registered - verify they can be queried
    double avg = reporter.query(".router_pipeline--eventloop_pending_tasks_avg.Gauge").value();
    double max = reporter.query(".router_pipeline--eventloop_pending_tasks_max.Gauge").value();
    Assert.assertEquals(avg, 15.0);
    Assert.assertEquals(max, 20.0);
  }

  @Test
  public void testEventLoopPendingTasksAvgWithZeroExecutors() {
    MultithreadEventLoopGroup emptyGroup = mock(MultithreadEventLoopGroup.class);
    when(emptyGroup.iterator()).thenAnswer(inv -> Arrays.<EventExecutor>asList().iterator());

    RouterPipelineStats stats =
        new RouterPipelineStats(metricsRepository, "router_pipeline", emptyGroup, unwritableCount::get);

    double avg = reporter.query(".router_pipeline--eventloop_pending_tasks_avg.Gauge").value();
    Assert.assertEquals(avg, 0.0);
  }

  @Test
  public void testUnwritableChannelCountGauge() {
    unwritableCount.set(5);

    RouterPipelineStats stats =
        new RouterPipelineStats(metricsRepository, "router_pipeline", mockEventLoopGroup, unwritableCount::get);

    double count = reporter.query(".router_pipeline--unwritable_channel_count.Gauge").value();
    Assert.assertEquals(count, 5.0);
  }

  @Test
  public void testUnwritableChannelCountGaugeDynamic() {
    RouterPipelineStats stats =
        new RouterPipelineStats(metricsRepository, "router_pipeline", mockEventLoopGroup, unwritableCount::get);

    Assert.assertEquals(reporter.query(".router_pipeline--unwritable_channel_count.Gauge").value(), 0.0);

    unwritableCount.set(3);
    Assert.assertEquals(reporter.query(".router_pipeline--unwritable_channel_count.Gauge").value(), 3.0);

    unwritableCount.set(0);
    Assert.assertEquals(reporter.query(".router_pipeline--unwritable_channel_count.Gauge").value(), 0.0);
  }
}
