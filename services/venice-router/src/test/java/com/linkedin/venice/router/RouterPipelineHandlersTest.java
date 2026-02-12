package com.linkedin.venice.router;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.alpini.netty4.misc.BasicFullHttpRequest;
import com.linkedin.venice.router.stats.RouterPipelineStats;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import java.util.concurrent.TimeUnit;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RouterPipelineHandlersTest {
  /**
   * Creates a real BasicFullHttpRequest with a specific requestNanos timestamp.
   * Uses POST method since PipelineTimingHandler only measures non-GET (multiget/compute) requests.
   * We use real objects because getRequestNanos() is a final method that cannot be mocked.
   */
  private static BasicFullHttpRequest createPostRequest(long requestNanos) {
    return new BasicFullHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.POST,
        "/test",
        System.currentTimeMillis(),
        requestNanos);
  }

  private static BasicFullHttpRequest createGetRequest(long requestNanos) {
    return new BasicFullHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.GET,
        "/test",
        System.currentTimeMillis(),
        requestNanos);
  }

  // --- PipelineTimingHandler tests ---

  @Test
  public void testPipelineTimingHandlerRecordsPreHandlerLatency() throws Exception {
    RouterPipelineStats mockStats = mock(RouterPipelineStats.class);
    RouterServer.PipelineTimingHandler handler = new RouterServer.PipelineTimingHandler(mockStats);

    // Use EmbeddedChannel for real channel attribute support
    EmbeddedChannel channel = new EmbeddedChannel();
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    when(ctx.channel()).thenReturn(channel);
    when(ctx.fireChannelRead(any())).thenReturn(ctx);

    long requestNanos = System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(50);
    BasicFullHttpRequest request = createPostRequest(requestNanos);

    handler.channelRead(ctx, request);

    // Verify pre_handler_latency was recorded with reasonable value
    ArgumentCaptor<Double> latencyCaptor = ArgumentCaptor.forClass(Double.class);
    verify(mockStats).recordPreHandlerLatency(latencyCaptor.capture());
    double recorded = latencyCaptor.getValue();
    Assert.assertTrue(recorded >= 40, "Expected >= 40ms but got: " + recorded);

    // Verify HANDLER_CHAIN_START_NANOS attribute was set
    Long startNanos = channel.attr(RouterServer.PipelineTimingHandler.HANDLER_CHAIN_START_NANOS).get();
    Assert.assertNotNull(startNanos, "HANDLER_CHAIN_START_NANOS attribute should be set");

    // Verify LAST_CHECKPOINT_NANOS attribute was also set (for per-handler checkpoints)
    Long checkpointNanos = channel.attr(RouterServer.PipelineTimingHandler.LAST_CHECKPOINT_NANOS).get();
    Assert.assertNotNull(checkpointNanos, "LAST_CHECKPOINT_NANOS attribute should be set");
    Assert.assertEquals(checkpointNanos, startNanos, "Both attributes should have the same value");

    // Verify message was forwarded
    verify(ctx).fireChannelRead(request);

    request.release();
  }

  @Test
  public void testPipelineTimingHandlerSkipsGetRequests() throws Exception {
    RouterPipelineStats mockStats = mock(RouterPipelineStats.class);
    RouterServer.PipelineTimingHandler handler = new RouterServer.PipelineTimingHandler(mockStats);

    EmbeddedChannel channel = new EmbeddedChannel();
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    when(ctx.channel()).thenReturn(channel);
    when(ctx.fireChannelRead(any())).thenReturn(ctx);

    long requestNanos = System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(50);
    BasicFullHttpRequest request = createGetRequest(requestNanos);

    handler.channelRead(ctx, request);

    // Should NOT record any latency for GET requests (single-get)
    verify(mockStats, never()).recordPreHandlerLatency(any(Double.class));

    // Attributes should NOT be set for GET requests
    Long startNanos = channel.attr(RouterServer.PipelineTimingHandler.HANDLER_CHAIN_START_NANOS).get();
    Assert.assertNull(startNanos, "HANDLER_CHAIN_START_NANOS should not be set for GET requests");
    Long checkpointNanos = channel.attr(RouterServer.PipelineTimingHandler.LAST_CHECKPOINT_NANOS).get();
    Assert.assertNull(checkpointNanos, "LAST_CHECKPOINT_NANOS should not be set for GET requests");

    // Message should still be forwarded
    verify(ctx).fireChannelRead(request);

    request.release();
  }

  @Test
  public void testPipelineTimingHandlerIgnoresNonBasicFullHttpRequest() throws Exception {
    RouterPipelineStats mockStats = mock(RouterPipelineStats.class);
    RouterServer.PipelineTimingHandler handler = new RouterServer.PipelineTimingHandler(mockStats);

    EmbeddedChannel channel = new EmbeddedChannel();
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    when(ctx.channel()).thenReturn(channel);
    when(ctx.fireChannelRead(any())).thenReturn(ctx);

    Object nonHttpMsg = new Object();
    handler.channelRead(ctx, nonHttpMsg);

    // Should not record any latency for non-BasicFullHttpRequest messages
    verify(mockStats, never()).recordPreHandlerLatency(any(Double.class));

    // Attributes should not be set
    Long startNanos = channel.attr(RouterServer.PipelineTimingHandler.HANDLER_CHAIN_START_NANOS).get();
    Assert.assertNull(startNanos, "HANDLER_CHAIN_START_NANOS should not be set for non-HTTP messages");
    Long checkpointNanos = channel.attr(RouterServer.PipelineTimingHandler.LAST_CHECKPOINT_NANOS).get();
    Assert.assertNull(checkpointNanos, "LAST_CHECKPOINT_NANOS should not be set for non-HTTP messages");

    // Message should still be forwarded
    verify(ctx).fireChannelRead(nonHttpMsg);
  }

  // --- PipelineTimingEndHandler tests ---

  @Test
  public void testPipelineTimingEndHandlerRecordsHandlerChainLatency() throws Exception {
    RouterPipelineStats mockStats = mock(RouterPipelineStats.class);
    RouterServer.PipelineTimingEndHandler handler = new RouterServer.PipelineTimingEndHandler(mockStats);

    EmbeddedChannel channel = new EmbeddedChannel();
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    when(ctx.channel()).thenReturn(channel);
    when(ctx.fireChannelRead(any())).thenReturn(ctx);

    // Pre-set attributes (simulating PipelineTimingHandler having run)
    long startNanos = System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(25);
    channel.attr(RouterServer.PipelineTimingHandler.HANDLER_CHAIN_START_NANOS).set(startNanos);
    channel.attr(RouterServer.PipelineTimingHandler.LAST_CHECKPOINT_NANOS).set(startNanos);

    Object msg = new Object();
    handler.channelRead(ctx, msg);

    // Verify handler_chain_latency was recorded
    ArgumentCaptor<Double> latencyCaptor = ArgumentCaptor.forClass(Double.class);
    verify(mockStats).recordHandlerChainLatency(latencyCaptor.capture());
    double recorded = latencyCaptor.getValue();
    Assert.assertTrue(recorded >= 20, "Expected >= 20ms but got: " + recorded);

    // Verify HANDLER_CHAIN_START_NANOS was cleared
    Long remainingNanos = channel.attr(RouterServer.PipelineTimingHandler.HANDLER_CHAIN_START_NANOS).get();
    Assert.assertNull(remainingNanos, "HANDLER_CHAIN_START_NANOS should be cleared after recording");
    Long remainingCheckpoint = channel.attr(RouterServer.PipelineTimingHandler.LAST_CHECKPOINT_NANOS).get();
    Assert.assertNull(remainingCheckpoint, "LAST_CHECKPOINT_NANOS should be cleared after recording");

    // Verify HANDLER_CHAIN_END_NANOS was set (for dispatch gap measurement)
    Long endNanos = channel.attr(RouterServer.PipelineTimingHandler.HANDLER_CHAIN_END_NANOS).get();
    Assert.assertNotNull(endNanos, "HANDLER_CHAIN_END_NANOS should be set for dispatch gap measurement");

    // Verify message was forwarded
    verify(ctx).fireChannelRead(msg);
  }

  @Test
  public void testPipelineTimingEndHandlerSkipsWhenNoAttribute() throws Exception {
    RouterPipelineStats mockStats = mock(RouterPipelineStats.class);
    RouterServer.PipelineTimingEndHandler handler = new RouterServer.PipelineTimingEndHandler(mockStats);

    EmbeddedChannel channel = new EmbeddedChannel();
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    when(ctx.channel()).thenReturn(channel);
    when(ctx.fireChannelRead(any())).thenReturn(ctx);

    Object msg = new Object();
    handler.channelRead(ctx, msg);

    // Should not record latency when attribute is not set
    verify(mockStats, never()).recordHandlerChainLatency(any(Double.class));

    // Message should still be forwarded
    verify(ctx).fireChannelRead(msg);
  }

  @Test
  public void testTimingHandlersEndToEndWithCheckpoints() throws Exception {
    RouterPipelineStats mockStats = mock(RouterPipelineStats.class);
    RouterServer.PipelineTimingHandler startHandler = new RouterServer.PipelineTimingHandler(mockStats);
    RouterServer.HandlerTimingCheckpoint checkpoint1 = new RouterServer.HandlerTimingCheckpoint(mockStats, "handler_a");
    RouterServer.HandlerTimingCheckpoint checkpoint2 = new RouterServer.HandlerTimingCheckpoint(mockStats, "handler_b");
    RouterServer.PipelineTimingEndHandler endHandler = new RouterServer.PipelineTimingEndHandler(mockStats);

    // Use same EmbeddedChannel for all handlers to share attributes
    EmbeddedChannel channel = new EmbeddedChannel();

    ChannelHandlerContext startCtx = mock(ChannelHandlerContext.class);
    when(startCtx.channel()).thenReturn(channel);
    when(startCtx.fireChannelRead(any())).thenReturn(startCtx);

    ChannelHandlerContext cp1Ctx = mock(ChannelHandlerContext.class);
    when(cp1Ctx.channel()).thenReturn(channel);
    when(cp1Ctx.fireChannelRead(any())).thenReturn(cp1Ctx);

    ChannelHandlerContext cp2Ctx = mock(ChannelHandlerContext.class);
    when(cp2Ctx.channel()).thenReturn(channel);
    when(cp2Ctx.fireChannelRead(any())).thenReturn(cp2Ctx);

    ChannelHandlerContext endCtx = mock(ChannelHandlerContext.class);
    when(endCtx.channel()).thenReturn(channel);
    when(endCtx.fireChannelRead(any())).thenReturn(endCtx);

    long requestNanos = System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(100);
    BasicFullHttpRequest request = createPostRequest(requestNanos);

    // Simulate full pipeline: start → checkpoint1 → checkpoint2 → end
    startHandler.channelRead(startCtx, request);
    verify(mockStats).recordPreHandlerLatency(any(Double.class));

    checkpoint1.channelRead(cp1Ctx, request);
    verify(mockStats).recordHandlerLatency(ArgumentMatchers.eq("handler_a"), any(Double.class));

    checkpoint2.channelRead(cp2Ctx, request);
    verify(mockStats).recordHandlerLatency(ArgumentMatchers.eq("handler_b"), any(Double.class));

    endHandler.channelRead(endCtx, request);
    verify(mockStats).recordHandlerChainLatency(any(Double.class));

    // Verify both attributes were cleaned up
    Long remaining = channel.attr(RouterServer.PipelineTimingHandler.HANDLER_CHAIN_START_NANOS).get();
    Assert.assertNull(remaining, "HANDLER_CHAIN_START_NANOS should be cleared after end handler runs");
    Long remainingCheckpoint = channel.attr(RouterServer.PipelineTimingHandler.LAST_CHECKPOINT_NANOS).get();
    Assert.assertNull(remainingCheckpoint, "LAST_CHECKPOINT_NANOS should be cleared after end handler runs");

    request.release();
  }

  // --- HandlerTimingCheckpoint tests ---

  @Test
  public void testHandlerTimingCheckpointRecordsLatency() throws Exception {
    RouterPipelineStats mockStats = mock(RouterPipelineStats.class);
    RouterServer.HandlerTimingCheckpoint checkpoint = new RouterServer.HandlerTimingCheckpoint(mockStats, "throttle");

    EmbeddedChannel channel = new EmbeddedChannel();
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    when(ctx.channel()).thenReturn(channel);
    when(ctx.fireChannelRead(any())).thenReturn(ctx);

    // Set LAST_CHECKPOINT_NANOS to simulate PipelineTimingHandler having run
    long checkpointNanos = System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(30);
    channel.attr(RouterServer.PipelineTimingHandler.LAST_CHECKPOINT_NANOS).set(checkpointNanos);

    Object msg = new Object();
    checkpoint.channelRead(ctx, msg);

    // Verify per-handler latency was recorded with correct handler name
    ArgumentCaptor<Double> latencyCaptor = ArgumentCaptor.forClass(Double.class);
    verify(mockStats).recordHandlerLatency(ArgumentMatchers.eq("throttle"), latencyCaptor.capture());
    double recorded = latencyCaptor.getValue();
    Assert.assertTrue(recorded >= 25, "Expected >= 25ms but got: " + recorded);

    // Verify LAST_CHECKPOINT_NANOS was updated to a new (more recent) value
    Long updatedNanos = channel.attr(RouterServer.PipelineTimingHandler.LAST_CHECKPOINT_NANOS).get();
    Assert.assertNotNull(updatedNanos, "LAST_CHECKPOINT_NANOS should be updated");
    Assert.assertTrue(updatedNanos > checkpointNanos, "LAST_CHECKPOINT_NANOS should be advanced");

    // Verify message was forwarded
    verify(ctx).fireChannelRead(msg);
  }

  @Test
  public void testHandlerTimingCheckpointSkipsWhenNoAttribute() throws Exception {
    RouterPipelineStats mockStats = mock(RouterPipelineStats.class);
    RouterServer.HandlerTimingCheckpoint checkpoint = new RouterServer.HandlerTimingCheckpoint(mockStats, "throttle");

    EmbeddedChannel channel = new EmbeddedChannel();
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    when(ctx.channel()).thenReturn(channel);
    when(ctx.fireChannelRead(any())).thenReturn(ctx);

    // Do NOT set LAST_CHECKPOINT_NANOS
    Object msg = new Object();
    checkpoint.channelRead(ctx, msg);

    // Should not record any latency
    verify(mockStats, never()).recordHandlerLatency(any(String.class), any(Double.class));

    // Message should still be forwarded
    verify(ctx).fireChannelRead(msg);
  }

  @Test
  public void testHandlerTimingCheckpointChaining() throws Exception {
    RouterPipelineStats mockStats = mock(RouterPipelineStats.class);
    RouterServer.HandlerTimingCheckpoint cp1 = new RouterServer.HandlerTimingCheckpoint(mockStats, "health_check");
    RouterServer.HandlerTimingCheckpoint cp2 = new RouterServer.HandlerTimingCheckpoint(mockStats, "ssl_verify");

    EmbeddedChannel channel = new EmbeddedChannel();

    ChannelHandlerContext ctx1 = mock(ChannelHandlerContext.class);
    when(ctx1.channel()).thenReturn(channel);
    when(ctx1.fireChannelRead(any())).thenReturn(ctx1);

    ChannelHandlerContext ctx2 = mock(ChannelHandlerContext.class);
    when(ctx2.channel()).thenReturn(channel);
    when(ctx2.fireChannelRead(any())).thenReturn(ctx2);

    // Set initial checkpoint
    long initialNanos = System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(20);
    channel.attr(RouterServer.PipelineTimingHandler.LAST_CHECKPOINT_NANOS).set(initialNanos);

    Object msg = new Object();

    // First checkpoint records health_check latency
    cp1.channelRead(ctx1, msg);
    verify(mockStats).recordHandlerLatency(ArgumentMatchers.eq("health_check"), any(Double.class));

    // Capture the updated checkpoint nanos after cp1
    Long afterCp1 = channel.attr(RouterServer.PipelineTimingHandler.LAST_CHECKPOINT_NANOS).get();
    Assert.assertNotNull(afterCp1);
    Assert.assertTrue(afterCp1 > initialNanos, "Checkpoint should have advanced after first handler");

    // Second checkpoint records ssl_verify latency (measured from cp1's timestamp)
    cp2.channelRead(ctx2, msg);
    verify(mockStats).recordHandlerLatency(ArgumentMatchers.eq("ssl_verify"), any(Double.class));

    Long afterCp2 = channel.attr(RouterServer.PipelineTimingHandler.LAST_CHECKPOINT_NANOS).get();
    Assert.assertNotNull(afterCp2);
    Assert.assertTrue(afterCp2 >= afterCp1, "Checkpoint should have advanced after second handler");
  }

  @Test
  public void testHandlerTimingCheckpointGetHandlerName() {
    RouterPipelineStats mockStats = mock(RouterPipelineStats.class);
    RouterServer.HandlerTimingCheckpoint checkpoint = new RouterServer.HandlerTimingCheckpoint(mockStats, "admin");
    Assert.assertEquals(checkpoint.getHandlerName(), "admin");
  }

  // --- WritabilityMonitorHandler tests ---

  @Test
  public void testWritabilityMonitorIncrementOnUnwritable() throws Exception {
    RouterServer.WritabilityMonitorHandler handler = new RouterServer.WritabilityMonitorHandler();
    Assert.assertEquals(handler.getUnwritableCount(), 0);

    Channel mockChannel = mock(Channel.class);
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    when(ctx.channel()).thenReturn(mockChannel);
    when(ctx.fireChannelWritabilityChanged()).thenReturn(ctx);
    when(mockChannel.isWritable()).thenReturn(false);

    handler.channelWritabilityChanged(ctx);
    Assert.assertEquals(handler.getUnwritableCount(), 1);

    // Verify event was forwarded
    verify(ctx).fireChannelWritabilityChanged();
  }

  @Test
  public void testWritabilityMonitorDecrementOnWritable() throws Exception {
    RouterServer.WritabilityMonitorHandler handler = new RouterServer.WritabilityMonitorHandler();

    Channel mockChannel = mock(Channel.class);
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    when(ctx.channel()).thenReturn(mockChannel);
    when(ctx.fireChannelWritabilityChanged()).thenReturn(ctx);

    // First: channel becomes unwritable
    when(mockChannel.isWritable()).thenReturn(false);
    handler.channelWritabilityChanged(ctx);
    Assert.assertEquals(handler.getUnwritableCount(), 1);

    // Then: channel becomes writable again
    when(mockChannel.isWritable()).thenReturn(true);
    handler.channelWritabilityChanged(ctx);
    Assert.assertEquals(handler.getUnwritableCount(), 0);
  }

  @Test
  public void testWritabilityMonitorMultipleChannels() throws Exception {
    RouterServer.WritabilityMonitorHandler handler = new RouterServer.WritabilityMonitorHandler();

    // Channel 1 becomes unwritable
    Channel ch1 = mock(Channel.class);
    ChannelHandlerContext ctx1 = mock(ChannelHandlerContext.class);
    when(ctx1.channel()).thenReturn(ch1);
    when(ctx1.fireChannelWritabilityChanged()).thenReturn(ctx1);
    when(ch1.isWritable()).thenReturn(false);
    handler.channelWritabilityChanged(ctx1);

    // Channel 2 becomes unwritable
    Channel ch2 = mock(Channel.class);
    ChannelHandlerContext ctx2 = mock(ChannelHandlerContext.class);
    when(ctx2.channel()).thenReturn(ch2);
    when(ctx2.fireChannelWritabilityChanged()).thenReturn(ctx2);
    when(ch2.isWritable()).thenReturn(false);
    handler.channelWritabilityChanged(ctx2);

    Assert.assertEquals(handler.getUnwritableCount(), 2);

    // Channel 1 becomes writable
    when(ch1.isWritable()).thenReturn(true);
    handler.channelWritabilityChanged(ctx1);
    Assert.assertEquals(handler.getUnwritableCount(), 1);

    // Channel 2 goes inactive while still unwritable
    when(ctx2.fireChannelInactive()).thenReturn(ctx2);
    when(ch2.isWritable()).thenReturn(false);
    handler.channelInactive(ctx2);
    Assert.assertEquals(handler.getUnwritableCount(), 0);

    verify(ctx2).fireChannelInactive();
  }

  @Test
  public void testWritabilityMonitorInactiveWritableChannelDoesNotDecrement() throws Exception {
    RouterServer.WritabilityMonitorHandler handler = new RouterServer.WritabilityMonitorHandler();

    Channel mockChannel = mock(Channel.class);
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    when(ctx.channel()).thenReturn(mockChannel);
    when(ctx.fireChannelInactive()).thenReturn(ctx);
    when(mockChannel.isWritable()).thenReturn(true);

    // Channel goes inactive while writable - should NOT decrement
    handler.channelInactive(ctx);
    Assert.assertEquals(handler.getUnwritableCount(), 0);

    verify(ctx).fireChannelInactive();
  }

  @Test
  public void testWritabilityMonitorSharableAcrossChannels() throws Exception {
    // Verify the @Sharable handler works correctly when shared across many channels
    RouterServer.WritabilityMonitorHandler handler = new RouterServer.WritabilityMonitorHandler();

    // Simulate rapid writability changes from multiple channels
    for (int i = 0; i < 10; i++) {
      Channel ch = mock(Channel.class);
      ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
      when(ctx.channel()).thenReturn(ch);
      when(ctx.fireChannelWritabilityChanged()).thenReturn(ctx);
      when(ch.isWritable()).thenReturn(false);
      handler.channelWritabilityChanged(ctx);
    }
    Assert.assertEquals(handler.getUnwritableCount(), 10);

    // All become writable
    for (int i = 0; i < 10; i++) {
      Channel ch = mock(Channel.class);
      ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
      when(ctx.channel()).thenReturn(ch);
      when(ctx.fireChannelWritabilityChanged()).thenReturn(ctx);
      when(ch.isWritable()).thenReturn(true);
      handler.channelWritabilityChanged(ctx);
    }
    Assert.assertEquals(handler.getUnwritableCount(), 0);
  }
}
