package com.linkedin.alpini.netty4.pool;

import static com.linkedin.alpini.netty4.pool.Http2AwareChannelPool.*;
import static org.mockito.Mockito.*;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http2.EspressoHttp2MultiplexHandler;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2FrameCodec;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import io.netty.handler.codec.http2.Http2Stream;
import io.netty.handler.codec.http2.Http2StreamChannel;
import io.netty.util.concurrent.CompleteFuture;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestHttp2AwareChannelPool {
  private final AtomicInteger _streamId = new AtomicInteger(1);

  private final EventExecutor _executor = ImmediateEventExecutor.INSTANCE;

  private static final Consumer<Channel> CONSUME_NOTHING = c -> {};

  private static class BlahException extends RuntimeException {
  }

  private Future<Channel> makeFuture(boolean successfulAcquire, boolean isHttp2) {
    return makeFuture(successfulAcquire, isHttp2, false);
  }

  private Future<Channel> makeFuture(boolean successfulAcquire, boolean isHttp2, boolean reuse) {

    return new CompleteFuture<Channel>(_executor) {
      EmbeddedChannel _embeddedChannel;

      @Override
      public boolean isSuccess() {
        return successfulAcquire;
      }

      @Override
      public Throwable cause() {
        return successfulAcquire ? null : new BlahException();
      }

      @Override
      public Channel getNow() {
        if (_embeddedChannel != null) {
          return _embeddedChannel;
        }
        EmbeddedChannel channel = new EmbeddedChannel();
        ChannelHandler channelHandler = Mockito.mock(ChannelHandler.class);
        if (isHttp2) {
          channel.pipeline().addLast(Http2FrameCodecBuilder.forClient().build());
          ChannelHandler multiplexHandler = reuse
              ? new EspressoHttp2MultiplexHandler(channelHandler, true, false)
              : new Http2MultiplexHandler(channelHandler);
          channel.pipeline().addLast(multiplexHandler);
          _embeddedChannel = channel;
        }
        return channel;
      }
    };
  }

  private ManagedChannelPool prepareAClosedParentPool() {
    ManagedChannelPool parentPool = mock(ManagedChannelPool.class);
    ChannelPoolHandler channelPoolHandler = mock(ChannelPoolHandler.class);
    when(parentPool.handler()).thenReturn(channelPoolHandler);

    Future<Channel> successfulAcquire = makeFuture(true, true);
    Future<Channel> failedFuture = makeFuture(false, true);
    when(parentPool.acquire()).thenReturn(successfulAcquire).thenReturn(failedFuture);
    when(parentPool.isClosed()).thenReturn(true);
    when(parentPool.name()).thenReturn("closedPool");

    return parentPool;
  }

  private ManagedChannelPool prepareParentPool(boolean successfulAcquire, boolean isHttp2) {
    return prepareParentPool(successfulAcquire, isHttp2, false);
  }

  private ManagedChannelPool prepareParentPool(boolean successfulAcquire, boolean isHttp2, boolean reuse) {
    Future<Channel> future = makeFuture(successfulAcquire, isHttp2, reuse);
    ManagedChannelPool parentPool = parentPool();
    when(parentPool.acquire()).thenReturn(future);
    when(parentPool.name()).thenReturn("pool");
    return parentPool;
  }

  private ManagedChannelPool parentPool() {
    ManagedChannelPool parentPool = mock(ManagedChannelPool.class);
    ChannelPoolHandler channelPoolHandler = mock(ChannelPoolHandler.class);
    when(parentPool.handler()).thenReturn(channelPoolHandler);
    when(parentPool.name()).thenReturn("pool");
    return parentPool;
  }

  /**
   * Return a mock parent pool that will return successful acquire future until the latch is 0.
   * @param latch Latch to use
   * @return Mock parent pool
   */
  private ManagedChannelPool prepareParentPoolWithCountDown(CountDownLatch latch) {
    Future<Channel> future = makeFuture(true, true, true);
    Future<Channel> failedFuture = makeFuture(false, true, true);
    ManagedChannelPool parentPool = parentPool();
    when(parentPool.acquire()).thenAnswer(invocation -> latch.getCount() == 0 ? failedFuture : future);
    return parentPool;
  }

  private Http2Stream createStream(Channel ch) throws Exception {
    Channel channel = ch instanceof Http2StreamChannel ? ch.parent() : ch;
    Assert.assertTrue(channel.hasAttr(HTTP2_CONNECTION));
    return channel.attr(HTTP2_CONNECTION).get().local().createStream(_streamId.getAndAdd(2), false);
  }

  /**
   * Creates a wrapped {@link Http2AwareChannelPool} that will create a new H2 stream when acquire succeeds,
   * and close the corresponding H2 stream when release succeeds. This is for simulating the active streams in the
   * underlying HTTP/2 connection.
   *
   * @param parentPool Parent pool
   * @return Wrapped H2 channel pool
   */
  private Http2AwareChannelPool createHttp2AwareChannelPool(ManagedChannelPool parentPool) {
    return createHttp2AwareChannelPool(parentPool, new HashMap<>(), f -> {});
  }

  private Http2AwareChannelPool createHttp2AwareChannelPool(
      ManagedChannelPool parentPool,
      Map<Channel, Http2Stream> streams,
      GenericFutureListener<Future<Channel>> acquire0Listener) {
    return new Http2AwareChannelPool(parentPool, CONSUME_NOTHING, CONSUME_NOTHING) {
      @Override
      public Future<Channel> acquire() {
        Future<Channel> future = super.acquire();
        future.addListener((Future<Channel> f) -> {
          if (f.isSuccess()) {
            streams.put(f.getNow(), createStream(f.getNow()));
          }
        });
        return future;
      }

      @Override
      protected Promise<Channel> acquire0(Promise<Channel> promise) {
        return super.acquire0(promise).addListener(acquire0Listener);
      }

      @Override
      public Future<Void> release(Channel channel) {
        Future<Void> future = super.release(channel);
        future.addListener(f -> {
          if (f.isSuccess()) {
            streams.remove(channel).close();
          }
        });
        return future;
      }
    };
  }

  private static void assertActiveStreams(ManagedChannelPool pool, int expectedCount) {
    Assert.assertEquals(pool.getTotalActiveStreams(), expectedCount, "Netty active stream count should be correct");
    Assert.assertEquals(
        pool.getTotalActiveStreamChannels(),
        expectedCount,
        "Active stream channel counter should be correct");
  }

  @Test(groups = "unit")
  public void testAcquireWithPromiseSucceedForHttp2AndReturnAnHttp2StreamChannel() throws Exception {
    ManagedChannelPool parentPool = prepareParentPool(true, true);
    Http2AwareChannelPool pool = createHttp2AwareChannelPool(parentPool);
    pool.setMoreThanOneHttp2Connection(false);
    Future<Channel> future = pool.acquire();
    Channel channel = future.sync().getNow();
    Assert.assertNotNull(channel);
    Assert.assertTrue(channel instanceof Http2StreamChannel, channel.getClass().toString());
    assertActiveStreams(pool, 1);
    verify(parentPool, times(1)).release(any(Channel.class));
    verify(parentPool, times(1)).acquire();
    verify(parentPool, times(1)).handler();
    verify(parentPool, atLeastOnce()).name();
    Assert.assertTrue(future.isSuccess());
    verifyNoMoreInteractions(parentPool);
    pool.release(channel);
    assertActiveStreams(pool, 0);
    Assert.assertEquals(pool.getTotalStreamCreations(), 1, "Should increment stream creation count");
  }

  @Test(groups = "unit")
  public void testTotalActiveStreams() throws Exception {
    ManagedChannelPool parentPool = prepareParentPool(true, true);
    Http2AwareChannelPool pool = createHttp2AwareChannelPool(parentPool);
    pool.setMoreThanOneHttp2Connection(false);
    Future<Channel> future = pool.acquire();
    Channel channel1 = future.sync().getNow();
    assertActiveStreams(pool, 1);
    Assert.assertNotNull(channel1);
    Assert.assertTrue(channel1 instanceof Http2StreamChannel);
    verify(parentPool, times(1)).release(any(Channel.class));
    verify(parentPool, times(1)).acquire();
    verify(parentPool, times(1)).handler();
    verify(parentPool, atLeastOnce()).name();
    Assert.assertTrue(future.isSuccess());
    verifyNoMoreInteractions(parentPool);

    Channel channel2 = pool.acquire().sync().getNow();
    Assert.assertNotNull(channel2);
    Assert.assertTrue(channel2 instanceof Http2StreamChannel);
    assertActiveStreams(pool, 2);
    Assert.assertEquals(pool.getTotalStreamCreations(), 2, "Should increment stream creation count");

    pool.release(channel1);
    assertActiveStreams(pool, 1);
    pool.release(channel2);
    assertActiveStreams(pool, 0);
  }

  @Test(groups = "unit")
  public void testAcquireWithPromiseFailedWithClosedParentPool() throws InterruptedException {
    // The the parentPool is mocked in such a way that it would return a succeeded future first then failed as second.
    ManagedChannelPool parentPool = prepareAClosedParentPool();
    Http2AwareChannelPool pool = new Http2AwareChannelPool(parentPool, CONSUME_NOTHING, CONSUME_NOTHING);
    pool.setMoreThanOneHttp2Connection(true);
    Future<Channel> future = pool.acquire();
    Channel channel = future.sync().getNow();
    Assert.assertNotNull(channel);
    Assert.assertTrue(channel instanceof Http2StreamChannel);
    verify(parentPool, times(1)).release(any(Channel.class));
    verify(parentPool, times(1)).release(any(Channel.class));
    verify(parentPool, times(1)).isClosed();
    Assert.assertEquals(pool.getTotalStreamCreations(), 1, "Should increment stream creation count");
    Assert.assertTrue(future.isSuccess());
  }

  @Test(groups = "unit")
  public void testAcquireWithPromiseSucceed2ForHttp2AndReturnAnHttp2StreamChannel() throws Exception {
    ManagedChannelPool parentPool = prepareParentPool(true, true);
    Http2AwareChannelPool pool = createHttp2AwareChannelPool(parentPool);
    pool.setMoreThanOneHttp2Connection(true);
    Future<Channel> future = pool.acquire();
    Channel channel = future.sync().getNow();
    Assert.assertNotNull(channel);
    Assert.assertTrue(channel instanceof Http2StreamChannel);
    assertActiveStreams(pool, 1);
    verify(parentPool, times(2)).release(any(Channel.class));
    verify(parentPool, times(2)).acquire();
    verify(parentPool, times(1)).handler();
    verify(parentPool, times(1)).isClosing();
    verify(parentPool, atLeastOnce()).name();
    Assert.assertEquals(pool.getTotalStreamCreations(), 1, "Should increment stream creation count");
    Assert.assertTrue(future.isSuccess());
    verifyNoMoreInteractions(parentPool);
  }

  @Test(groups = "unit")
  public void testAcquireWithPromiseSucceedForHttp1AndReturnAChannel() throws InterruptedException {
    ManagedChannelPool parentPool = prepareParentPool(true, false);
    doAnswer(invocation -> {
      Promise<Void> promise = invocation.getArgument(1);
      return promise.setSuccess(null);
    }).when(parentPool).release(any(), any());
    Http2AwareChannelPool pool = new Http2AwareChannelPool(parentPool, CONSUME_NOTHING, CONSUME_NOTHING);
    Future<Channel> future = pool.acquire();
    Channel channel = future.sync().getNow();
    Assert.assertNotNull(channel);
    Assert.assertFalse(channel instanceof Http2StreamChannel);
    Assert.assertTrue(channel instanceof EmbeddedChannel);
    Assert.assertEquals(pool.getTotalStreamCreations(), 0, "Should not increment creation count since not H2");
    verify(parentPool, times(0)).release(any(Channel.class));
    Assert.assertTrue(future.isSuccess());
    pool.release(channel).sync();
  }

  @Test(groups = "unit")
  public void testAcquireWithPromiseSucceedForHttp1AndReturnAChannelTwice() throws InterruptedException {
    ManagedChannelPool parentPool = prepareParentPool(true, false);
    Http2AwareChannelPool pool = new Http2AwareChannelPool(parentPool, CONSUME_NOTHING, CONSUME_NOTHING);
    Future<Channel> future = pool.acquire();
    Channel channel = future.sync().getNow();
    Assert.assertNotNull(channel);
    Assert.assertFalse(channel instanceof Http2StreamChannel);
    Assert.assertTrue(channel instanceof EmbeddedChannel);
    verify(parentPool, times(0)).release(any(Channel.class));
    Assert.assertTrue(future.isSuccess());

    Future<Channel> future2 = pool.acquire();
    Channel channel2 = future2.sync().getNow();
    Assert.assertNotNull(channel2);
    Assert.assertFalse(channel2 instanceof Http2StreamChannel);
    Assert.assertTrue(channel2 instanceof EmbeddedChannel);
    Assert.assertEquals(pool.getTotalStreamCreations(), 0, "Should not increment creation count since not H2");
    verify(parentPool, times(0)).release(any(Channel.class));
    Assert.assertTrue(future2.isSuccess());
    Assert.assertNotSame(channel, channel2, "In HTTP 1.1 we should expect two different parent channels");
  }

  @Test(groups = "unit")
  public void testAcquireWithPromiseFailedForHttp2() throws InterruptedException {
    ManagedChannelPool parentPool = prepareParentPool(false, true);
    Http2AwareChannelPool pool = new Http2AwareChannelPool(parentPool, CONSUME_NOTHING, CONSUME_NOTHING);
    pool.setRetryOnMaxStreamsLimit(true);
    Future<Channel> future = null;
    Channel channel = null;
    try {
      future = pool.acquire();
      channel = future.sync().getNow();
      Assert.fail("Should throw exception");
    } catch (BlahException bl) {
      // expected.
    }
    Assert.assertNull(channel);
    assertActiveStreams(pool, 0);
    Assert.assertEquals(pool.getTotalStreamCreations(), 0, "Should not increment creation count since creation failed");
    Assert.assertEquals(pool.getTotalAcquireRetries(), 0, "Should not retry on BlahException");
    verify(parentPool, times(1)).acquire();
    verify(parentPool, times(0)).release(any(Channel.class));
    Assert.assertFalse(future.isSuccess());
    Assert.assertTrue(future.cause() instanceof BlahException);
  }

  @Test(groups = "unit")
  public void testAcquireWithPromiseFailedForHttp1() throws InterruptedException {
    ManagedChannelPool parentPool = prepareParentPool(false, false);
    Http2AwareChannelPool pool = new Http2AwareChannelPool(parentPool, CONSUME_NOTHING, CONSUME_NOTHING);
    pool.setRetryOnMaxStreamsLimit(true);
    Future<Channel> future = null;
    Channel channel = null;
    try {
      future = pool.acquire();
      channel = future.sync().getNow();
      Assert.fail("Should throw exception");
    } catch (BlahException bl) {
      // expected.
    }
    Assert.assertNull(channel);
    Assert.assertEquals(pool.getTotalAcquireRetries(), 0, "Should not retry on BlahException");
    verify(parentPool, times(1)).acquire();
    verify(parentPool, times(0)).release(any(Channel.class));
    Assert.assertFalse(future.isSuccess());
    Assert.assertTrue(future.cause() instanceof BlahException);
    Assert.assertEquals(pool.getTotalStreamCreations(), 0, "Should not increment creation count since not H2");
  }

  @Test(groups = "unit")
  public void testHttp2ChannelReuse() throws Exception {
    ManagedChannelPool parentPool = prepareParentPool(true, true, true);
    Http2AwareChannelPool pool = createHttp2AwareChannelPool(parentPool);
    pool.setChannelReuse(true);
    pool.setMoreThanOneHttp2Connection(false);
    pool.setUseCustomH2Codec(true);
    Assert.assertTrue(pool.useCustomH2Codec());

    // Acquire one stream channel
    Channel channel = firstAcquire(pool, parentPool);

    pool.release(channel);
    assertActiveStreams(pool, 0);
    Assert.assertEquals(pool.getChannelReusePoolSize(), 1, "Should recycle channels to the deque");

    // Acquire another channel, this should be the same channel because of the reuse
    Future<Channel> future = pool.acquire();
    Channel anotherChannel = future.sync().getNow();
    assertActiveStreams(pool, 1);
    assertOccupiedStreamChannel(anotherChannel);
    Assert.assertSame(channel, anotherChannel, "Both channels should be the same due to the reuse");
    anotherChannel.attr(HTTP2_STREAM_CHANNEL_AVAILABLE_FOR_REUSE).set(Boolean.TRUE);
    verify(parentPool, times(2)).release(any(Channel.class));
    verify(parentPool, times(2)).acquire();
    verify(parentPool, times(3)).handler();
    Assert.assertTrue(future.isSuccess());
    verifyNoMoreInteractions(parentPool);
    Assert.assertEquals(pool.getTotalStreamChannelsReused(), 1, "Should have reused one channel");
    Assert.assertEquals(pool.getTotalStreamCreations(), 1, "Should not increment missed count since reusing");

    pool.release(anotherChannel);
    assertActiveStreams(pool, 0);
    Assert.assertEquals(pool.getChannelReusePoolSize(), 1, "Should recycle channels to the deque");
    Assert.assertEquals(pool.getTotalStreamChannelsReused(), 1, "Should have 1 reuse after release");
    Assert.assertEquals(pool.getCurrentStreamChannelsReused(), 0, "Should have 0 current reuses after release");

    anotherChannel.parent().close().getNow();
    Assert.assertFalse(channel.isOpen());
    Assert.assertFalse(anotherChannel.isOpen());
    Assert.assertEquals(pool.getChannelReusePoolSize(), 0, "Should clear recycle queues after parent channel closed");
  }

  @Test(groups = "unit")
  public void testHttp2ChannelReuseSequence() throws Exception {
    ManagedChannelPool parentPool = prepareParentPool(true, true, true);
    Http2AwareChannelPool pool = createHttp2AwareChannelPool(parentPool);
    pool.setChannelReuse(true);
    pool.setMoreThanOneHttp2Connection(false);
    pool.setUseCustomH2Codec(true);
    Assert.assertTrue(pool.useCustomH2Codec());

    // Acquire one stream channel
    Channel channel = firstAcquire(pool, parentPool);

    // Acquire another channel, this should be the a different channel since the first one is not released
    Future<Channel> anotherFuture = pool.acquire();
    Channel anotherChannel = anotherFuture.sync().getNow();
    assertActiveStreams(pool, 2);
    Assert.assertNotNull(anotherChannel);
    Assert.assertTrue(anotherChannel instanceof Http2StreamChannel);
    anotherChannel.attr(HTTP2_STREAM_CHANNEL_AVAILABLE_FOR_REUSE).set(Boolean.TRUE);
    verify(parentPool, times(2)).release(any(Channel.class));
    verify(parentPool, times(2)).acquire();
    verify(parentPool, times(2)).handler();
    Assert.assertTrue(anotherFuture.isSuccess());
    verifyNoMoreInteractions(parentPool);
    // Release the second channel, it should be reused later
    pool.release(anotherChannel);
    assertActiveStreams(pool, 1);
    Assert.assertEquals(pool.getChannelReusePoolSize(), 1, "Should recycle channel to the deque");
    Assert.assertEquals(pool.getTotalStreamChannelsReused(), 0, "Should have 0 reuses");
    Assert.assertEquals(pool.getTotalStreamCreations(), 2, "Should increment creation count");

    // Acquire the third channel, should be the same as the second channel due to reuse
    Future<Channel> thirdFuture = pool.acquire();
    Channel thirdChannel = thirdFuture.sync().getNow();
    assertActiveStreams(pool, 2);
    Assert.assertNotNull(thirdChannel);
    Assert.assertTrue(thirdChannel instanceof Http2StreamChannel);
    Assert.assertSame(anotherChannel, thirdChannel, "Both channels should be the same due to the reuse");
    thirdChannel.attr(HTTP2_STREAM_CHANNEL_AVAILABLE_FOR_REUSE).set(Boolean.TRUE);
    verify(parentPool, times(3)).release(any(Channel.class));
    verify(parentPool, times(3)).acquire();
    verify(parentPool, times(4)).handler();
    Assert.assertTrue(thirdFuture.isSuccess());
    verifyNoMoreInteractions(parentPool);
    Assert.assertEquals(pool.getTotalStreamChannelsReused(), 1, "Should have reused one channel");
    Assert.assertEquals(pool.getTotalStreamCreations(), 2, "Should not increment count since reusing");

    pool.release(channel);
    pool.release(thirdChannel);
    assertActiveStreams(pool, 0);
    Assert.assertEquals(pool.getChannelReusePoolSize(), 2, "Should recycle channels to the deque");
    Assert.assertEquals(pool.getTotalStreamChannelsReused(), 1, "Should have 1 reuse after release");
    Assert.assertEquals(pool.getCurrentStreamChannelsReused(), 0, "Should have 0 current reuses after release");
    Assert.assertTrue(channel.isOpen());
    Assert.assertTrue(thirdChannel.isOpen());
  }

  @Test(groups = "unit")
  public void testHttp2ChannelReuseLimit() throws Exception {
    ManagedChannelPool parentPool = prepareParentPool(true, true, true);
    Http2AwareChannelPool pool = createHttp2AwareChannelPool(parentPool);
    pool.setChannelReuse(true);
    pool.setMoreThanOneHttp2Connection(false);
    pool.setUseCustomH2Codec(true);
    pool.setMaxReuseStreamChannelsLimit(1);
    Assert.assertTrue(pool.useCustomH2Codec());
    Assert.assertEquals(pool.getMaxReuseStreamChannelsLimit(), 1);

    // Acquire one stream channel
    Channel channel = firstAcquire(pool, parentPool);

    // Acquire another channel, this should be a different channel since the first one is not released
    Future<Channel> anotherFuture = pool.acquire();
    Channel anotherChannel = anotherFuture.sync().getNow();
    assertActiveStreams(pool, 2);
    Assert.assertNotNull(anotherChannel);
    Assert.assertTrue(anotherChannel instanceof Http2StreamChannel);
    anotherChannel.attr(HTTP2_STREAM_CHANNEL_AVAILABLE_FOR_REUSE).set(Boolean.TRUE);
    verify(parentPool, times(2)).release(any(Channel.class));
    verify(parentPool, times(2)).acquire();
    verify(parentPool, times(2)).handler();
    Assert.assertTrue(anotherFuture.isSuccess());
    verifyNoMoreInteractions(parentPool);
    // Release the second channel, it should be reused later
    pool.release(anotherChannel);
    assertActiveStreams(pool, 1);
    Assert.assertEquals(pool.getChannelReusePoolSize(), 1, "Should recycle channel to the deque");
    Assert.assertEquals(pool.getTotalStreamCreations(), 2, "Should increment stream creation count");
    Assert.assertTrue(anotherChannel.isOpen(), "Stream channel added back to queue, should not close");

    pool.release(channel);
    assertActiveStreams(pool, 0);
    Assert.assertEquals(
        pool.getChannelReusePoolSize(),
        1,
        "Should not recycle channel to the deque since we hit the size limit");
    Assert.assertEquals(pool.getTotalStreamChannelsReused(), 0);
    Assert.assertFalse(channel.isOpen(), "Should close the channel since recycle queue full");
  }

  @Test(groups = "unit")
  public void testHttp2ActiveStreamLimit() throws Exception {
    ManagedChannelPool parentPool = prepareParentPool(true, true, true);
    Http2AwareChannelPool pool = createHttp2AwareChannelPool(parentPool);
    pool.setChannelReuse(true);
    pool.setMoreThanOneHttp2Connection(false);
    pool.setUseCustomH2Codec(true);
    pool.setMaxConcurrentStreams(1);
    Assert.assertTrue(pool.useCustomH2Codec());

    // Acquire one stream channel
    Channel channel = firstAcquire(pool, parentPool);

    Future<Channel> anotherFuture = pool.acquire();
    Assert.assertFalse(anotherFuture.isSuccess());
    Assert
        .assertTrue(anotherFuture.cause() instanceof Http2Exception, "Acquire should fail due to active stream limit");
    Assert.assertEquals(((Http2Exception) anotherFuture.cause()).error(), Http2Error.REFUSED_STREAM);
    Assert.assertTrue(
        anotherFuture.cause().getMessage().contains("Reached maxConcurrentStreamsLimit=1, totalActiveStream=1"));
    Assert.assertEquals(pool.getActiveStreamsLimitReachedCount(), 1);

    pool.release(channel);
    assertActiveStreams(pool, 0);
    Assert.assertEquals(pool.getChannelReusePoolSize(), 1, "Should recycle channels to the deque");

    // After the channel is released, acquire should work again
    Future<Channel> future = pool.acquire();
    Channel anotherChannel = future.sync().getNow();
    assertActiveStreams(pool, 1);
    assertOccupiedStreamChannel(anotherChannel);
    Assert.assertSame(channel, anotherChannel, "Both channels should be the same due to the reuse");
    anotherChannel.attr(HTTP2_STREAM_CHANNEL_AVAILABLE_FOR_REUSE).set(Boolean.TRUE);
    verify(parentPool, times(3)).release(any(Channel.class));
    verify(parentPool, times(3)).acquire();
    verify(parentPool, times(3)).handler();
    Assert.assertTrue(future.isSuccess());
    verifyNoMoreInteractions(parentPool);
    Assert.assertEquals(pool.getTotalStreamChannelsReused(), 1, "Should have reused one channel");
    Assert.assertEquals(pool.getTotalStreamCreations(), 1);

    pool.release(anotherChannel);
    assertActiveStreams(pool, 0);
    Assert.assertEquals(pool.getChannelReusePoolSize(), 1, "Should recycle channels to the deque");
    Assert.assertEquals(pool.getTotalStreamChannelsReused(), 1, "Should have 1 reuse after release");
    Assert.assertEquals(pool.getCurrentStreamChannelsReused(), 0, "Should have 0 current reuses after release");

    pool.close();
  }

  @Test(groups = "unit")
  public void testHttp2ActiveStreamLimitWithRetry() throws Exception {
    ManagedChannelPool parentPool = prepareParentPool(true, true, true);
    Http2AwareChannelPool pool = createHttp2AwareChannelPool(parentPool);
    pool.setChannelReuse(true);
    pool.setMoreThanOneHttp2Connection(false);
    pool.setUseCustomH2Codec(true);
    pool.setMaxConcurrentStreams(1);
    pool.setRetryOnMaxStreamsLimit(true);

    // Acquire one stream channel
    Channel channel = firstAcquire(pool, parentPool);

    Future<Channel> anotherFuture = pool.acquire();
    Assert.assertFalse(anotherFuture.isSuccess());
    Assert
        .assertTrue(anotherFuture.cause() instanceof Http2Exception, "Acquire should fail due to active stream limit");
    Assert.assertEquals(((Http2Exception) anotherFuture.cause()).error(), Http2Error.REFUSED_STREAM);
    Assert.assertEquals(pool.getActiveStreamsLimitReachedCount(), 3);
    Assert.assertEquals(pool.getTotalAcquireRetries(), 2);

    pool.release(channel);
    assertActiveStreams(pool, 0);
    Assert.assertEquals(pool.getChannelReusePoolSize(), 1, "Should recycle channels to the deque");

    // After the channel is released, acquire should work again
    Future<Channel> future = pool.acquire();
    Channel anotherChannel = future.sync().getNow();
    assertActiveStreams(pool, 1);
    assertOccupiedStreamChannel(anotherChannel);
    Assert.assertSame(channel, anotherChannel, "Both channels should be the same due to the reuse");
    anotherChannel.attr(HTTP2_STREAM_CHANNEL_AVAILABLE_FOR_REUSE).set(Boolean.TRUE);
    verify(parentPool, times(5)).release(any(Channel.class));
    verify(parentPool, times(5)).acquire();
    verify(parentPool, times(3)).handler();
    Assert.assertTrue(future.isSuccess());
    verifyNoMoreInteractions(parentPool);
    Assert.assertEquals(pool.getTotalStreamChannelsReused(), 1, "Should have reused one channel");
    Assert.assertEquals(pool.getTotalStreamCreations(), 1);

    pool.release(anotherChannel);
    assertActiveStreams(pool, 0);
    Assert.assertEquals(pool.getChannelReusePoolSize(), 1, "Should recycle channels to the deque");
    Assert.assertEquals(pool.getTotalStreamChannelsReused(), 1, "Should have 1 reuse after release");
    Assert.assertEquals(pool.getCurrentStreamChannelsReused(), 0, "Should have 0 current reuses after release");

    pool.close();

    // Test when acquire retry succeeded on the second attempt
    parentPool = prepareParentPool(true, true, true);
    Map<Channel, Http2Stream> streams = new HashMap<>();
    pool = createHttp2AwareChannelPool(parentPool, streams, f -> {
      if (!streams.isEmpty()) {
        streams.values().iterator().next().close();
      }
    });
    pool.setChannelReuse(true);
    pool.setMoreThanOneHttp2Connection(false);
    pool.setUseCustomH2Codec(true);
    pool.setMaxConcurrentStreams(1);
    pool.setRetryOnMaxStreamsLimit(true);

    channel = firstAcquire(pool, parentPool);
    future = pool.acquire();
    anotherChannel = future.sync().getNow();
    Assert.assertEquals(pool.getTotalActiveStreams(), 1);
    Assert.assertEquals(pool.getTotalActiveStreamChannels(), 2);
    assertOccupiedStreamChannel(anotherChannel);
    Assert.assertNotEquals(channel, anotherChannel, "Should create new channel since first one not released");
    anotherChannel.attr(HTTP2_STREAM_CHANNEL_AVAILABLE_FOR_REUSE).set(Boolean.TRUE);
    verify(parentPool, times(3)).release(any(Channel.class));
    verify(parentPool, times(3)).acquire();
    verify(parentPool, times(2)).handler();
    Assert.assertTrue(future.isSuccess());
    verifyNoMoreInteractions(parentPool);
    Assert.assertEquals(pool.getTotalStreamChannelsReused(), 0, "Should not have reused channels");
    Assert.assertEquals(pool.getTotalStreamCreations(), 2);

    pool.release(channel);
    pool.release(anotherChannel);
    assertActiveStreams(pool, 0);
    Assert.assertEquals(pool.getChannelReusePoolSize(), 2, "Should recycle channels to the deque");
    pool.close();

    // Test when acquire retry failed with other failures
    CountDownLatch latch = new CountDownLatch(2);
    parentPool = prepareParentPoolWithCountDown(latch);
    pool = createHttp2AwareChannelPool(parentPool, new HashMap<>(), f -> latch.countDown());
    pool.setChannelReuse(true);
    pool.setMoreThanOneHttp2Connection(false);
    pool.setMaxConcurrentStreams(1);
    pool.setRetryOnMaxStreamsLimit(true);

    channel = firstAcquire(pool, parentPool);

    anotherFuture = pool.acquire();
    Assert.assertFalse(anotherFuture.isSuccess());
    Assert.assertTrue(anotherFuture.cause() instanceof BlahException);
    Assert.assertEquals(pool.getActiveStreamsLimitReachedCount(), 1);
    Assert.assertEquals(pool.getTotalAcquireRetries(), 1);

    pool.release(channel);
    assertActiveStreams(pool, 0);
    Assert.assertEquals(pool.getChannelReusePoolSize(), 1, "Should recycle channels to the deque");
    pool.close();
  }

  private void assertOccupiedStreamChannel(Channel anotherChannel) {
    Assert.assertNotNull(anotherChannel);
    Assert.assertTrue(anotherChannel instanceof Http2StreamChannel);
    Assert.assertNotEquals(
        anotherChannel.attr(HTTP2_STREAM_CHANNEL_AVAILABLE_FOR_REUSE).get(),
        Boolean.TRUE,
        "Should set reuse to false since occupied");
  }

  private Channel firstAcquire(Http2AwareChannelPool pool, ManagedChannelPool parentPool) throws Exception {
    Future<Channel> future = pool.acquire();
    Channel channel = future.sync().getNow();
    channel.attr(HTTP2_STREAM_CHANNEL_AVAILABLE_FOR_REUSE).set(Boolean.TRUE);
    assertActiveStreams(pool, 1);
    Assert.assertNotNull(channel);
    Assert.assertTrue(channel instanceof Http2StreamChannel);
    verify(parentPool, times(1)).release(any(Channel.class));
    verify(parentPool, times(1)).acquire();
    verify(parentPool, times(1)).handler();
    verify(parentPool, atLeastOnce()).name();
    Assert.assertTrue(future.isSuccess());
    verifyNoMoreInteractions(parentPool);
    Assert.assertEquals(pool.getTotalStreamCreations(), 1, "Should increment creation count");
    Assert.assertEquals(pool.getActiveStreamsLimitReachedCount(), 0);
    Assert.assertEquals(pool.getTotalAcquireRetries(), 0);
    return channel;
  }

  @Test(groups = "unit")
  public void testH2ActiveConnections() throws InterruptedException {
    ManagedChannelPool parentPool = prepareParentPool(true, true, true);
    Http2AwareChannelPool pool = new Http2AwareChannelPool(parentPool, CONSUME_NOTHING, CONSUME_NOTHING);
    pool.setChannelReuse(true);
    pool.setMoreThanOneHttp2Connection(false);
    pool.setUseCustomH2Codec(true);

    // no active h2 tcp connections and no active streams
    Assert.assertEquals(pool.getH2ActiveConnections(), -1);

    // The tcp connection is created but the stream it has is not activated
    Future<Channel> future = pool.acquire();
    Channel channel = future.sync().getNow();
    Assert.assertNotNull(channel.parent().pipeline().get(Http2FrameCodec.class).connection());
    Assert.assertEquals(pool.getH2ActiveConnections(), 0);

    // activate the stream, then the h2 tcp connection containing the active stream is active
    channel.writeAndFlush(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/foo/bar/0/21"));
    Assert.assertEquals(pool.getH2ActiveConnections(), 1);
  }
}
