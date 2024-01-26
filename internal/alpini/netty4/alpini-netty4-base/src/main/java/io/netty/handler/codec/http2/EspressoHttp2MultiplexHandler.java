package io.netty.handler.codec.http2;

import static io.netty.handler.codec.http2.Http2Error.INTERNAL_ERROR;
import static io.netty.handler.codec.http2.Http2Exception.connectionError;

import com.linkedin.alpini.netty4.misc.Http2Utils;
import com.linkedin.alpini.netty4.misc.NettyUtils;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
import io.netty.channel.ServerChannel;
import io.netty.handler.codec.http2.Http2FrameCodec.DefaultHttp2FrameStream;
import io.netty.util.AttributeKey;
import io.netty.util.internal.ObjectUtil;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Forked from Netty's Http2MultiplexHandler (4.1.42)
 * (https://github.com/netty/netty/blob/4.1/codec-http2/src/main/java/io/netty/handler/codec/http2/Http2MultiplexHandler.java)
 *
 * In HTTP/1.1, the requests from the client are distributed across 48 workers threads of router.
 * With HTTP/2 there will be only connection between the client -&gt; router, all the requests for a given client are executed on a single I/O worker thread.
 * If there are less than 48 client to a given router, not all threads will be utilized.
 *
 * To mitigate this, this rb forks Netty's multiplex handler and registers the child channels on different I/O workers.
 * This will help distribute the work on to different threads.
 *
 * Design document - https://iwww.corp.linkedin.com/wiki/cf/pages/viewpage.action?pageId=323757534
 *
 * @author Abhishek Andhavarapu
 */
public final class EspressoHttp2MultiplexHandler extends Http2ChannelDuplexHandler {
  private static final AttributeKey<Boolean> REMOTE_RST_STREAM_RECEIVED_KEY =
      AttributeKey.valueOf(EspressoHttp2MultiplexHandler.class, "remoteRstStreamReceivedKey");

  static final ChannelFutureListener CHILD_CHANNEL_REGISTRATION_LISTENER = EspressoHttp2MultiplexHandler::registerDone;

  private static final Logger LOG = LogManager.getLogger(EspressoHttp2MultiplexHandler.class);

  private final ChannelHandler inboundStreamHandler;
  private final ChannelHandler upgradeStreamHandler;
  private final Queue<EspressoAbstractHttp2StreamChannel> readCompletePendingQueue = new MaxCapacityQueue<>(
      new ArrayDeque<>(8),
      // Choose 100 which is what is used most of the times as default.
      Http2CodecUtil.SMALLEST_MAX_CONCURRENT_STREAMS);

  private boolean parentReadInProgress;
  private final AtomicInteger idCount = new AtomicInteger();
  private boolean reuseChildChannels = false;
  private boolean offloadChildChannels = false;

  // Need to be volatile as accessed from within the EspressoHttp2MultiplexHandlerStreamChannel in a multi-threaded
  // fashion.
  private volatile ChannelHandlerContext ctx;

  private final ArrayDeque<EspressoAbstractHttp2StreamChannel> childChannelPool = new ArrayDeque<>();

  /**
   * Creates a new instance
   *
   * @param inboundStreamHandler the {@link ChannelHandler} that will be added to the { ChannelPipeline} of
   *                             the {@link Channel}s created for new inbound streams.
   */
  public EspressoHttp2MultiplexHandler(ChannelHandler inboundStreamHandler) {
    this(inboundStreamHandler, null);
  }

  /**
   * Creates a new instance
   *
   * @param inboundStreamHandler the {@link ChannelHandler} that will be added to the { ChannelPipeline} of
   *                             the {@link Channel}s created for new inbound streams.
   * @param upgradeStreamHandler the {@link ChannelHandler} that will be added to the { ChannelPipeline} of the
   *                             upgraded {@link Channel}.
   */
  public EspressoHttp2MultiplexHandler(ChannelHandler inboundStreamHandler, ChannelHandler upgradeStreamHandler) {
    this.inboundStreamHandler = ObjectUtil.checkNotNull(inboundStreamHandler, "inboundStreamHandler");
    this.upgradeStreamHandler = upgradeStreamHandler;
  }

  public EspressoHttp2MultiplexHandler(
      ChannelHandler inboundStreamHandler,
      boolean reuseChildChannels,
      boolean offloadChildChannels) {
    this(inboundStreamHandler, null);
    this.reuseChildChannels = reuseChildChannels;
    this.offloadChildChannels = offloadChildChannels;
  }

  // For Unit tests
  /* protected */ boolean reuseChildChannelsEnabled() {
    return reuseChildChannels;
  }

  // For unit tests
  public Queue<EspressoAbstractHttp2StreamChannel> getChildChannelPool() {
    return this.childChannelPool;
  }

  static void registerDone(ChannelFuture future) {
    // Handle any errors that occurred on the local thread while registering. Even though
    // failures can happen after this point, they will be handled by the channel by closing the
    // childChannel.
    if (!future.isSuccess()) {
      Channel childChannel = future.channel();
      if (childChannel.isRegistered()) {
        childChannel.close();
      } else {
        childChannel.unsafe().closeForcibly();
      }
    }
  }

  @Override
  protected void handlerAdded0(ChannelHandlerContext ctx) {
    // if (ctx.executor() != ctx.channel().eventLoop()) {
    // throw new IllegalStateException("EventExecutor must be EventLoop of Channel");
    // }
    this.ctx = ctx;
  }

  @Override
  protected void handlerRemoved0(ChannelHandlerContext ctx) {
    readCompletePendingQueue.clear();
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    while (!childChannelPool.isEmpty()) {
      try {
        childChannelPool.remove().unsafe().close(ctx.newPromise());
      } catch (Exception ex) {
        LOG.error("Exception while closing the parent HTTP/2 channel", ex);
      }
    }
    super.channelInactive(ctx);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    parentReadInProgress = true;
    if (msg instanceof Http2StreamFrame) {
      if (msg instanceof Http2WindowUpdateFrame) {
        // We dont want to propagate update frames to the user
        return;
      }
      Http2StreamFrame streamFrame = (Http2StreamFrame) msg;
      DefaultHttp2FrameStream s = (DefaultHttp2FrameStream) streamFrame.stream();

      EspressoAbstractHttp2StreamChannel channel = (EspressoAbstractHttp2StreamChannel) s.attachment;
      if (msg instanceof Http2ResetFrame) {
        checkUnexpectedError((Http2ResetFrame) streamFrame, channel);
        // Reset frames needs to be propagated via user events as these are not flow-controlled and so
        // must not be controlled by suppressing channel.read() on the child channel.
        channel.pipeline().fireUserEventTriggered(msg);

        // RST frames will also trigger closing of the streams which then will call
        // AbstractHttp2StreamChannel.streamClosed()
      } else {
        channel.fireChildRead(streamFrame);
      }
      return;
    }

    if (msg instanceof Http2GoAwayFrame) {
      // goaway frames will also trigger closing of the streams which then will call
      // AbstractHttp2StreamChannel.streamClosed()
      onHttp2GoAwayFrame(ctx, (Http2GoAwayFrame) msg);
    }

    // Send everything down the pipeline
    ctx.fireChannelRead(msg);
  }

  private static void checkUnexpectedError(Http2ResetFrame streamFrame, EspressoAbstractHttp2StreamChannel ch) {
    if (Http2Utils.isUnexpectedError(streamFrame.errorCode(), true)) {
      // Set the stream channel attribute to reflect that this channel received a reset stream with error from remote
      ch.attr(REMOTE_RST_STREAM_RECEIVED_KEY).set(Boolean.TRUE);
    }
  }

  private static boolean receivedRemoteRst(EspressoAbstractHttp2StreamChannel channel) {
    return NettyUtils.isTrue(channel, REMOTE_RST_STREAM_RECEIVED_KEY);
  }

  @Override
  public void channelWritabilityChanged(final ChannelHandlerContext ctx) throws Exception {
    if (ctx.channel().isWritable()) {
      // While the writability state may change during iterating of the streams we just set all of the streams
      // to writable to not affect fairness. These will be "limited" by their own watermarks in any case.
      forEachActiveStream(EspressoAbstractHttp2StreamChannel.WRITABLE_VISITOR);
    }

    ctx.fireChannelWritabilityChanged();
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    if (evt instanceof Http2FrameStreamEvent) {
      Http2FrameStreamEvent event = (Http2FrameStreamEvent) evt;
      DefaultHttp2FrameStream stream = (DefaultHttp2FrameStream) event.stream();
      if (event.type() == Http2FrameStreamEvent.Type.State) {
        switch (stream.state()) {
          case HALF_CLOSED_LOCAL:
            if (stream.id() != Http2CodecUtil.HTTP_UPGRADE_STREAM_ID) {
              // Ignore everything which was not caused by an upgrade
              break;
            }
            // fall-through
          case HALF_CLOSED_REMOTE: // SUPPRESS CHECKSTYLE FallThroughCheck
            // fall-through
          case OPEN:
            if (stream.attachment != null) {
              // ignore if child channel was already created.
              break;
            }
            EspressoAbstractHttp2StreamChannel ch;
            // We need to handle upgrades special when on the client side.
            if (stream.id() == Http2CodecUtil.HTTP_UPGRADE_STREAM_ID && !isServer(ctx)) {
              // We must have an upgrade handler or else we can't handle the stream
              if (upgradeStreamHandler == null) {
                throw connectionError(INTERNAL_ERROR, "Client is misconfigured for upgrade requests");
              }
              ch = new EspressoHttp2MultiplexHandlerStreamChannel(stream, upgradeStreamHandler, true);
              ch.closeOutbound();
            } else {
              // if queue is empty, register a new channel.
              ch = childChannelPool.poll();

              if (ch == null || !ch.isOpen()) {
                // Register child channel on different event loop.
                ch = new EspressoHttp2MultiplexHandlerStreamChannel(stream, inboundStreamHandler);
                // Register the channel on either parent or offload to a different I/O worker.
                EventLoop eventLoop = ctx.channel().eventLoop();
                ChannelFuture future = (!offloadChildChannels || eventLoop.parent() == null)
                    ? eventLoop.register(ch)
                    : eventLoop.parent().next().register(ch);
                if (future.isDone()) {
                  registerDone(future);
                } else {
                  future.addListener(CHILD_CHANNEL_REGISTRATION_LISTENER);
                }
              } else {
                // Initialize previous child channel.
                ch.init(stream, idCount.incrementAndGet());
              }
            }
            break;
          case CLOSED:
            EspressoAbstractHttp2StreamChannel channel = (EspressoAbstractHttp2StreamChannel) stream.attachment;
            if (channel != null) {
              if (reuseChildChannels && channel.isOpen() && !channel.containsUpgradeHandler()
              // Close the channel and don't reuse it when we receive error RST from remote
              // For the router -> SN pipeline, the reuse is handled in Http2AwareChannelPool
              // and closing the channel will prevent from reuse
                  && !receivedRemoteRst(channel)) {
                // Attempt to drain any queued data from the queue and deliver it to the application.
                channel.streamClosed(false);
                if (channel.isReadyToRecycle()) {
                  channel.reset();
                  if (isServer(ctx)) {
                    childChannelPool.add(channel);
                  }
                  break;
                }
              }
              // Force close the channel.
              channel.streamClosed(true);
            }
            break;
          default:
            // ignore for now
            break;
        }
      }
      return;
    }
    ctx.fireUserEventTriggered(evt);
  }

  // TODO: This is most likely not the best way to expose this, need to think more about it.
  Http2StreamChannel newOutboundStream() {
    return new EspressoHttp2MultiplexHandlerStreamChannel((DefaultHttp2FrameStream) newStream(), null);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    if (cause instanceof Http2FrameStreamException) {
      Http2FrameStreamException exception = (Http2FrameStreamException) cause;
      Http2FrameStream stream = exception.stream();
      EspressoAbstractHttp2StreamChannel childChannel =
          (EspressoAbstractHttp2StreamChannel) ((DefaultHttp2FrameStream) stream).attachment;
      try {
        childChannel.pipeline().fireExceptionCaught(cause.getCause());
      } finally {
        // Don't recycle child channels
        childChannel.setExceptionCaught(true);
        childChannel.unsafe().closeForcibly();
      }
      return;
    }
    ctx.fireExceptionCaught(cause);
  }

  private static boolean isServer(ChannelHandlerContext ctx) {
    return ctx.channel().parent() instanceof ServerChannel;
  }

  private void onHttp2GoAwayFrame(ChannelHandlerContext ctx, final Http2GoAwayFrame goAwayFrame) {
    try {
      final boolean server = isServer(ctx);
      forEachActiveStream(new Http2FrameStreamVisitor() {
        @Override
        public boolean visit(Http2FrameStream stream) {
          final int streamId = stream.id();
          if (streamId > goAwayFrame.lastStreamId() && Http2CodecUtil.isStreamIdValid(streamId, server)) {
            final EspressoAbstractHttp2StreamChannel childChannel =
                (EspressoAbstractHttp2StreamChannel) ((DefaultHttp2FrameStream) stream).attachment;
            childChannel.pipeline().fireUserEventTriggered(goAwayFrame.retainedDuplicate());
          }
          return true;
        }
      });
    } catch (Http2Exception e) {
      ctx.fireExceptionCaught(e);
      ctx.close();
    }
  }

  /**
   * Notifies any child streams of the read completion.
   */
  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    processPendingReadCompleteQueue();
    ctx.fireChannelReadComplete();
  }

  private void processPendingReadCompleteQueue() {
    parentReadInProgress = true;
    // If we have many child channel we can optimize for the case when multiple call flush() in
    // channelReadComplete(...) callbacks and only do it once as otherwise we will end-up with multiple
    // write calls on the socket which is expensive.
    EspressoAbstractHttp2StreamChannel childChannel = readCompletePendingQueue.poll();
    if (childChannel != null) {
      try {
        do {
          childChannel.fireChildReadComplete();
          childChannel = readCompletePendingQueue.poll();
        } while (childChannel != null);
      } finally {
        parentReadInProgress = false;
        readCompletePendingQueue.clear();
        ctx.flush();
      }
    } else {
      parentReadInProgress = false;
    }
  }

  public class EspressoHttp2MultiplexHandlerStreamChannel extends EspressoAbstractHttp2StreamChannel {
    EspressoHttp2MultiplexHandlerStreamChannel(
        DefaultHttp2FrameStream stream,
        ChannelHandler inboundHandler,
        boolean containsUpgradeHandler) {
      super(stream, idCount.incrementAndGet(), inboundHandler, containsUpgradeHandler);
    }

    EspressoHttp2MultiplexHandlerStreamChannel(DefaultHttp2FrameStream stream, ChannelHandler inboundHandler) {
      super(stream, idCount.incrementAndGet(), reuseChildChannels, inboundHandler);
    }

    public void init() {
      // TODO : Check if idCount needs to be volatile
      super.init((DefaultHttp2FrameStream) newStream(), idCount.incrementAndGet());
    }

    @Override
    protected boolean isParentReadInProgress() {
      return ctx.channel().eventLoop().inEventLoop() && parentReadInProgress;
    }

    @Override
    protected void addChannelToReadCompletePendingQueue() {
      EventLoop eventLoop = parentContext().channel().eventLoop();
      if (eventLoop.inEventLoop()) {
        addChannelToReadCompletePendingQueue0();
      } else {
        eventLoop.execute(this::addChannelToReadCompletePendingQueue0);
      }
    }

    private void addChannelToReadCompletePendingQueue0() {
      // If the queue already contains this, we will skip.
      if (readCompletePendingQueue.contains(this)) {
        return;
      }
      // If there is no space left in the queue, just keep on processing everything that is already
      // stored there and try again.
      while (!readCompletePendingQueue.offer(this)) {
        processPendingReadCompleteQueue();
      }
    }

    @Override
    protected ChannelHandlerContext parentContext() {
      return ctx;
    }
  }
}
