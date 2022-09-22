package io.netty.handler.codec.http2;

import static io.netty.handler.codec.http2.Http2CodecUtil.isStreamIdValid;
import static java.lang.Math.min;

import com.linkedin.alpini.netty4.misc.NettyUtils;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelConfig;
import io.netty.channel.DefaultChannelPipeline;
import io.netty.channel.EventLoop;
import io.netty.channel.MessageSizeEstimator;
import io.netty.channel.RecvByteBufAllocator;
import io.netty.channel.VoidChannelPromise;
import io.netty.handler.codec.http2.Http2FrameCodec.DefaultHttp2FrameStream;
import io.netty.util.DefaultAttributeMap;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.StringUtil;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Forked from Netty's AbstractHttp2StreamChannel (4.1.42)
 *
 * https://github.com/netty/netty/blob/4.1/codec-http2/src/main/java/io/netty/handler/codec/http2/AbstractHttp2StreamChannel.java
 */

abstract class EspressoAbstractHttp2StreamChannel extends DefaultAttributeMap implements Http2StreamChannel {
  static final Http2FrameStreamVisitor WRITABLE_VISITOR = new Http2FrameStreamVisitor() {
    @Override
    public boolean visit(Http2FrameStream stream) {
      final EspressoAbstractHttp2StreamChannel childChannel =
          (EspressoAbstractHttp2StreamChannel) ((DefaultHttp2FrameStream) stream).attachment;
      childChannel.trySetWritable();
      return true;
    }
  };

  private static final Logger LOG = LogManager.getLogger(EspressoAbstractHttp2StreamChannel.class);

  private static final ChannelMetadata METADATA = new ChannelMetadata(false, 16);

  /**
   * Number of bytes to consider non-payload messages. 9 is arbitrary, but also the minimum size of an HTTP/2 frame.
   * Primarily is non-zero.
   */
  private static final int MIN_HTTP2_FRAME_SIZE = 9;

  /**
   * Returns the flow-control size for DATA frames, and {@value MIN_HTTP2_FRAME_SIZE} for all other frames.
   */
  private static final class FlowControlledFrameSizeEstimator implements MessageSizeEstimator {
    static final FlowControlledFrameSizeEstimator INSTANCE = new FlowControlledFrameSizeEstimator();

    private static final Handle HANDLE_INSTANCE = new Handle() {
      @Override
      public int size(Object msg) {
        return msg instanceof Http2DataFrame // Guard against overflow.
            ? (int) min(
                Integer.MAX_VALUE,
                ((Http2DataFrame) msg).initialFlowControlledBytes() + (long) MIN_HTTP2_FRAME_SIZE)
            : MIN_HTTP2_FRAME_SIZE;
      }
    };

    @Override
    public Handle newHandle() {
      return HANDLE_INSTANCE;
    }
  }

  private static final AtomicLongFieldUpdater<EspressoAbstractHttp2StreamChannel> TOTAL_PENDING_SIZE_UPDATER =
      AtomicLongFieldUpdater.newUpdater(EspressoAbstractHttp2StreamChannel.class, "totalPendingSize");

  private static final AtomicIntegerFieldUpdater<EspressoAbstractHttp2StreamChannel> UNWRITABLE_UPDATER =
      AtomicIntegerFieldUpdater.newUpdater(EspressoAbstractHttp2StreamChannel.class, "unwritable");

  /**
   * The current status of the read-processing for a {@link EspressoAbstractHttp2StreamChannel}.
   */
  private enum ReadStatus {
    /**
     * No read in progress and no read was requested (yet)
     */
    IDLE,

    /**
     * Reading in progress
     */
    IN_PROGRESS,

    /**
     * A read operation was requested.
     */
    REQUESTED
  }

  private final EspressoAbstractHttp2StreamChannel.Http2StreamChannelConfig config = new Http2StreamChannelConfig(this);
  private final EspressoAbstractHttp2StreamChannel.Http2ChannelUnsafe unsafe = new Http2ChannelUnsafe();
  private ChannelId channelId;
  private final ChannelPipeline pipeline;
  private DefaultHttp2FrameStream stream;
  private final ChannelPromise closePromise;

  private volatile boolean registered;

  private volatile long totalPendingSize;
  private volatile int unwritable;

  // Cached to reduce GC
  private Runnable fireChannelWritabilityChangedTask;

  private boolean outboundClosed;
  private int flowControlledBytes;
  private EventLoop eventLoop;
  private boolean containsUpgradeHandler = false;
  private boolean readyToRecycle = false;
  // If an exception is caught
  private boolean exceptionCaught = false;
  // If close is in progress
  private boolean closeInProgress = false;
  // If reuse child channels, don't force close on EOS
  private boolean reuseChannels = false;

  /**
   * This variable represents if a read is in progress for the current channel or was requested.
   * Note that depending upon the {@link RecvByteBufAllocator} behavior a read may extend beyond the
   * {@link Http2ChannelUnsafe#beginRead()} method scope. The {@link Http2ChannelUnsafe#beginRead()} loop may
   * drain all pending data, and then if the parent channel is reading this channel may still accept frames.
   */
  private ReadStatus readStatus = ReadStatus.IDLE;

  private Queue<Object> inboundBuffer;

  /** {@code true} after the first HEADERS frame has been written **/
  private boolean firstFrameWritten;
  private boolean readCompletePending;

  EspressoAbstractHttp2StreamChannel(
      DefaultHttp2FrameStream stream,
      int id,
      ChannelHandler inboundHandler,
      boolean containsUpgradeHandler) {
    this(stream, id, false, inboundHandler);
    this.containsUpgradeHandler = containsUpgradeHandler;
  }

  EspressoAbstractHttp2StreamChannel(
      DefaultHttp2FrameStream stream,
      int id,
      boolean reuseChannels,
      ChannelHandler inboundHandler) {
    this.stream = stream;
    stream.attachment = this;
    this.reuseChannels = reuseChannels;
    pipeline = new DefaultChannelPipeline(this) {
      @Override
      protected void incrementPendingOutboundBytes(long size) {
        EspressoAbstractHttp2StreamChannel.this.incrementPendingOutboundBytes(size, true);
      }

      @Override
      protected void decrementPendingOutboundBytes(long size) {
        EspressoAbstractHttp2StreamChannel.this.decrementPendingOutboundBytes(size, true);
      }
    };

    closePromise = pipeline.newPromise();
    channelId = new Http2StreamChannelId(parent().id(), id);

    if (inboundHandler != null) {
      // Add the handler to the pipeline now that we are registered.
      pipeline.addLast(NettyUtils.executorGroup(pipeline), inboundHandler);
    }
  }

  public void init(DefaultHttp2FrameStream stream, int id) {
    this.stream = stream;
    stream.attachment = this;
    channelId = new Http2StreamChannelId(parent().id(), id);
    // Reset the EOS flag
    resetEOS();
    totalPendingSize = 0L;
    unwritable = 0;
    flowControlledBytes = 0;
    firstFrameWritten = false;
    readCompletePending = false;
  }

  public void reset() {
    this.readyToRecycle = false;
  }

  public boolean containsUpgradeHandler() {
    return this.containsUpgradeHandler;
  }

  public boolean isReadyToRecycle() {
    return this.readyToRecycle;
  }

  public void setReadyToRecycle(boolean readyToRecycle) {
    this.readyToRecycle = readyToRecycle;
  }

  public boolean isExceptionCaught() {
    return exceptionCaught;
  }

  public void setExceptionCaught(boolean exceptionCaught) {
    this.exceptionCaught = exceptionCaught;
  }

  public boolean isCloseInProgress() {
    return closeInProgress;
  }

  public void setCloseInProgress(boolean closeIsInProgress) {
    this.closeInProgress = closeIsInProgress;
  }

  private void incrementPendingOutboundBytes(long size, boolean invokeLater) {
    if (size == 0) {
      return;
    }

    long newWriteBufferSize = TOTAL_PENDING_SIZE_UPDATER.addAndGet(this, size);
    if (newWriteBufferSize > config().getWriteBufferHighWaterMark()) {
      setUnwritable(invokeLater);
    }
  }

  private void decrementPendingOutboundBytes(long size, boolean invokeLater) {
    if (size == 0) {
      return;
    }

    long newWriteBufferSize = TOTAL_PENDING_SIZE_UPDATER.addAndGet(this, -size);
    // Once the totalPendingSize dropped below the low water-mark we can mark the child channel
    // as writable again. Before doing so we also need to ensure the parent channel is writable to
    // prevent excessive buffering in the parent outbound buffer. If the parent is not writable
    // we will mark the child channel as writable once the parent becomes writable by calling
    // trySetWritable() later.
    if (newWriteBufferSize < config().getWriteBufferLowWaterMark() && parent().isWritable()) {
      setWritable(invokeLater);
    }
  }

  final void trySetWritable() {
    // The parent is writable again but the child channel itself may still not be writable.
    // Lets try to set the child channel writable to match the state of the parent channel
    // if (and only if) the totalPendingSize is smaller then the low water-mark.
    // If this is not the case we will try again later once we drop under it.
    if (totalPendingSize < config().getWriteBufferLowWaterMark()) {
      setWritable(false);
    }
  }

  private void setWritable(boolean invokeLater) {
    for (;;) {
      final int oldValue = unwritable;
      final int newValue = oldValue & ~1;
      if (UNWRITABLE_UPDATER.compareAndSet(this, oldValue, newValue)) {
        if (oldValue != 0 && newValue == 0) {
          fireChannelWritabilityChanged(invokeLater);
        }
        break;
      }
    }
  }

  private void setUnwritable(boolean invokeLater) {
    for (;;) {
      final int oldValue = unwritable;
      final int newValue = oldValue | 1;
      if (UNWRITABLE_UPDATER.compareAndSet(this, oldValue, newValue)) {
        if (oldValue == 0 && newValue != 0) {
          fireChannelWritabilityChanged(invokeLater);
        }
        break;
      }
    }
  }

  private void fireChannelWritabilityChanged(boolean invokeLater) {
    final ChannelPipeline pipeline = pipeline();
    if (invokeLater) {
      Runnable task = fireChannelWritabilityChangedTask;
      if (task == null) {
        fireChannelWritabilityChangedTask = task = new Runnable() { // SUPPRESS CHECKSTYLE InnerAssignment
          @Override
          public void run() {
            pipeline.fireChannelWritabilityChanged();
          }
        };
      }
      eventLoop().execute(task);
    } else {
      pipeline.fireChannelWritabilityChanged();
    }
  }

  @Override
  public Http2FrameStream stream() {
    return stream;
  }

  void closeOutbound() {
    outboundClosed = true;
  }

  void streamClosed(boolean closeChannelOnEOS) {
    unsafe.readEOS();
    // Attempt to drain any queued data from the queue and deliver it to the application before closing this
    // channel.
    unsafe.doBeginRead(closeChannelOnEOS);
  }

  void resetEOS() {
    // Reset the EOS flag to false;
    unsafe.resetEOS();
  }

  @Override
  public ChannelMetadata metadata() {
    return METADATA;
  }

  @Override
  public ChannelConfig config() {
    return config;
  }

  @Override
  public boolean isOpen() {
    return !closePromise.isDone();
  }

  @Override
  public boolean isActive() {
    return isOpen();
  }

  @Override
  public boolean isWritable() {
    return unwritable == 0;
  }

  @Override
  public ChannelId id() {
    return channelId;
  }

  @Override
  public EventLoop eventLoop() {
    return eventLoop;
  }

  @Override
  public Channel parent() {
    return parentContext().channel();
  }

  @Override
  public boolean isRegistered() {
    return registered;
  }

  @Override
  public SocketAddress localAddress() {
    return parent().localAddress();
  }

  @Override
  public SocketAddress remoteAddress() {
    return parent().remoteAddress();
  }

  @Override
  public ChannelFuture closeFuture() {
    return closePromise;
  }

  @Override
  public long bytesBeforeUnwritable() {
    long bytes = config().getWriteBufferHighWaterMark() - totalPendingSize;
    // If bytes is negative we know we are not writable, but if bytes is non-negative we have to check
    // writability. Note that totalPendingSize and isWritable() use different volatile variables that are not
    // synchronized together. totalPendingSize will be updated before isWritable().
    if (bytes > 0) {
      return isWritable() ? bytes : 0;
    }
    return 0;
  }

  @Override
  public long bytesBeforeWritable() {
    long bytes = totalPendingSize - config().getWriteBufferLowWaterMark();
    // If bytes is negative we know we are writable, but if bytes is non-negative we have to check writability.
    // Note that totalPendingSize and isWritable() use different volatile variables that are not synchronized
    // together. totalPendingSize will be updated before isWritable().
    if (bytes > 0) {
      return isWritable() ? 0 : bytes;
    }
    return 0;
  }

  @Override
  public Unsafe unsafe() {
    return unsafe;
  }

  @Override
  public ChannelPipeline pipeline() {
    return pipeline;
  }

  @Override
  public ByteBufAllocator alloc() {
    return config().getAllocator();
  }

  @Override
  public Channel read() {
    pipeline().read();
    return this;
  }

  @Override
  public Channel flush() {
    pipeline().flush();
    return this;
  }

  @Override
  public ChannelFuture bind(SocketAddress localAddress) {
    return pipeline().bind(localAddress);
  }

  @Override
  public ChannelFuture connect(SocketAddress remoteAddress) {
    return pipeline().connect(remoteAddress);
  }

  @Override
  public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress) {
    return pipeline().connect(remoteAddress, localAddress);
  }

  @Override
  public ChannelFuture disconnect() {
    return pipeline().disconnect();
  }

  @Override
  public ChannelFuture close() {
    // If the channel is set to close, do not recycle.
    setCloseInProgress(true);
    return pipeline().close();
  }

  @Override
  public ChannelFuture deregister() {
    return pipeline().deregister();
  }

  @Override
  public ChannelFuture bind(SocketAddress localAddress, ChannelPromise promise) {
    return pipeline().bind(localAddress, promise);
  }

  @Override
  public ChannelFuture connect(SocketAddress remoteAddress, ChannelPromise promise) {
    return pipeline().connect(remoteAddress, promise);
  }

  @Override
  public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) {
    return pipeline().connect(remoteAddress, localAddress, promise);
  }

  @Override
  public ChannelFuture disconnect(ChannelPromise promise) {
    return pipeline().disconnect(promise);
  }

  @Override
  public ChannelFuture close(ChannelPromise promise) {
    // If the channel is set to close, not recycle.
    setCloseInProgress(true);
    return pipeline().close(promise);
  }

  @Override
  public ChannelFuture deregister(ChannelPromise promise) {
    return pipeline().deregister(promise);
  }

  @Override
  public ChannelFuture write(Object msg) {
    return pipeline().write(msg);
  }

  @Override
  public ChannelFuture write(Object msg, ChannelPromise promise) {
    return pipeline().write(msg, promise);
  }

  @Override
  public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
    return pipeline().writeAndFlush(msg, promise);
  }

  @Override
  public ChannelFuture writeAndFlush(Object msg) {
    return pipeline().writeAndFlush(msg);
  }

  @Override
  public ChannelPromise newPromise() {
    return pipeline().newPromise();
  }

  @Override
  public ChannelProgressivePromise newProgressivePromise() {
    return pipeline().newProgressivePromise();
  }

  @Override
  public ChannelFuture newSucceededFuture() {
    return pipeline().newSucceededFuture();
  }

  @Override
  public ChannelFuture newFailedFuture(Throwable cause) {
    return pipeline().newFailedFuture(cause);
  }

  @Override
  public ChannelPromise voidPromise() {
    return pipeline().voidPromise();
  }

  @Override
  public int hashCode() {
    return id().hashCode();
  }

  @Override
  public boolean equals(Object o) {
    return this == o;
  }

  @Override
  public int compareTo(Channel o) {
    if (this == o) {
      return 0;
    }

    return id().compareTo(o.id());
  }

  @Override
  public String toString() {
    return parent().toString() + "(H2 - " + stream + ')';
  }

  /**
   * Receive a read message. This does not notify handlers unless a read is in progress on the
   * channel.
   */
  void fireChildRead(Http2Frame frame) {
    if (eventLoop().inEventLoop()) {
      fireChildRead0(frame);
    } else {
      eventLoop().execute(() -> fireChildRead0(frame));
    }
  }

  void fireChildRead0(Http2Frame frame) {
    // Child channel different executor
    assert eventLoop().inEventLoop();
    if (!isActive()) {
      ReferenceCountUtil.release(frame);
    } else if (readStatus != ReadStatus.IDLE) {
      // If a read is in progress or has been requested, there cannot be anything in the queue,
      // otherwise we would have drained it from the queue and processed it during the read cycle.
      assert inboundBuffer == null || inboundBuffer.isEmpty();
      final RecvByteBufAllocator.Handle allocHandle = unsafe.recvBufAllocHandle();
      flowControlledBytes += unsafe.doRead0(frame, allocHandle);
      // We currently don't need to check for readEOS because the parent channel and child channel are limited
      // to the same EventLoop thread. There are a limited number of frame types that may come after EOS is
      // read (unknown, reset) and the trade off is less conditionals for the hot path (headers/data) at the
      // cost of additional readComplete notifications on the rare path.
      if (allocHandle.continueReading()) {
        if (!readCompletePending) {
          readCompletePending = true;
          addChannelToReadCompletePendingQueue();
        }
      } else {
        unsafe.notifyReadComplete(allocHandle, true);
      }
    } else {
      if (inboundBuffer == null) {
        inboundBuffer = new ArrayDeque<Object>(4);
      }
      inboundBuffer.add(frame);
    }
  }

  void fireChildReadComplete() {
    if (eventLoop().inEventLoop()) {
      fireChildReadComplete0();
    } else {
      eventLoop().execute(this::fireChildReadComplete0);
    }
  }

  void fireChildReadComplete0() {
    assert eventLoop().inEventLoop();
    assert readStatus != ReadStatus.IDLE || !readCompletePending;
    unsafe.notifyReadComplete(unsafe.recvBufAllocHandle(), false);
  }

  private final class Http2ChannelUnsafe implements Unsafe {
    private final VoidChannelPromise unsafeVoidPromise =
        new VoidChannelPromise(EspressoAbstractHttp2StreamChannel.this, false);
    @SuppressWarnings("deprecation")
    private RecvByteBufAllocator.Handle recvHandle;
    private boolean writeDoneAndNoFlush;
    private boolean closeInitiated;
    private boolean readEOS;

    @Override
    public void connect(final SocketAddress remoteAddress, SocketAddress localAddress, final ChannelPromise promise) {
      if (!promise.setUncancellable()) {
        return;
      }
      promise.setFailure(new UnsupportedOperationException());
    }

    @Override
    public RecvByteBufAllocator.Handle recvBufAllocHandle() {
      if (recvHandle == null) {
        recvHandle = config().getRecvByteBufAllocator().newHandle();
        recvHandle.reset(config());
      }
      return recvHandle;
    }

    @Override
    public SocketAddress localAddress() {
      return parent().unsafe().localAddress();
    }

    @Override
    public SocketAddress remoteAddress() {
      return parent().unsafe().remoteAddress();
    }

    @Override
    public void register(EventLoop eventLoop, ChannelPromise promise) {
      EspressoAbstractHttp2StreamChannel.this.eventLoop = eventLoop;
      if (!promise.setUncancellable()) {
        return;
      }
      if (registered) {
        promise.setFailure(new UnsupportedOperationException("Re-register is not supported"));
        return;
      }

      registered = true;

      promise.setSuccess();

      pipeline().fireChannelRegistered();
      if (isActive()) {
        pipeline().fireChannelActive();
      }
    }

    @Override
    public void bind(SocketAddress localAddress, ChannelPromise promise) {
      if (!promise.setUncancellable()) {
        return;
      }
      promise.setFailure(new UnsupportedOperationException());
    }

    @Override
    public void disconnect(ChannelPromise promise) {
      close(promise);
    }

    @Override
    public void close(final ChannelPromise promise) {
      if (!promise.setUncancellable()) {
        return;
      }
      if (closeInitiated) {
        if (closePromise.isDone()) {
          // Closed already.
          promise.setSuccess();
        } else if (!(promise instanceof VoidChannelPromise)) { // Only needed if no VoidChannelPromise.
          // This means close() was called before so we just register a listener and return
          closePromise.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) {
              promise.setSuccess();
            }
          });
        }
        return;
      }
      setCloseInProgress(true);
      closeInitiated = true;
      // Just set to false as removing from an underlying queue would even be more expensive.
      readCompletePending = false;

      final boolean wasActive = isActive();

      updateLocalWindowIfNeeded();

      // Only ever send a reset frame if the connection is still alive and if the stream was created before
      // as otherwise we may send a RST on a stream in an invalid state and cause a connection error.
      if (parent().isActive() && !readEOS && Http2CodecUtil.isStreamIdValid(stream.id())) {
        Http2StreamFrame resetFrame = new DefaultHttp2ResetFrame(Http2Error.CANCEL).stream(stream());
        write(resetFrame, unsafe().voidPromise());
        flush();
      }

      if (inboundBuffer != null) {
        for (;;) {
          Object msg = inboundBuffer.poll();
          if (msg == null) {
            break;
          }
          ReferenceCountUtil.release(msg);
        }
        inboundBuffer = null;
      }

      // The promise should be notified before we call fireChannelInactive().
      outboundClosed = true;
      closePromise.setSuccess();
      promise.setSuccess();

      fireChannelInactiveAndDeregister(voidPromise(), wasActive);
    }

    @Override
    public void closeForcibly() {
      close(unsafe().voidPromise());
    }

    @Override
    public void deregister(ChannelPromise promise) {
      fireChannelInactiveAndDeregister(promise, false);
    }

    private void fireChannelInactiveAndDeregister(final ChannelPromise promise, final boolean fireChannelInactive) {
      if (!promise.setUncancellable()) {
        return;
      }

      if (!registered) {
        promise.setSuccess();
        return;
      }

      // As a user may call deregister() from within any method while doing processing in the ChannelPipeline,
      // we need to ensure we do the actual deregister operation later. This is necessary to preserve the
      // behavior of the AbstractChannel, which always invokes channelUnregistered and channelInactive
      // events 'later' to ensure the current events in the handler are completed before these events.
      //
      // See:
      // https://github.com/netty/netty/issues/4435
      invokeLater(new Runnable() {
        @Override
        public void run() {
          if (fireChannelInactive) {
            pipeline.fireChannelInactive();
          }
          // The user can fire `deregister` events multiple times but we only want to fire the pipeline
          // event if the channel was actually registered.
          if (registered) {
            registered = false;
            pipeline.fireChannelUnregistered();
          }
          safeSetSuccess(promise);
        }
      });
    }

    private void safeSetSuccess(ChannelPromise promise) {
      if (!(promise instanceof VoidChannelPromise) && !promise.trySuccess()) {
        LOG.warn("Failed to mark a promise as success because it is done already: {}", promise);
      }
    }

    private void invokeLater(Runnable task) {
      try {
        // This method is used by outbound operation implementations to trigger an inbound event later.
        // They do not trigger an inbound event immediately because an outbound operation might have been
        // triggered by another inbound event handler method. If fired immediately, the call stack
        // will look like this for example:
        //
        // handlerA.inboundBufferUpdated() - (1) an inbound handler method closes a connection.
        // -> handlerA.ctx.close()
        // -> channel.unsafe.close()
        // -> handlerA.channelInactive() - (2) another inbound handler method called while in (1) yet
        //
        // which means the execution of two inbound handler methods of the same handler overlap undesirably.
        eventLoop().execute(task);
      } catch (RejectedExecutionException e) {
        LOG.warn("Can't invoke task later as EventLoop rejected it", e);
      }
    }

    @Override
    public void beginRead() {
      if (!isActive()) {
        return;
      }
      updateLocalWindowIfNeeded();

      switch (readStatus) {
        case IDLE:
          readStatus = ReadStatus.IN_PROGRESS;
          doBeginRead();
          break;
        case IN_PROGRESS:
          readStatus = ReadStatus.REQUESTED;
          break;
        default:
          break;
      }
    }

    private Object pollQueuedMessage() {
      return inboundBuffer == null ? null : inboundBuffer.poll();
    }

    void doBeginRead() {
      doBeginRead(!reuseChannels);
    }

    void doBeginRead(boolean closeChannelOnEOS) {
      if (eventLoop().inEventLoop()) {
        doBeginRead0(closeChannelOnEOS);
      } else {
        eventLoop().execute(() -> doBeginRead0(closeChannelOnEOS));
      }
    }

    private void doBeginRead0(boolean closeChannelOnEOS) {
      // Process messages until there are none left (or the user stopped requesting) and also handle EOS.
      while (readStatus != ReadStatus.IDLE) {
        Object message = pollQueuedMessage();
        if (message == null) {
          if (readEOS) {
            if (closeChannelOnEOS || isExceptionCaught() || isCloseInProgress()) {
              unsafe.closeForcibly();
            } else {
              readyToRecycle = true;
            }
          }
          break;
        }
        final RecvByteBufAllocator.Handle allocHandle = recvBufAllocHandle();
        allocHandle.reset(config());
        boolean continueReading = false;
        do {
          flowControlledBytes += doRead0((Http2Frame) message, allocHandle);
        } while ((readEOS || (continueReading = allocHandle.continueReading())) // SUPPRESS CHECKSTYLE InnerAssignment
            && (message = pollQueuedMessage()) != null); // SUPPRESS CHECKSTYLE InnerAssignment

        if (continueReading && isParentReadInProgress() && !readEOS) {
          // Currently the parent and child channel are on the same EventLoop thread. If the parent is
          // currently reading it is possible that more frames will be delivered to this child channel. In
          // the case that this child channel still wants to read we delay the channelReadComplete on this
          // child channel until the parent is done reading.
          if (!readCompletePending) {
            readCompletePending = true;
            addChannelToReadCompletePendingQueue();
          }
        } else {
          notifyReadComplete(allocHandle, true);
        }
      }
    }

    void readEOS() {
      readEOS = true;
    }

    void resetEOS() {
      readEOS = false;
    }

    private void updateLocalWindowIfNeeded() {
      if (flowControlledBytes != 0) {
        int bytes = flowControlledBytes;
        flowControlledBytes = 0;
        write0(parentContext(), new DefaultHttp2WindowUpdateFrame(bytes).stream(stream));
        writeDoneAndNoFlush = true;
      }
    }

    void notifyReadComplete(RecvByteBufAllocator.Handle allocHandle, boolean forceReadComplete) {
      if (!readCompletePending && !forceReadComplete) {
        return;
      }
      // Set to false just in case we added the channel multiple times before.
      readCompletePending = false;

      if (readStatus == ReadStatus.REQUESTED) {
        readStatus = ReadStatus.IN_PROGRESS;
      } else {
        readStatus = ReadStatus.IDLE;
      }

      allocHandle.readComplete();
      pipeline().fireChannelReadComplete();
      // Reading data may result in frames being written (e.g. WINDOW_UPDATE, RST, etc..). If the parent
      // channel is not currently reading we need to force a flush at the child channel, because we cannot
      // rely upon flush occurring in channelReadComplete on the parent channel.
      flush();
      // If reuse is enabled, don't close the child channel
      if (readEOS && !reuseChannels) {
        unsafe.closeForcibly();
      }
    }

    @SuppressWarnings("deprecation")
    int doRead0(Http2Frame frame, RecvByteBufAllocator.Handle allocHandle) {
      pipeline().fireChannelRead(frame);
      allocHandle.incMessagesRead(1);

      if (frame instanceof Http2DataFrame) {
        final int numBytesToBeConsumed = ((Http2DataFrame) frame).initialFlowControlledBytes();
        allocHandle.attemptedBytesRead(numBytesToBeConsumed);
        allocHandle.lastBytesRead(numBytesToBeConsumed);
        return numBytesToBeConsumed;
      } else {
        allocHandle.attemptedBytesRead(MIN_HTTP2_FRAME_SIZE);
        allocHandle.lastBytesRead(MIN_HTTP2_FRAME_SIZE);
      }
      return 0;
    }

    @Override
    public void write(Object msg, final ChannelPromise promise) {
      // After this point its not possible to cancel a write anymore.
      if (!promise.setUncancellable()) {
        ReferenceCountUtil.release(msg);
        return;
      }

      if (!isActive()
          // Once the outbound side was closed we should not allow header / data frames
          || outboundClosed && (msg instanceof Http2HeadersFrame || msg instanceof Http2DataFrame)) {
        ReferenceCountUtil.release(msg);
        promise.setFailure(new ClosedChannelException());
        return;
      }

      try {
        if (msg instanceof Http2StreamFrame) {
          Http2StreamFrame frame = validateStreamFrame((Http2StreamFrame) msg).stream(stream());
          if (!firstFrameWritten && !isStreamIdValid(stream().id())) {
            if (!(frame instanceof Http2HeadersFrame)) {
              ReferenceCountUtil.release(frame);
              promise.setFailure(
                  new IllegalArgumentException("The first frame must be a headers frame. Was: " + frame.name()));
              return;
            }
            firstFrameWritten = true;
            ChannelFuture f = write0(parentContext(), frame);
            if (f.isDone()) {
              firstWriteComplete(f, promise);
            } else {
              final long bytes = FlowControlledFrameSizeEstimator.HANDLE_INSTANCE.size(msg);
              incrementPendingOutboundBytes(bytes, false);
              f.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) {
                  firstWriteComplete(future, promise);
                  decrementPendingOutboundBytes(bytes, false);
                }
              });
              writeDoneAndNoFlush = true;
            }
            return;
          }
        } else {
          String msgStr = msg.toString();
          ReferenceCountUtil.release(msg);
          promise.setFailure(
              new IllegalArgumentException(
                  "Message must be an " + StringUtil.simpleClassName(Http2StreamFrame.class) + ": " + msgStr));
          return;
        }

        ChannelFuture f = write0(parentContext(), msg);
        if (f.isDone()) {
          writeComplete(f, promise);
        } else {
          final long bytes = FlowControlledFrameSizeEstimator.HANDLE_INSTANCE.size(msg);
          incrementPendingOutboundBytes(bytes, false);
          f.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) {
              writeComplete(future, promise);
              decrementPendingOutboundBytes(bytes, false);
            }
          });
          writeDoneAndNoFlush = true;
        }
      } catch (Throwable t) {
        promise.tryFailure(t);
      }
    }

    private void firstWriteComplete(ChannelFuture future, ChannelPromise promise) {
      Throwable cause = future.cause();
      if (cause == null) {
        promise.setSuccess();
      } else {
        // If the first write fails there is not much we can do, just close
        closeForcibly();
        promise.setFailure(wrapStreamClosedError(cause));
      }
    }

    private void writeComplete(ChannelFuture future, ChannelPromise promise) {
      Throwable cause = future.cause();
      if (cause == null) {
        promise.setSuccess();
      } else {
        Throwable error = wrapStreamClosedError(cause);
        // To make it more consistent with AbstractChannel we handle all IOExceptions here.
        if (error instanceof IOException) {
          if (config.isAutoClose()) {
            // Close channel if needed.
            closeForcibly();
          } else {
            // TODO: Once Http2StreamChannel extends DuplexChannel we should call shutdownOutput(...)
            outboundClosed = true;
          }
        }
        promise.setFailure(error);
      }
    }

    private Throwable wrapStreamClosedError(Throwable cause) {
      // If the error was caused by STREAM_CLOSED we should use a ClosedChannelException to better
      // mimic other transports and make it easier to reason about what exceptions to expect.
      // Just print one line to make it less noisy
      LOG.warn("wrapStreamCloseError {}", cause.toString());
      if (cause instanceof Http2Exception && ((Http2Exception) cause).error() == Http2Error.STREAM_CLOSED) {
        return new ClosedChannelException().initCause(cause);
      }
      return cause;
    }

    private Http2StreamFrame validateStreamFrame(Http2StreamFrame frame) {
      if (frame.stream() != null && frame.stream() != stream) {
        String msgString = frame.toString();
        ReferenceCountUtil.release(frame);
        throw new IllegalArgumentException("Stream " + frame.stream() + " must not be set on the frame: " + msgString);
      }
      return frame;
    }

    @Override
    public void flush() {
      // If we are currently in the parent channel's read loop we should just ignore the flush.
      // We will ensure we trigger ctx.flush() after we processed all Channels later on and
      // so aggregate the flushes. This is done as ctx.flush() is expensive when as it may trigger an
      // write(...) or writev(...) operation on the socket.
      if (!writeDoneAndNoFlush || isParentReadInProgress()) {
        // There is nothing to flush so this is a NOOP.
        return;
      }
      try {
        flush0(parentContext());
      } finally {
        writeDoneAndNoFlush = false;
      }
    }

    @Override
    public ChannelPromise voidPromise() {
      return unsafeVoidPromise;
    }

    @Override
    public ChannelOutboundBuffer outboundBuffer() {
      // Always return null as we not use the ChannelOutboundBuffer and not even support it.
      return null;
    }
  }

  /**
   * {@link ChannelConfig} so that the high and low writebuffer watermarks can reflect the outbound flow control
   * window, without having to create a new WriteBufferWaterMark object whenever the flow control window
   * changes.
   */
  private final class Http2StreamChannelConfig extends DefaultChannelConfig {
    Http2StreamChannelConfig(Channel channel) {
      super(channel);
    }

    @Override
    public MessageSizeEstimator getMessageSizeEstimator() {
      return FlowControlledFrameSizeEstimator.INSTANCE;
    }

    @Override
    public ChannelConfig setMessageSizeEstimator(MessageSizeEstimator estimator) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ChannelConfig setRecvByteBufAllocator(RecvByteBufAllocator allocator) {
      if (!(allocator.newHandle() instanceof RecvByteBufAllocator.ExtendedHandle)) {
        throw new IllegalArgumentException(
            "allocator.newHandle() must return an object of type: " + RecvByteBufAllocator.ExtendedHandle.class);
      }
      super.setRecvByteBufAllocator(allocator);
      return this;
    }
  }

  protected void flush0(ChannelHandlerContext ctx) {
    ctx.flush();
  }

  protected ChannelFuture write0(ChannelHandlerContext ctx, Object msg) {
    ChannelPromise promise = newPromise();
    ctx.write(msg).addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) throws Exception {
        if (future.isSuccess()) {
          promise.setSuccess();
        } else {
          promise.setFailure(future.cause());
        }
      }
    });
    return promise;
  }

  protected abstract boolean isParentReadInProgress();

  protected abstract void addChannelToReadCompletePendingQueue();

  protected abstract ChannelHandlerContext parentContext();
}
