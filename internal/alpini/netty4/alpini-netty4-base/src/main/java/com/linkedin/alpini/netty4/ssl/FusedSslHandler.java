package com.linkedin.alpini.netty4.ssl;

import com.linkedin.alpini.base.misc.ExceptionUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.ssl.SslHandler;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.concurrent.Executor;
import javax.annotation.Nonnegative;
import javax.net.ssl.SSLEngine;


/**
 * An implementation of {@linkplain SslHandler} which limits the number of objects held within the
 * pendingUnencryptedWrites queue. This queue is normally very small but when there is some badly
 * behaved code which writes to the channel in a tight loop, we would want to fail further writes
 * so that we don't fall into an OOM situation.
 *
 * @author acurtis
 */
public class FusedSslHandler extends SslHandler {
  private static final Field PENDING_UNENCRYPTED_WRITES;
  private static final Field BUF_AND_LISTENER_PAIRS;
  private static final IOException OUTBOUND_QUEUE_OVERFLOW;

  private int _maxOutboundQueueSize;
  private ArrayDeque<Object> _outboundBufAndListenerPairs;

  public FusedSslHandler(SSLEngine engine) {
    super(engine);
  }

  public FusedSslHandler(SSLEngine engine, Executor delegatedTaskExecutor) {
    super(engine, delegatedTaskExecutor);
  }

  public void setOutboundQueueSizeLimit(@Nonnegative int maxOutboundQueueSize) {
    _maxOutboundQueueSize = maxOutboundQueueSize;
  }

  public int getOutboundQueueSizeLimit() {
    return _maxOutboundQueueSize;
  }

  /**
   * The length of the outbound queue. This queue includes both {@linkplain io.netty.buffer.ByteBuf} and
   * {@linkplain ChannelPromise} objects. This operation is O(1) since the queue is an {@linkplain ArrayDeque}.
   * @return number of objects in the queue
   */
  public int getOutboundQueueSize() {
    return _outboundBufAndListenerPairs != null ? _outboundBufAndListenerPairs.size() : 0;
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    int maxOutboundQueueSize = getOutboundQueueSizeLimit();
    if (maxOutboundQueueSize > 0 && getOutboundQueueSize() > maxOutboundQueueSize) {
      promise.tryFailure(OUTBOUND_QUEUE_OVERFLOW);
      return;
    }
    super.write(ctx, msg, promise);
  }

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
    super.handlerAdded(ctx);

    Object pendingUnencryptedWrites = PENDING_UNENCRYPTED_WRITES.get(this);
    Object bufAndListenerPairsObject = BUF_AND_LISTENER_PAIRS.get(pendingUnencryptedWrites);

    // noinspection unchecked
    _outboundBufAndListenerPairs = (ArrayDeque<Object>) bufAndListenerPairsObject;
  }

  public static class OverflowException extends IOException {
    public OverflowException(String message) {
      super(message);
    }
  }

  static {
    try {
      PENDING_UNENCRYPTED_WRITES = SslHandler.class.getDeclaredField("pendingUnencryptedWrites");
      PENDING_UNENCRYPTED_WRITES.setAccessible(true);

      for (Class<?> clazz = PENDING_UNENCRYPTED_WRITES.getType();;) {
        Field bufAndListerPairs;
        try {
          bufAndListerPairs = clazz.getDeclaredField("bufAndListenerPairs");
        } catch (Exception ex) {
          clazz = clazz.getSuperclass();
          if (clazz == Object.class) {
            throw ex;
          }
          continue;
        }
        BUF_AND_LISTENER_PAIRS = bufAndListerPairs;
        break;
      }
      BUF_AND_LISTENER_PAIRS.setAccessible(true);

      OUTBOUND_QUEUE_OVERFLOW = ExceptionUtil.withoutStackTrace(new OverflowException("Outbound queue overflow"));
    } catch (Exception ex) {
      throw new Error(ex);
    }
  }
}
