package com.linkedin.venice.router.streaming;

import com.linkedin.alpini.netty4.misc.BasicFullHttpRequest;
import com.linkedin.venice.exceptions.VeniceException;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import java.util.Optional;


/**
 * This class is the wrapper of {@link ChunkedWriteHandler}, and essentially it will introduce the following logic:
 * 1. Register the {@link ChannelHandlerContext} and {@link VeniceChunkedWriteHandler} in the request since those
 *    objects are not available to Venice directly when using DDS lib;
 * 2. Intercept all the writes to delegate the pre-processing to {@link VeniceChunkedResponse} before proceeding;
 */
public class VeniceChunkedWriteHandler extends NettyChunkedWriteHandler {
  /**
   * Callback function when {@link VeniceChunkedWriteHandler} receives any {@link #write(ChannelHandlerContext, Object, ChannelPromise)}.
   */
  private Optional<WriteMessageCallback> writeMessageCallback = Optional.empty();

  public static final AttributeKey<VeniceChunkedWriteHandler> CHUNKED_WRITE_HANDLER_ATTRIBUTE_KEY =
      AttributeKey.valueOf("CHUNKED_WRITE_HANDLER_ATTRIBUTE_KEY");
  public static final AttributeKey<ChannelHandlerContext> CHANNEL_HANDLER_CONTEXT_ATTRIBUTE_KEY =
      AttributeKey.valueOf("CHANNEL_HANDLER_CONTEXT_ATTRIBUTE_KEY");

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (!(msg instanceof BasicFullHttpRequest)) {
      throw new VeniceException("The incoming request must be an instance of BasicFullHttpRequest");
    }
    BasicFullHttpRequest fullHttpRequest = (BasicFullHttpRequest) msg;
    /**
     * Register those objects in request, so that they could be retrieved in {@link com.linkedin.venice.router.api.VenicePathParser}.
     */
    fullHttpRequest.attr(CHUNKED_WRITE_HANDLER_ATTRIBUTE_KEY).set(this);
    fullHttpRequest.attr(CHANNEL_HANDLER_CONTEXT_ATTRIBUTE_KEY).set(ctx);

    // reset callback
    writeMessageCallback = Optional.empty();

    ctx.fireChannelRead(msg);
  }

  public interface WriteMessageCallback {
    boolean whetherToSkipMessage(Object msg, ChannelPromise promise);
  }

  public void setWriteMessageCallback(WriteMessageCallback callback) {
    if (this.writeMessageCallback.isPresent()) {
      throw new VeniceException("'writeMessageCallback' has already been setup");
    }
    this.writeMessageCallback = Optional.of(callback);
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
    if (writeMessageCallback.isPresent()) {
      /**
       * Delegate the pre-processing to the registered {@link #writeMessageCallback}.
       */
      boolean whetherToSkip = writeMessageCallback.get().whetherToSkipMessage(msg, promise);
      if (whetherToSkip) {
        return;
      }
    }
    try {
      // pass the write to the downstream handlers.
      super.write(ctx, msg, promise);
    } catch (Exception e) {
      ReferenceCountUtil.release(msg);
      promise.setFailure(e);
    }
  }
}
