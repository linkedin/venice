package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.netty4.misc.ChannelTaskSerializer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.handler.codec.http.HttpContent;


/**
 * @deprecated broken. use the content chunking in HttpMultiPartContentEncoder
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
@Deprecated
@ChannelHandler.Sharable
public class ChunkedResponseLimiter extends ChannelInitializer<Channel> {
  private final int _maxChunkSize;
  private final int _chunkSize;

  public ChunkedResponseLimiter() {
    this(8192, 3072);
  }

  public ChunkedResponseLimiter(int maxChunkSize, int chunkSize) {
    if (chunkSize > maxChunkSize || chunkSize < 512) {
      throw new IllegalArgumentException("chunk size must be at least 512 and less than maxChunkSize");
    }
    _maxChunkSize = maxChunkSize;
    _chunkSize = chunkSize;
  }

  @Override
  protected void initChannel(Channel ch) throws Exception {
    ch.pipeline().replace(this, "chunked-response-limiter", new Handler());
  }

  class Handler extends ChannelOutboundHandlerAdapter {
    private ChannelTaskSerializer _serializer;
    private ChannelFuture _afterWrite;

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
      _serializer = new ChannelTaskSerializer(ctx);
      _afterWrite = ctx.newSucceededFuture();
    }

    /**
     * Calls {@link ChannelHandlerContext#write(Object, ChannelPromise)} to forward
     * to the next {@link io.netty.channel.ChannelOutboundHandler} in the {@link io.netty.channel.ChannelPipeline}.
     * <p>
     * Sub-classes may override this method to change behavior.
     *
     * @param ctx
     * @param msg
     * @param promise
     */
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
      ChannelPromise afterWrite = ctx.newPromise();
      _serializer.executeTask(completion -> {
        try {
          write0(ctx, msg, promise);
        } catch (Exception ex) {
          promise.setFailure(ex);
        } finally {
          afterWrite.setSuccess();
          completion.setSuccess();
        }
      }, future -> {});
      _afterWrite = afterWrite;
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception {
      if (_afterWrite.isDone()) {
        super.flush(ctx);
      } else {
        _afterWrite.addListener(ignored -> super.flush(ctx));
      }
    }

    private void write0(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
      if (msg instanceof HttpContent && !(msg instanceof FullHttpMessage)) {
        HttpContent chunk = (HttpContent) msg;
        if (chunk.content().readableBytes() > _maxChunkSize) {
          ByteBuf data = chunk.content().duplicate();
          do {
            ctx.write(new DefaultHttpContent(data.readBytes(_chunkSize)));
          } while (data.readableBytes() > _chunkSize);
          msg = chunk.replace(data.readBytes(data.readableBytes()));
          chunk.release();
        }
      }
      super.write(ctx, msg, promise);
    }
  }
}
