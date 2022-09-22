package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.netty4.misc.BasicHttpResponse;
import com.linkedin.alpini.netty4.misc.ChannelTaskSerializer;
import com.linkedin.alpini.netty4.misc.ChunkedHttpResponse;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import javax.annotation.Nonnull;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
@ChannelHandler.Sharable
public class ChunkedResponseHandler extends ChannelInitializer<Channel> {
  @Override
  protected void initChannel(Channel ch) throws Exception {
    ch.pipeline().replace(this, "chunked-response-handler", new Handler());
  }

  static class Handler extends ChannelOutboundHandlerAdapter {
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
      if (msg instanceof ChunkedHttpResponse && ((ChunkedHttpResponse) msg).content().isReadable()) {
        throw new IllegalStateException("Chunked responses must not have content");
      }

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
      _afterWrite = afterWrite.isDone() ? ctx.newSucceededFuture() : afterWrite;
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception {
      ChannelFuture afterWrite = _afterWrite;
      if (afterWrite.isDone()) {
        super.flush(ctx);
      } else {
        afterWrite.addListener(ignored -> super.flush(ctx));
      }
    }

    private void write0(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
      if (msg instanceof ChunkedHttpResponse) {
        ChunkedHttpResponse response = (ChunkedHttpResponse) msg;
        response.headers().remove(HttpHeaderNames.CONTENT_LENGTH);
        HttpUtil.setTransferEncodingChunked(response, true);
        ctx.writeAndFlush(new ChunkedResponseHeader(response)).addListener(future -> {
          if (future.isSuccess()) {
            response.writeChunkedContent(ctx, ctx.executor().<LastHttpContent>newPromise().addListener(f -> {
              if (f.isSuccess()) {
                ctx.writeAndFlush(f.getNow(), promise);
              } else {
                promise.setFailure(f.cause());
              }
            }));
          } else {
            promise.setFailure(future.cause());
          }
        });
        response.release();
      } else {
        super.write(ctx, msg, promise);
      }
    }
  }

  private static class ChunkedResponseHeader extends BasicHttpResponse {
    private final HttpResponse _response;

    private ChunkedResponseHeader(@Nonnull HttpResponse response) {
      super(response);
      _response = response;
    }

    /**
     * Returns the status of this {@link HttpResponse}.
     *
     * @return The {@link HttpResponseStatus} of this {@link HttpResponse}
     */
    @Override
    public HttpResponseStatus status() {
      return _response.status();
    }

    /**
     * Set the status of this {@link HttpResponse}.
     *
     * @param status
     */
    @Override
    public HttpResponse setStatus(HttpResponseStatus status) {
      _response.setStatus(status);
      return this;
    }

    /**
     * Returns the protocol version of this {@link HttpResponse}
     */
    @Override
    public HttpVersion protocolVersion() {
      return _response.protocolVersion();
    }

    @Override
    public HttpResponse setProtocolVersion(HttpVersion version) {
      _response.setProtocolVersion(version);
      return this;
    }

    /**
     * Returns the result of decoding this object.
     */
    @Override
    public DecoderResult decoderResult() {
      return _response.decoderResult();
    }

    /**
     * Updates the result of decoding this object. This method is supposed to be invoked by a decoder.
     * Do not call this method unless you know what you are doing.
     *
     * @param result
     */
    @Override
    public void setDecoderResult(DecoderResult result) {
      _response.setDecoderResult(result);
    }
  }
}
