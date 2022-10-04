package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.netty4.misc.BasicFullHttpResponse;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;
import java.nio.charset.StandardCharsets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
@ChannelHandler.Sharable
public class VIPRequestHandler extends ChannelInitializer<Channel> {
  public enum State {
    INITIAL,
    /** The router is up and running. */
    RUNNING,
    /** Shutdown has been requested. */
    SHUTDOWN
  }

  private volatile State _state = State.INITIAL;
  private volatile long _stateTS = Time.currentTimeMillis();

  private final String _healthcheckUri;
  private final Logger _log = LogManager.getLogger(getClass());

  /**
   * @param healthcheckUri URI to respond to; typically /<service_name>/admin
   */
  public VIPRequestHandler(String healthcheckUri) {
    _healthcheckUri = healthcheckUri;
    _log.info("Created VIPRequestHandler for healthcheckUri={}", _healthcheckUri);
  }

  @Override
  protected void initChannel(Channel ch) throws Exception {
    ch.pipeline().replace(this, "vip-request-handler", new Handler());
  }

  protected boolean isHealty() {
    return _state == State.RUNNING;
  }

  public State state() {
    return _state;
  }

  public void start() {
    _stateTS = Time.currentTimeMillis();
    _state = State.RUNNING;
  }

  public void shutdown() {
    _stateTS = Time.currentTimeMillis();
    _state = State.SHUTDOWN;
  }

  class Handler extends ChannelInboundHandlerAdapter {
    private boolean _consume;

    /**
     * Calls {@link ChannelHandlerContext#fireChannelActive()} to forward
     * to the next {@link io.netty.channel.ChannelInboundHandler} in the {@link io.netty.channel.ChannelPipeline}.
     * <p>
     * Sub-classes may override this method to change behavior.
     *
     * @param ctx
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
      // If we're shutting down and the VIP is still opening connections, log a warning about it.
      long now;
      if (_state == State.SHUTDOWN && ((now = Time.currentTimeMillis()) - _stateTS) > 5000) { // SUPPRESS CHECKSTYLE
                                                                                              // InnerAssignment
        _log.warn("New connection opened {}ms after entering shutdown mode.", (now - _stateTS));
      }
      super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      if (msg instanceof HttpRequest) {
        HttpRequest request = (HttpRequest) msg;
        if (request.uri().equals(_healthcheckUri) || request.uri().equals("/")) {
          _consume = true;

          // Build the GOOD / BAD response
          FullHttpResponse response;
          if (isHealty()) {
            ByteBuf content = Unpooled.copiedBuffer("GOOD", StandardCharsets.US_ASCII);
            response = new BasicFullHttpResponse(
                request,
                HttpResponseStatus.OK,
                request.method() == HttpMethod.HEAD ? Unpooled.EMPTY_BUFFER : content);
            HttpUtil.setContentLength(response, content.readableBytes());
            HttpUtil.setKeepAlive(response, HttpUtil.isKeepAlive(request));
          } else {
            ByteBuf content = Unpooled.copiedBuffer("BAD", StandardCharsets.US_ASCII);
            response = new BasicFullHttpResponse(
                request,
                HttpResponseStatus.SERVICE_UNAVAILABLE,
                request.method() == HttpMethod.HEAD ? Unpooled.EMPTY_BUFFER : content);
            HttpUtil.setContentLength(response, content.readableBytes());
            HttpUtil.setKeepAlive(response, false);
          }

          if (HttpUtil.is100ContinueExpected(request)) {
            ctx.writeAndFlush(new BasicFullHttpResponse(request, HttpResponseStatus.CONTINUE))
                .addListener((ChannelFutureListener) f -> {
                  if (f.isSuccess()) {
                    writeResponse(ctx, response);
                  } else {
                    ChannelFutureListener.CLOSE_ON_FAILURE.operationComplete(f);
                  }
                });
          } else {
            writeResponse(ctx, response);
          }
        }
      }

      if (_consume) {
        if (msg instanceof LastHttpContent) {
          _consume = false;
        }
        ReferenceCountUtil.release(msg);
      } else {
        super.channelRead(ctx, msg);
      }
    }

    private void writeResponse(ChannelHandlerContext ctx, FullHttpResponse response) {
      ctx.writeAndFlush(response)
          .addListener(
              HttpUtil.isKeepAlive(response) ? ChannelFutureListener.CLOSE_ON_FAILURE : ChannelFutureListener.CLOSE);
    }
  }
}
