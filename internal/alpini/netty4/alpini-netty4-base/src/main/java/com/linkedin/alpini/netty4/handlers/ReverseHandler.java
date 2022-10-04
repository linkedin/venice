package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.netty4.misc.NettyUtils;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.function.Supplier;
import javax.annotation.Nonnull;


/**
 * Reverse the polarity of the streams!
 *
 * Created by acurtis on 6/26/17.
 */
public class ReverseHandler extends ChannelDuplexHandler {
  private final Supplier<ChannelHandler[]> _handlers;
  private EmbeddedChannel _embeddedChannel;

  public ReverseHandler(@Nonnull ChannelHandler... handlers) {
    this(() -> handlers);
  }

  public ReverseHandler(@Nonnull Supplier<ChannelHandler[]> handlers) {
    _handlers = handlers;
  }

  @Override
  public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
    _embeddedChannel.disconnect().addListener(future -> {
      if (future.isSuccess()) {
        super.disconnect(ctx, promise);
      } else {
        promise.setFailure(future.cause());
      }
    });
  }

  @Override
  public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
    _embeddedChannel.close().addListener(future -> {
      if (future.isSuccess()) {
        super.close(ctx, promise);
      } else {
        promise.setFailure(future.cause());
      }
    });
  }

  @Override
  public void deregister(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
    _embeddedChannel.deregister().addListener(future -> {
      if (future.isSuccess()) {
        super.deregister(ctx, promise);
      } else {
        promise.setFailure(future.cause());
      }
    });
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    _embeddedChannel.writeOneInbound(msg, promise);
  }

  @Override
  public void flush(ChannelHandlerContext ctx) throws Exception {
    _embeddedChannel.flushInbound();
    super.flush(ctx);
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
    _embeddedChannel.pipeline().fireChannelUnregistered();
    super.channelUnregistered(ctx);
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    _embeddedChannel.pipeline().fireChannelActive();
    super.channelActive(ctx);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    _embeddedChannel.pipeline().fireChannelInactive();
    super.channelInactive(ctx);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    _embeddedChannel.writeOutbound(msg);
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    _embeddedChannel.flushOutbound();
    super.channelReadComplete(ctx);
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    _embeddedChannel.pipeline().fireUserEventTriggered(evt);
    super.userEventTriggered(ctx, evt);
  }

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
    _embeddedChannel = new EmbeddedChannel(
        ctx.channel().id(),
        ctx.channel().metadata().hasDisconnect(),
        ctx.channel().config(),
        _handlers.get()) {
      @Override
      protected void handleOutboundMessage(Object msg) {
        ctx.fireChannelRead(msg);
      }

      @Override
      protected void handleInboundMessage(Object msg) {
        ctx.write(msg);
      }
    };
    _embeddedChannel.pipeline().addLast(NettyUtils.executorGroup(ctx.channel()), new ChannelInboundHandlerAdapter() {
      @Override
      public void channelReadComplete(ChannelHandlerContext innerCtx) throws Exception {
        ctx.flush();
        super.channelReadComplete(innerCtx);
      }
    });
    super.handlerAdded(ctx);
  }
}
