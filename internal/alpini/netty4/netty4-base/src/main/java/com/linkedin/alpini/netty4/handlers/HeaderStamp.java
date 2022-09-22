package com.linkedin.alpini.netty4.handlers;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpResponse;
import java.util.Objects;
import javax.annotation.Nonnull;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
@ChannelHandler.Sharable
public class HeaderStamp extends ChannelOutboundHandlerAdapter {
  private final String _key;
  private final String _value;

  public HeaderStamp(@Nonnull String headerName, @Nonnull String headerValue) {
    _key = Objects.requireNonNull(headerName, "headerName");
    _value = Objects.requireNonNull(headerValue, "headerValue");
  }

  /**
   * Calls {@link ChannelHandlerContext#write(Object, ChannelPromise)} to forward
   * to the next {@link io.netty.channel.ChannelOutboundHandler} in the {@link io.netty.channel.ChannelPipeline}.
   * <p>
   * Sub-classes may override this method to change behavior.
   */
  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    if (msg instanceof HttpResponse) {
      HttpResponse response = (HttpResponse) msg;
      response.headers().add(_key, _value);
    }

    super.write(ctx, msg, promise);
  }
}
