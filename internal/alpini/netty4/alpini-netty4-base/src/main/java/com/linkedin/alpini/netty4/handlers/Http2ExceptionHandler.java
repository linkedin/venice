package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.base.misc.ExceptionUtil;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.WriteTimeoutException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is ExceptionHandler for all HTTP/2.
 *
 * @author Abhishek Andhavarapu
 */
@ChannelHandler.Sharable
public class Http2ExceptionHandler extends ChannelInboundHandlerAdapter {
  private static final Logger LOG = LogManager.getLogger(Http2ExceptionHandler.class);

  public static final Http2ExceptionHandler INSTANCE = new Http2ExceptionHandler();

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    if (ExceptionUtil.unwrap(cause, WriteTimeoutException.class) != null) {
      // Write to the client timeout
      LOG.error("Couldn't write back to the client {} with in the timeout", ctx.channel());
      return;
    }
    super.exceptionCaught(ctx, cause);
  }
}
