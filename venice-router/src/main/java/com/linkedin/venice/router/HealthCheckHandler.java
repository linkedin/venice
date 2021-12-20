package com.linkedin.venice.router;

import com.linkedin.venice.router.api.VenicePathParserHelper;
import com.linkedin.venice.router.stats.HealthCheckStats;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.Utils;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.util.ReferenceCountUtil;
import java.net.InetSocketAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.linkedin.venice.router.api.VenicePathParser.*;
import static com.linkedin.venice.router.api.VenicePathParserHelper.*;
import static com.linkedin.venice.utils.NettyUtils.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;


@ChannelHandler.Sharable
public class HealthCheckHandler extends SimpleChannelInboundHandler<HttpRequest> {
  private static final Logger logger = LogManager.getLogger(HealthCheckHandler.class);
  private static final byte[] EMPTY_BYTES = new byte[0];
  private static final RedundantExceptionFilter filter = RedundantExceptionFilter.getRedundantExceptionFilter();

  private final HealthCheckStats healthCheckStats;

  public HealthCheckHandler(HealthCheckStats healthCheckStats) {
    this.healthCheckStats = healthCheckStats;
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, HttpRequest msg) {
    // Check whether it's a health check request
    boolean isHealthCheck = false;
    if (msg.method().equals(HttpMethod.OPTIONS)) {
      isHealthCheck = true;
    } else if (msg.method().equals(HttpMethod.GET)) {
      VenicePathParserHelper helper = parseRequest(msg);

      if (TYPE_HEALTH_CHECK.equals(helper.getResourceType()) && Utils.isNullOrEmpty(helper.getResourceName())) {
        isHealthCheck = true;
      }
    }

    if (isHealthCheck) {
      healthCheckStats.recordHealthCheck();
      setupResponseAndFlush(OK, EMPTY_BYTES, false, ctx);
    } else {
      // Pass request to the next channel if it's not a health check
      ReferenceCountUtil.retain(msg);
      ctx.fireChannelRead(msg);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) {
    healthCheckStats.recordErrorHealthCheck();
    InetSocketAddress sockAddr = (InetSocketAddress)(ctx.channel().remoteAddress());
    String remoteAddr = sockAddr.getHostName() + ":" + sockAddr.getPort();
    if (!filter.isRedundantException(sockAddr.getHostName(), e)) {
      logger.error("Got exception while handling health check request from " + remoteAddr + ", and error: " + e.getMessage());
    }
    setupResponseAndFlush(INTERNAL_SERVER_ERROR, EMPTY_BYTES, false, ctx);
    ctx.close();
  }
}
