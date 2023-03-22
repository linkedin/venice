package com.linkedin.venice.router;

import static com.linkedin.venice.router.api.VenicePathParserHelper.parseRequest;
import static com.linkedin.venice.utils.NettyUtils.setupResponseAndFlush;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import com.linkedin.venice.router.api.RouterResourceType;
import com.linkedin.venice.router.api.VenicePathParserHelper;
import com.linkedin.venice.router.stats.HealthCheckStats;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.util.ReferenceCountUtil;
import java.net.InetSocketAddress;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


@ChannelHandler.Sharable
public class HealthCheckHandler extends SimpleChannelInboundHandler<HttpRequest> {
  private static final Logger LOGGER = LogManager.getLogger(HealthCheckHandler.class);
  private static final byte[] EMPTY_BYTES = new byte[0];
  private static final RedundantExceptionFilter EXCEPTION_FILTER =
      RedundantExceptionFilter.getRedundantExceptionFilter();

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

      if (helper.getResourceType() == RouterResourceType.TYPE_ADMIN && StringUtils.isEmpty(helper.getResourceName())) {
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
    InetSocketAddress sockAddr = (InetSocketAddress) (ctx.channel().remoteAddress());
    String remoteAddr = sockAddr.getHostName() + ":" + sockAddr.getPort();
    if (!EXCEPTION_FILTER.isRedundantException(sockAddr.getHostName(), e)) {
      LOGGER.error(
          "Got exception while handling health check request from {}, and error: {}",
          remoteAddr,
          e.getMessage());
    }
    setupResponseAndFlush(INTERNAL_SERVER_ERROR, EMPTY_BYTES, false, ctx);
    ctx.close();
  }
}
