package com.linkedin.venice.router;

import static com.linkedin.venice.HttpConstants.VENICE_KEY_COUNT;
import static com.linkedin.venice.router.api.VenicePathParserHelper.parseRequest;
import static com.linkedin.venice.utils.NettyUtils.setupResponseAndFlush;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.TOO_MANY_REQUESTS;

import com.linkedin.alpini.netty4.misc.BasicFullHttpRequest;
import com.linkedin.venice.exceptions.QuotaExceededException;
import com.linkedin.venice.router.api.RouterResourceType;
import com.linkedin.venice.router.api.VenicePathParserHelper;
import com.linkedin.venice.router.stats.RouterThrottleStats;
import com.linkedin.venice.router.utils.VeniceRouterUtils;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.NettyUtils;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.apache.avro.io.OptimizedBinaryDecoder;
import org.apache.avro.io.OptimizedBinaryDecoderFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


@ChannelHandler.Sharable
public class RouterThrottleHandler extends SimpleChannelInboundHandler<HttpRequest> {
  public static final AttributeKey<byte[]> THROTTLE_HANDLER_BYTE_ATTRIBUTE_KEY =
      AttributeKey.valueOf("THROTTLE_HANDLER_BYTE_ATTRIBUTE_KEY");
  private static final Logger LOGGER = LogManager.getLogger(RouterThrottleHandler.class);
  private static final byte[] EMPTY_BYTES = new byte[0];

  private static final RedundantExceptionFilter EXCEPTION_FILTER =
      RedundantExceptionFilter.getRedundantExceptionFilter();

  private final RouterThrottleStats routerStats;
  private final EventThrottler throttler;
  private final VeniceRouterConfig config;

  public RouterThrottleHandler(RouterThrottleStats routerStats, EventThrottler throttler, VeniceRouterConfig config) {
    this.routerStats = routerStats;
    this.throttler = throttler;
    this.config = config;
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, HttpRequest msg) throws IOException {
    if (!config.isEarlyThrottleEnabled() || msg.method().equals(HttpMethod.OPTIONS)
        || !(msg instanceof BasicFullHttpRequest)) {
      // Pass request to the next channel
      ReferenceCountUtil.retain(msg);
      ctx.fireChannelRead(msg);
      return;
    }
    VenicePathParserHelper helper = parseRequest(msg);

    if (helper.getResourceType() == RouterResourceType.TYPE_STORAGE
        || helper.getResourceType() == RouterResourceType.TYPE_COMPUTE) {
      try {
        int keyCount;

        // single-get
        if (VeniceRouterUtils.isHttpGet(msg.method().name())) {
          keyCount = 1;
        } else { // batch-get/compute requests
          BasicFullHttpRequest basicFullHttpRequest = (BasicFullHttpRequest) msg;
          CharSequence keyCountsHeader = basicFullHttpRequest.getRequestHeaders().get(VENICE_KEY_COUNT);
          if (keyCountsHeader != null) {
            keyCount = Integer.parseInt((String) keyCountsHeader);
          } else if (helper.getResourceType() == RouterResourceType.TYPE_STORAGE) {
            ByteBuf byteBuf = basicFullHttpRequest.content();
            byte[] bytes = new byte[byteBuf.readableBytes()];
            int readerIndex = byteBuf.readerIndex();

            byteBuf.getBytes(readerIndex, bytes);
            OptimizedBinaryDecoder binaryDecoder =
                OptimizedBinaryDecoderFactory.defaultFactory().createOptimizedBinaryDecoder(bytes, 0, bytes.length);
            keyCount = getKeyCount(binaryDecoder);
            // Reuse the byte array in VeniceMultiGetPath
            basicFullHttpRequest.attr(THROTTLE_HANDLER_BYTE_ATTRIBUTE_KEY).set(bytes);
          } else {
            // Pass request to the next channel for compute request with older client
            ReferenceCountUtil.retain(msg);
            ctx.fireChannelRead(msg);
            return;
          }
        }
        throttler.maybeThrottle(keyCount);
      } catch (QuotaExceededException e) {
        routerStats.recordRouterThrottledRequest();
        String errorMessage = "Total router read quota has been exceeded. Resource name: " + helper.getResourceName();
        if (!EXCEPTION_FILTER.isRedundantException(errorMessage)) {
          LOGGER.warn(errorMessage);
        }
        NettyUtils.setupResponseAndFlush(TOO_MANY_REQUESTS, new byte[0], false, ctx);
        return;
      }
    }

    // Pass request to the next channel
    ReferenceCountUtil.retain(msg);
    ctx.fireChannelRead(msg);
  }

  /**
   * Return number of elements in Avro serialized array of records.
   * @return
   * @throws IOException
   */
  public int getKeyCount(OptimizedBinaryDecoder binaryDecoder) throws IOException {
    int count = 0;

    while (binaryDecoder.inputStream().available() > 0) {
      int bytesLength = binaryDecoder.readInt();
      binaryDecoder.skipFixed(bytesLength);
      count++;
    }
    return count;
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) {
    InetSocketAddress sockAddr = (InetSocketAddress) (ctx.channel().remoteAddress());
    String remoteAddr = sockAddr.getHostName() + ":" + sockAddr.getPort();
    if (!EXCEPTION_FILTER.isRedundantException(sockAddr.getHostName(), e)) {
      LOGGER.error("Got exception while throttling request from {}. ", remoteAddr, e);
    }
    setupResponseAndFlush(INTERNAL_SERVER_ERROR, EMPTY_BYTES, false, ctx);
  }
}
