package com.linkedin.venice.router;

import com.linkedin.ddsstorage.netty4.misc.BasicFullHttpRequest;
import com.linkedin.venice.exceptions.QuotaExceededException;
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
import org.apache.log4j.Logger;

import static com.linkedin.venice.router.api.VenicePathParser.*;
import static com.linkedin.venice.utils.NettyUtils.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;


@ChannelHandler.Sharable
public class RouterThrottleHandler extends SimpleChannelInboundHandler<HttpRequest> {
  public static final AttributeKey<byte[]> THROTTLE_HANDLER_BYTE_ATTRIBUTE_KEY =
      AttributeKey.valueOf("THROTTLE_HANDLER_BYTE_ATTRIBUTE_KEY");
  private static final Logger logger = Logger.getLogger(RouterThrottleHandler.class);
  private static final byte[] EMPTY_BYTES = new byte[0];

  private static RedundantExceptionFilter filter = RedundantExceptionFilter.getRedundantExceptionFilter();

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
    if (!config.isEarlyThrottleEnabled() || msg.method().equals(HttpMethod.OPTIONS) || !(msg instanceof BasicFullHttpRequest)) {
      // Pass request to the next channel
      ReferenceCountUtil.retain(msg);
      ctx.fireChannelRead(msg);
      return;
    }

    VenicePathParserHelper helper = new VenicePathParserHelper(msg.uri());
    // Only throttle storage requests
    // TODO: Support compute operation throttling through key count being passed from the client in http header. Also for multi-get fall back to this
    // only if header does not contain the key count
    if (helper.getResourceType().equals(TYPE_STORAGE)) {
      try {
        int keyCount;
        BasicFullHttpRequest basicFullHttpRequest = (BasicFullHttpRequest)msg;

        // single-get
        if (VeniceRouterUtils.isHttpGet(msg.method().name())) {
          keyCount = 1;
        } else { // batch-get requests
          ByteBuf byteBuf = basicFullHttpRequest.content();
          byte[] bytes = new byte[byteBuf.readableBytes()];
          int readerIndex = byteBuf.readerIndex();

          byteBuf.getBytes(readerIndex, bytes);
          OptimizedBinaryDecoder binaryDecoder =
              OptimizedBinaryDecoderFactory.defaultFactory().createOptimizedBinaryDecoder(bytes, 0, bytes.length);
          keyCount = getKeyCount(binaryDecoder);
          // Reuse the byte array in VeniceMultiGetPath
          basicFullHttpRequest.attr(THROTTLE_HANDLER_BYTE_ATTRIBUTE_KEY).set(bytes);
        }
        throttler.maybeThrottle(keyCount);
      } catch (QuotaExceededException e) {
        routerStats.recordRouterThrottledRequest();
        String errorMessage = "Total router read quota has been exceeded. Resource name: " + helper.getResourceName();
        if (!filter.isRedundantException(errorMessage)) {
          logger.warn(errorMessage);
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
    InetSocketAddress sockAddr = (InetSocketAddress)(ctx.channel().remoteAddress());
    String remoteAddr = sockAddr.getHostName() + ":" + sockAddr.getPort();
    if (!filter.isRedundantException(sockAddr.getHostName(), e)) {
      logger.error("Got exception while throttling request from " + remoteAddr + ": ", e);
    }
    setupResponseAndFlush(INTERNAL_SERVER_ERROR, EMPTY_BYTES, false, ctx);
  }
}
