package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.netty4.misc.HttpMultiPart;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;
import java.util.List;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Aggregates the result of {@link HttpContentMultiPartDecode} into {@link com.linkedin.alpini.netty4.misc.FullHttpMultiPart}
 *
 * This uses {@link HttpMultiPartContentAggregator} to aggregate the {@linkplain com.linkedin.alpini.netty4.misc.FullHttpMultiPart}
 * messages and bypasses the aggregator when not aggregating multipart messages.
 *
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */

public class HttpContentMultiPartAggregator extends MessageToMessageDecoder<HttpObject> {
  private static final Logger LOG = LogManager.getLogger(HttpContentMultiPartAggregator.class);

  private final Aggregator _aggregator;

  private int _inMultiPart;

  public HttpContentMultiPartAggregator(int maxContentLength) {
    _aggregator = new Aggregator(maxContentLength);
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, HttpObject msg, List<Object> out) throws Exception {
    if (msg instanceof HttpMultiPart) {
      _inMultiPart++;
      LOG.log(_inMultiPart > 1 ? Level.ERROR : Level.DEBUG, "inMultiPart {} {}", _inMultiPart, msg);
    }
    if (_inMultiPart > 0) {
      if (msg instanceof LastHttpContent) {
        try {
          LOG.debug("lastMultiPart {} {}", _inMultiPart, msg);
          if (_aggregator.acceptInboundMessage(msg)) {
            _aggregator.decode(ctx, msg, out);
            return;
          }
        } finally {
          _inMultiPart--;
        }
      } else {
        LOG.debug("decode {} {}", _inMultiPart, msg);
        if (_aggregator.acceptInboundMessage(msg)) {
          _aggregator.decode(ctx, msg, out);
          return;
        }
      }
    }
    out.add(ReferenceCountUtil.retain(msg));
  }

  private static class Aggregator extends HttpMultiPartContentAggregator {
    Aggregator(int maxContentLength) {
      super(maxContentLength);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, HttpObject msg, List<Object> out) throws Exception {
      super.decode(ctx, msg, out);
    }
  }
}
