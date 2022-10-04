package com.linkedin.alpini.netty4.compression;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.HttpContentDecompressor;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class ContentDecompressor extends HttpContentDecompressor {
  @Override
  protected EmbeddedChannel newContentDecoder(String contentEncoding) throws Exception {
    if (CompressionUtils.SNAPPY_ENCODING.equalsIgnoreCase(contentEncoding)
        || CompressionUtils.X_SNAPPY.contentEqualsIgnoreCase(contentEncoding)) {
      return new EmbeddedChannel(
          ctx.channel().id(),
          ctx.channel().metadata().hasDisconnect(),
          ctx.channel().config(),
          CompressionUtils.newSnappyDecoder());
    }

    if (CompressionUtils.SNAPPY_FRAMED_ENCODING.equalsIgnoreCase(contentEncoding)) {
      return new EmbeddedChannel(
          ctx.channel().id(),
          ctx.channel().metadata().hasDisconnect(),
          ctx.channel().config(),
          CompressionUtils.newSnappyFramedDecoder());
    }

    return super.newContentDecoder(contentEncoding);
  }
}
