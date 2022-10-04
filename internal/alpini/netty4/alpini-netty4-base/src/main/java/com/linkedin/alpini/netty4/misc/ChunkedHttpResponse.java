package com.linkedin.alpini.netty4.misc;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.concurrent.Promise;
import java.io.IOException;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public interface ChunkedHttpResponse extends FullHttpResponse {
  void writeChunkedContent(ChannelHandlerContext ctx, Promise<LastHttpContent> writePromise) throws IOException;
}
