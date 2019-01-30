package com.linkedin.venice.listener;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.listener.response.HttpShortcutResponse;
import com.linkedin.venice.listener.response.ReadResponse;
import com.linkedin.venice.utils.ExceptionUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.nio.charset.StandardCharsets;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/***
 * wraps raw bytes into an HTTP response object that HttpServerCodec expects
 */

public class OutboundHttpWrapperHandler extends ChannelOutboundHandlerAdapter {
  private final StatsHandler statsHandler;

  public OutboundHttpWrapperHandler(StatsHandler handler) {
    super();
    statsHandler = handler;
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    ByteBuf body;
    String contentType = HttpConstants.AVRO_BINARY;
    HttpResponseStatus responseStatus = OK;
    String offsetHeader = "-1";
    int schemaIdHeader = -1;
    CompressionStrategy compressionStrategy = CompressionStrategy.NO_OP;
    try {
      if (msg instanceof ReadResponse) {
        ReadResponse obj = (ReadResponse) msg;
        statsHandler.setDatabaseLookupLatency(obj.getDatabaseLookupLatency());
        statsHandler.setStorageExecutionHandlerSubmissionWaitTime(obj.getStorageExecutionHandlerSubmissionWaitTime());
        statsHandler.setSuccessRequestKeyCount(obj.getRecordCount());
        statsHandler.setMultiChunkLargeValueCount(obj.getMultiChunkLargeValueCount());
        statsHandler.setReadComputeLatency(obj.getReadComputeLatency());
        statsHandler.setReadComputeDeserializationLatency(obj.getReadComputeDeserializationLatency());
        statsHandler.setReadComputeSerializationLatency(obj.getReadComputeSerializationLatency());
        compressionStrategy = obj.getCompressionStrategy();
        if (obj.isFound()) {
          body = obj.getResponseBody();
          offsetHeader = obj.getResponseOffsetHeader();
          schemaIdHeader = obj.getResponseSchemaIdHeader();
        } else {
          body = Unpooled.EMPTY_BUFFER;
          responseStatus = NOT_FOUND;
        }
      } else if (msg instanceof HttpShortcutResponse) {
        responseStatus = ((HttpShortcutResponse) msg).getStatus();
        body = Unpooled.wrappedBuffer(((HttpShortcutResponse) msg).getMessage().getBytes(StandardCharsets.UTF_8));
        contentType = HttpConstants.TEXT_PLAIN;
      } else if (msg instanceof DefaultFullHttpResponse){
        ctx.writeAndFlush(msg);
        return;
      } else {
        responseStatus = INTERNAL_SERVER_ERROR;
        body = Unpooled.wrappedBuffer(
            "Internal Server Error: Unrecognized object in OutboundHttpWrapperHandler".getBytes(StandardCharsets.UTF_8));
        contentType = HttpConstants.TEXT_PLAIN;
      }
    } catch (Exception e) {
      responseStatus = INTERNAL_SERVER_ERROR;
      body = Unpooled.wrappedBuffer(("Internal Server Error:\n\n" + ExceptionUtils.stackTraceToString(e)
          + "\n(End of server-side stacktrace)\n").getBytes(StandardCharsets.UTF_8));
      contentType = HttpConstants.TEXT_PLAIN;
    } finally {
      statsHandler.setResponseStatus(responseStatus);
    }

    FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, responseStatus, body);
    response.headers().set(CONTENT_TYPE, contentType);
    response.headers().set(CONTENT_LENGTH, body.readableBytes());
    response.headers().set(HttpConstants.VENICE_COMPRESSION_STRATEGY, compressionStrategy.getValue());
    response.headers().set(HttpConstants.VENICE_OFFSET, offsetHeader);
    response.headers().set(HttpConstants.VENICE_SCHEMA_ID, schemaIdHeader);

    /** {@link io.netty.handler.timeout.IdleStateHandler} is in charge of detecting the state
     *  of connection, and {@link RouterRequestHttpHandler} will close the connection if necessary.
     *
     *  writeAndFlush may have some performance issue since it will call the actual send every time.
     */
    ctx.writeAndFlush(response);
  }
}
