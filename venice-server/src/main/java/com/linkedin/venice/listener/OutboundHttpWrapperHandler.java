package com.linkedin.venice.listener;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.listener.response.MultiGetResponseWrapper;
import com.linkedin.venice.listener.response.HttpShortcutResponse;
import com.linkedin.venice.listener.response.StorageResponseObject;
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
    String offset = "-1";
    int schemaId = -1;
    String partitionOffsets = null;
    if (msg instanceof StorageResponseObject) {
      StorageResponseObject obj = (StorageResponseObject) msg;
      statsHandler.setBdbQueryLatency(obj.getBdbQueryLatency());
      offset = Long.toString(obj.getOffset());
      if (obj.isFound()) {
        body = obj.getValueRecord().getData();
        schemaId = obj.getValueRecord().getSchemaId();
        statsHandler.setSuccessRequestKeyCount(1);
      } else {
        // not found
        body = Unpooled.EMPTY_BUFFER;
        responseStatus = NOT_FOUND;
      }
    } else if (msg instanceof MultiGetResponseWrapper) {
      MultiGetResponseWrapper response = (MultiGetResponseWrapper) msg;
      statsHandler.setBdbQueryLatency(response.getBdbQueryLatency());
      statsHandler.setSuccessRequestKeyCount(response.getRecordCount());
      body = Unpooled.wrappedBuffer(response.serializedMultiGetResponse());
      schemaId = response.getResponseSchemaId();
      offset = response.serializedPartitionOffsetMap();
    } else if (msg instanceof HttpShortcutResponse) {
      responseStatus = ((HttpShortcutResponse) msg).getStatus();
      body = Unpooled.wrappedBuffer(((HttpShortcutResponse) msg).getMessage().getBytes(StandardCharsets.UTF_8));
      contentType = HttpConstants.TEXT_PLAIN;
    } else if (msg instanceof DefaultFullHttpResponse){
      ctx.writeAndFlush(msg);
      return;
    } else {
      responseStatus = INTERNAL_SERVER_ERROR;
      body = Unpooled.wrappedBuffer("Unrecognized object in OutboundHttpWrapperHandler".getBytes(StandardCharsets.UTF_8));
      contentType = HttpConstants.TEXT_PLAIN;
    }

    statsHandler.setResponseStatus(responseStatus);
    FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, responseStatus, body);
    response.headers().set(CONTENT_TYPE, contentType);
    response.headers().set(CONTENT_LENGTH, body.readableBytes());
    response.headers().set(HttpConstants.VENICE_OFFSET, offset);
    response.headers().set(HttpConstants.VENICE_SCHEMA_ID, schemaId);

    /** {@link io.netty.handler.timeout.IdleStateHandler} is in charge of detecting the state
     *  of connection, and {@link GetRequestHttpHandler} will close the connection if necessary.
     *
     *  writeAndFlush may have some performance issue since it will call the actual send every time.
     */
    ctx.writeAndFlush(response);
  }
}
