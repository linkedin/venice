package com.linkedin.venice.listener;

import static io.netty.handler.codec.http.HttpHeaders.Names.*;
import static io.netty.handler.codec.http.HttpVersion.*;
import static org.mockito.Mockito.*;

import com.linkedin.davinci.listener.response.MetadataResponse;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.compression.CompressionStrategy;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.testng.Assert;
import org.testng.annotations.Test;


public class OutboundHttpWrapperHandlerTest {
  @Test
  public void testWriteMetadataResponse() {
    MetadataResponse msg = new MetadataResponse();
    msg.setVersions(Collections.emptyList());
    StatsHandler statsHandler = mock(StatsHandler.class);
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);

    FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, HttpResponseStatus.OK, msg.getResponseBody());
    response.headers().set(CONTENT_TYPE, HttpConstants.AVRO_BINARY);
    response.headers().set(CONTENT_LENGTH, msg.getResponseBody().readableBytes());
    response.headers().set(HttpConstants.VENICE_COMPRESSION_STRATEGY, CompressionStrategy.NO_OP.getValue());
    response.headers().set(HttpConstants.VENICE_SCHEMA_ID, msg.getResponseSchemaIdHeader());
    response.headers().set(HttpConstants.VENICE_REQUEST_RCU, 1);

    OutboundHttpWrapperHandler outboundHttpWrapperHandler = new OutboundHttpWrapperHandler(statsHandler);

    when(mockCtx.writeAndFlush(any())).then(i -> {
      FullHttpResponse actualResponse = (DefaultFullHttpResponse) i.getArguments()[0];
      Assert.assertEquals(actualResponse.content(), response.content());
      Assert.assertTrue(actualResponse.headers().equals(response.headers()));
      Assert.assertTrue(actualResponse.equals(response));
      return null;
    });

    outboundHttpWrapperHandler.write(mockCtx, msg, null);
  }

  @Test
  public void testWriteMetadataErrorResponse() {
    MetadataResponse msg = new MetadataResponse();
    msg.setError(true);
    msg.setMessage("test-error");
    ByteBuf body = Unpooled.wrappedBuffer(msg.getMessage().getBytes(StandardCharsets.UTF_8));
    StatsHandler statsHandler = mock(StatsHandler.class);
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);

    FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR, body);
    response.headers().set(CONTENT_TYPE, HttpConstants.TEXT_PLAIN);
    response.headers().set(CONTENT_LENGTH, body.readableBytes());
    response.headers().set(HttpConstants.VENICE_COMPRESSION_STRATEGY, CompressionStrategy.NO_OP.getValue());
    response.headers().set(HttpConstants.VENICE_SCHEMA_ID, -1);
    response.headers().set(HttpConstants.VENICE_REQUEST_RCU, 1);

    OutboundHttpWrapperHandler outboundHttpWrapperHandler = new OutboundHttpWrapperHandler(statsHandler);

    when(mockCtx.writeAndFlush(any())).then(i -> {
      FullHttpResponse actualResponse = (DefaultFullHttpResponse) i.getArguments()[0];
      Assert.assertEquals(actualResponse.content(), response.content());
      Assert.assertTrue(actualResponse.headers().equals(response.headers()));
      Assert.assertTrue(actualResponse.equals(response));
      return null;
    });

    outboundHttpWrapperHandler.write(mockCtx, msg, null);
  }

  @Test
  public void testWriteMetadataUnknownErrorResponse() {
    MetadataResponse msg = new MetadataResponse();
    msg.setError(true);
    ByteBuf body = Unpooled.wrappedBuffer("Unknown error".getBytes(StandardCharsets.UTF_8));
    StatsHandler statsHandler = mock(StatsHandler.class);
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);

    FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR, body);
    response.headers().set(CONTENT_TYPE, HttpConstants.TEXT_PLAIN);
    response.headers().set(CONTENT_LENGTH, body.readableBytes());
    response.headers().set(HttpConstants.VENICE_COMPRESSION_STRATEGY, CompressionStrategy.NO_OP.getValue());
    response.headers().set(HttpConstants.VENICE_SCHEMA_ID, -1);
    response.headers().set(HttpConstants.VENICE_REQUEST_RCU, 1);

    OutboundHttpWrapperHandler outboundHttpWrapperHandler = new OutboundHttpWrapperHandler(statsHandler);

    when(mockCtx.writeAndFlush(any())).then(i -> {
      FullHttpResponse actualResponse = (DefaultFullHttpResponse) i.getArguments()[0];
      Assert.assertEquals(actualResponse.content(), response.content());
      Assert.assertTrue(actualResponse.headers().equals(response.headers()));
      Assert.assertTrue(actualResponse.equals(response));
      return null;
    });

    outboundHttpWrapperHandler.write(mockCtx, msg, null);
  }

  @Test
  public void testWriteDefaultFullHttpResponse() {
    FullHttpResponse msg = new DefaultFullHttpResponse(HTTP_1_1, HttpResponseStatus.OK);
    StatsHandler statsHandler = mock(StatsHandler.class);
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);

    FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, HttpResponseStatus.OK);

    OutboundHttpWrapperHandler outboundHttpWrapperHandler = new OutboundHttpWrapperHandler(statsHandler);

    when(mockCtx.writeAndFlush(any())).then(i -> {
      FullHttpResponse actualResponse = (DefaultFullHttpResponse) i.getArguments()[0];
      Assert.assertEquals(actualResponse.content(), response.content());
      Assert.assertTrue(actualResponse.headers().equals(response.headers()));
      Assert.assertTrue(actualResponse.equals(response));
      return null;
    });

    outboundHttpWrapperHandler.write(mockCtx, msg, null);
  }
}
