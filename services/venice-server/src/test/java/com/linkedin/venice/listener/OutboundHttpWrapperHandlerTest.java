package com.linkedin.venice.listener;

import static io.netty.handler.codec.http.HttpHeaders.Names.*;
import static io.netty.handler.codec.http.HttpVersion.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.davinci.listener.response.MetadataResponse;
import com.linkedin.davinci.listener.response.ReadResponse;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.listener.grpc.GrpcHandlerContext;
import com.linkedin.venice.listener.grpc.GrpcHandlerPipeline;
import com.linkedin.venice.protocols.VeniceServerResponse;
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
      assertEquals(actualResponse.content(), response.content());
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
      assertEquals(actualResponse.content(), response.content());
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
      assertEquals(actualResponse.content(), response.content());
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
      assertEquals(actualResponse.content(), response.content());
      Assert.assertTrue(actualResponse.headers().equals(response.headers()));
      Assert.assertTrue(actualResponse.equals(response));
      return null;
    });

    outboundHttpWrapperHandler.write(mockCtx, msg, null);
  }

  @Test
  public void testGrpcWrite() {
    ByteBuf mockBody = mock(ByteBuf.class);

    ReadResponse readResponse = mock(ReadResponse.class);
    when(readResponse.getResponseBody()).thenReturn(mockBody);
    when(readResponse.getCompressionStrategy()).thenReturn(CompressionStrategy.NO_OP);

    GrpcHandlerContext context = mock(GrpcHandlerContext.class);
    when(context.getReadResponse()).thenReturn(readResponse);

    VeniceServerResponse.Builder responseBuilder = VeniceServerResponse.newBuilder();
    when(context.getVeniceServerResponseBuilder()).thenReturn(responseBuilder);
    GrpcStatsContext statsContext = mock(GrpcStatsContext.class);
    when(context.getGrpcStatsContext()).thenReturn(statsContext);

    GrpcHandlerPipeline pipeline = mock(GrpcHandlerPipeline.class);

    OutboundHttpWrapperHandler handler = new OutboundHttpWrapperHandler(null);
    handler.grpcWrite(context, pipeline);

    assertEquals(responseBuilder.getCompressionStrategy(), CompressionStrategy.NO_OP.getValue());
    verify(pipeline).processResponse(context);
  }

  @Test
  public void testGrpcRead() {
    // kind of a useless test, grpcRead does nothing in this class, just a pass-thru to next handler in the pipeline
    // if it were a real pipeline.
    GrpcHandlerContext ctx = mock(GrpcHandlerContext.class);
    GrpcHandlerPipeline pipeline = mock(GrpcHandlerPipeline.class);

    OutboundHttpWrapperHandler handler = new OutboundHttpWrapperHandler(null);
    handler.grpcRead(ctx, pipeline);

    verify(pipeline, times(1)).processRequest(ctx);
  }

}
