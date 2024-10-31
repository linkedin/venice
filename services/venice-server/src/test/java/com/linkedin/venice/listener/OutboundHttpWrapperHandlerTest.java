package com.linkedin.venice.listener;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.linkedin.davinci.listener.response.MetadataResponse;
import com.linkedin.davinci.listener.response.ReadResponse;
import com.linkedin.davinci.listener.response.ServerCurrentVersionResponse;
import com.linkedin.davinci.listener.response.TopicPartitionIngestionContextResponse;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.listener.grpc.GrpcRequestContext;
import com.linkedin.venice.listener.grpc.handlers.GrpcOutboundResponseHandler;
import com.linkedin.venice.protocols.VeniceServerResponse;
import com.linkedin.venice.utils.ObjectMapperFactory;
import io.grpc.stub.StreamObserver;
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
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();

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
  public void testWriteCurrentVersionResponse() throws JsonProcessingException {
    ServerCurrentVersionResponse msg = new ServerCurrentVersionResponse();
    msg.setCurrentVersion(2);
    StatsHandler statsHandler = mock(StatsHandler.class);
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    ByteBuf body = Unpooled.wrappedBuffer(OBJECT_MAPPER.writeValueAsBytes(msg));
    FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, HttpResponseStatus.OK, body);
    response.headers().set(CONTENT_TYPE, HttpConstants.AVRO_BINARY);
    response.headers().set(CONTENT_LENGTH, body.readableBytes());
    response.headers().set(HttpConstants.VENICE_COMPRESSION_STRATEGY, CompressionStrategy.NO_OP.getValue());
    response.headers().set(HttpConstants.VENICE_SCHEMA_ID, -1);
    response.headers().set(HttpConstants.VENICE_REQUEST_RCU, 1);

    OutboundHttpWrapperHandler outboundHttpWrapperHandler = new OutboundHttpWrapperHandler(statsHandler);

    when(mockCtx.writeAndFlush(any())).then(i -> {
      FullHttpResponse actualResponse = (DefaultFullHttpResponse) i.getArguments()[0];
      Assert.assertEquals(actualResponse.content(), response.content());
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

  @Test
  public void testGrpcWrite() {
    ByteBuf mockBody = mock(ByteBuf.class);

    ReadResponse readResponse = mock(ReadResponse.class);
    when(readResponse.getResponseBody()).thenReturn(mockBody);
    when(readResponse.getCompressionStrategy()).thenReturn(CompressionStrategy.NO_OP);
    when(readResponse.isStreamingResponse()).thenReturn(false);

    GrpcRequestContext context = new GrpcRequestContext(null, VeniceServerResponse.newBuilder(), getStreamObserver());
    context.setReadResponse(readResponse);

    VeniceServerResponse.Builder responseBuilder = VeniceServerResponse.newBuilder();
    ServerStatsContext statsContext = mock(ServerStatsContext.class);
    context.setGrpcStatsContext(statsContext);
    GrpcOutboundResponseHandler grpcHandler = spy(new GrpcOutboundResponseHandler());

    grpcHandler.processRequest(context);

    Assert.assertEquals(ByteString.empty(), responseBuilder.getData());

    verify(grpcHandler).processRequest(context);
  }

  @Test
  public void testWriteTopicPartitionIngestionContextResponse() throws JsonProcessingException {
    TopicPartitionIngestionContextResponse msg = new TopicPartitionIngestionContextResponse();
    String topic = "test_store_v1";
    int expectedPartitionId = 12345;
    String jsonStr = "{\n" + "\"kafkaUrl\" : {\n" + "  TP(topic: \"" + topic + "\", partition: " + expectedPartitionId
        + ") : {\n" + "      \"latestOffset\" : 0,\n" + "      \"offsetLag\" : 1,\n" + "      \"msgRate\" : 2.0,\n"
        + "      \"byteRate\" : 4.0,\n" + "      \"consumerIdx\" : 6,\n"
        + "      \"elapsedTimeSinceLastPollInMs\" : 7\n" + "    }\n" + "  }\n" + "}";
    msg.setTopicPartitionIngestionContext(jsonStr.getBytes());
    StatsHandler statsHandler = mock(StatsHandler.class);
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    ByteBuf body = Unpooled.wrappedBuffer(OBJECT_MAPPER.writeValueAsBytes(msg));
    FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, HttpResponseStatus.OK, body);
    OutboundHttpWrapperHandler outboundHttpWrapperHandler = new OutboundHttpWrapperHandler(statsHandler);

    when(mockCtx.writeAndFlush(any())).then(i -> {
      FullHttpResponse actualResponse = (DefaultFullHttpResponse) i.getArguments()[0];
      Assert.assertEquals(actualResponse.content(), response.content());
      return null;
    });
    outboundHttpWrapperHandler.write(mockCtx, msg, null);
  }

  private StreamObserver<VeniceServerResponse> getStreamObserver() {
    return new StreamObserver<VeniceServerResponse>() {
      @Override
      public void onNext(VeniceServerResponse value) {

      }

      @Override
      public void onError(Throwable t) {

      }

      @Override
      public void onCompleted() {

      }
    };
  }
}
