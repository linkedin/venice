package com.linkedin.venice.listener.grpc.handlers;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import com.google.protobuf.ByteString;
import com.linkedin.davinci.listener.response.ReadResponse;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.listener.ServerStatsContext;
import com.linkedin.venice.listener.grpc.GrpcRequestContext;
import com.linkedin.venice.listener.response.CountByValueResponseWrapper;
import com.linkedin.venice.protocols.VeniceServerResponse;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;


public class GrpcOutboundResponseHandler extends VeniceServerGrpcHandler {
  @Override
  public void processRequest(GrpcRequestContext ctx) {
    ServerStatsContext statsContext = ctx.getGrpcStatsContext();

    if (ctx.isCountByValueRequest()) {
      // Handle CountByValue responses
      handleCountByValueResponse(ctx, statsContext);
    } else {
      // Handle regular get/batchget responses
      handleRegularResponse(ctx, statsContext);
    }
  }

  private void handleCountByValueResponse(GrpcRequestContext ctx, ServerStatsContext statsContext) {
    ReadResponse obj = ctx.getReadResponse();

    if (ctx.hasError() || obj == null) {
      statsContext.setResponseStatus(HttpResponseStatus.BAD_REQUEST);
      ctx.getCountByValueResponseObserver().onCompleted();
      return;
    }

    if (obj instanceof CountByValueResponseWrapper) {
      CountByValueResponseWrapper wrapper = (CountByValueResponseWrapper) obj;

      // The response is already built in the wrapper, just send it
      try {
        ctx.getCountByValueResponseObserver().onNext(ctx.getCountByValueResponseBuilder().build());
        ctx.getCountByValueResponseObserver().onCompleted();
        statsContext.setResponseStatus(OK);
      } catch (Exception e) {
        ctx.getCountByValueResponseObserver().onError(e);
        statsContext.setResponseStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      }
    } else {
      ctx.getCountByValueResponseObserver()
          .onError(new RuntimeException("Unexpected response type for CountByValue: " + obj.getClass()));
      statsContext.setResponseStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }

    invokeNextHandler(ctx);
  }

  private void handleRegularResponse(GrpcRequestContext ctx, ServerStatsContext statsContext) {
    ByteBuf body;
    CompressionStrategy compressionStrategy = CompressionStrategy.NO_OP;

    ReadResponse obj = ctx.getReadResponse();
    VeniceServerResponse.Builder veniceServerResponseBuilder = ctx.getVeniceServerResponseBuilder();
    if (ctx.hasError()) {
      statsContext.setResponseStatus(HttpResponseStatus.BAD_REQUEST);
      veniceServerResponseBuilder.setData(ByteString.EMPTY).setCompressionStrategy(compressionStrategy.getValue());
      invokeNextHandler(ctx);
      return;
    }

    compressionStrategy = obj.getCompressionStrategy();

    veniceServerResponseBuilder.setCompressionStrategy(compressionStrategy.getValue());
    veniceServerResponseBuilder.setResponseRCU(obj.getRCU());
    veniceServerResponseBuilder.setIsStreamingResponse(obj.isStreamingResponse());

    if (obj.isFound()) {
      body = obj.getResponseBody();

      byte[] array = new byte[body.readableBytes()];
      body.getBytes(body.readerIndex(), array);
      veniceServerResponseBuilder.setData(ByteString.copyFrom(array))
          .setCompressionStrategy(compressionStrategy.getValue());

      veniceServerResponseBuilder.setSchemaId(obj.getResponseSchemaIdHeader());
      statsContext.setResponseStatus(OK);
      invokeNextHandler(ctx);
      return;
    }

    ctx.setError();
    statsContext.setResponseStatus(NOT_FOUND);
    veniceServerResponseBuilder.setData(ByteString.EMPTY);
    veniceServerResponseBuilder.setErrorCode(VeniceReadResponseStatus.KEY_NOT_FOUND);
    veniceServerResponseBuilder.setErrorMessage("Key not found");
    invokeNextHandler(ctx);
  }
}
