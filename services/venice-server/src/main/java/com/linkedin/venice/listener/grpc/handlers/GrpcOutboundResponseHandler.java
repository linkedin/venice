package com.linkedin.venice.listener.grpc.handlers;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import com.google.protobuf.ByteString;
import com.linkedin.davinci.listener.response.ReadResponse;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.listener.ServerStatsContext;
import com.linkedin.venice.listener.grpc.GrpcRequestContext;
import com.linkedin.venice.protocols.VeniceServerResponse;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;


public class GrpcOutboundResponseHandler extends VeniceServerGrpcHandler {
  @Override
  public void processRequest(GrpcRequestContext ctx) {
    ByteBuf body;
    CompressionStrategy compressionStrategy = CompressionStrategy.NO_OP;

    ReadResponse obj = ctx.getReadResponse();
    ServerStatsContext statsContext = ctx.getGrpcStatsContext();
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
