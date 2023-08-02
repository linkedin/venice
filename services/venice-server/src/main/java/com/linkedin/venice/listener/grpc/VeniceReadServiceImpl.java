package com.linkedin.venice.listener.grpc;

import com.google.protobuf.ByteString;
import com.linkedin.davinci.listener.response.ReadResponse;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.listener.HttpChannelInitializer;
import com.linkedin.venice.listener.StorageReadRequestHandler;
import com.linkedin.venice.listener.request.GetRouterRequest;
import com.linkedin.venice.listener.request.MultiGetRouterRequestWrapper;
import com.linkedin.venice.listener.response.StorageResponseObject;
import com.linkedin.venice.protocols.VeniceClientRequest;
import com.linkedin.venice.protocols.VeniceReadServiceGrpc;
import com.linkedin.venice.protocols.VeniceServerResponse;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceReadServiceImpl extends VeniceReadServiceGrpc.VeniceReadServiceImplBase {
  private static final Logger LOGGER = LogManager.getLogger(VeniceReadServiceImpl.class);

  private GrpcHandlerPipeline handlerPipeline;

  private StorageReadRequestHandler storageReadRequestHandler;

  public VeniceReadServiceImpl() {
    LOGGER.info("Created gRPC Server for VeniceReadService");
  }

  public VeniceReadServiceImpl(StorageReadRequestHandler storageReadRequestHandler) {
    LOGGER.info("Created gRPC Server for VeniceReadService");
    this.storageReadRequestHandler = storageReadRequestHandler;
  }

  public VeniceReadServiceImpl(List<VeniceGrpcHandler> inboundHandlers, List<VeniceGrpcHandler> outboundHandlers) {
    LOGGER.info("Created gRPC Server for VeniceReadService");
    // this.inboundHandlers = inboundHandlers;
    // this.outboundHandlers = outboundHandlers;
  }

  public VeniceReadServiceImpl(HttpChannelInitializer channelInitializer) {
    // this.inboundHandlers = channelInitializer.getInboundHandlers();
    // this.outboundHandlers = channelInitializer.getOutboundHandlers();
    this.handlerPipeline = channelInitializer.initGrpcHandlers();
  }

  @Override
  public void get(VeniceClientRequest request, StreamObserver<VeniceServerResponse> responseObserver) {
    handleRequest(request, responseObserver);
  }

  @Override
  public void batchGet(VeniceClientRequest request, StreamObserver<VeniceServerResponse> responseObserver) {
    handleRequest(request, responseObserver);
  }

  private void handleRequest(VeniceClientRequest request, StreamObserver<VeniceServerResponse> responseObserver) {
    VeniceServerResponse.Builder responseBuilder = VeniceServerResponse.newBuilder();
    GrpcHandlerContext ctx = new GrpcHandlerContext(request, responseBuilder, responseObserver);
    // Iterator<VeniceGrpcHandler> inboundHandlerIterator = inboundHandlers.iterator();

    handlerPipeline.processRequest(ctx);
    handlerPipeline.processResponse(ctx);

    responseObserver.onNext(ctx.getVeniceServerResponseBuilder().build());
    responseObserver.onCompleted();
  }

  private VeniceServerResponse handleSingleGetRequest(VeniceClientRequest request) {
    GetRouterRequest getRouterRequest = GetRouterRequest.grpcGetRouterRequest(request);

    StorageResponseObject response =
        (StorageResponseObject) storageReadRequestHandler.handleSingleGetGrpcRequest(getRouterRequest);

    ValueRecord valueRecord = response.getValueRecord();
    CompressionStrategy compressionStrategy = response.getCompressionStrategy();
    return VeniceServerResponse.newBuilder()
        .setSchemaId(valueRecord.getSchemaId())
        .setData(ByteString.copyFrom(valueRecord.getData().array(), 4, valueRecord.getDataSize()))
        .setCompressionStrategy(compressionStrategy.getValue())
        .build();

  }

  private VeniceServerResponse handleMultiGetRequest(VeniceClientRequest request) {
    MultiGetRouterRequestWrapper multiGetRouterRequestWrapper =
        MultiGetRouterRequestWrapper.parseMultiGetGrpcRequest(request);

    ReadResponse readResponse = storageReadRequestHandler.handleMultiGetGrpcRequest(multiGetRouterRequestWrapper);
    int schemaId = readResponse.getResponseSchemaIdHeader();
    ByteBuf data = readResponse.getResponseBody();
    CompressionStrategy compressionStrategy = readResponse.getCompressionStrategy();

    return VeniceServerResponse.newBuilder()
        .setData(ByteString.copyFrom(data.array()))
        .setSchemaId(schemaId)
        .setCompressionStrategy(compressionStrategy.getValue())
        .build();
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }
}
