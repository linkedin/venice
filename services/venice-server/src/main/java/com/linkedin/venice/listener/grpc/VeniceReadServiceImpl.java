package com.linkedin.venice.listener.grpc;

import com.google.protobuf.ByteString;
import com.linkedin.davinci.listener.response.ReadResponse;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.listener.StorageReadRequestsHandler;
import com.linkedin.venice.listener.request.GetRouterRequest;
import com.linkedin.venice.listener.request.MultiGetRouterRequestWrapper;
import com.linkedin.venice.listener.response.StorageResponseObject;
import com.linkedin.venice.protocols.VeniceClientRequest;
import com.linkedin.venice.protocols.VeniceReadServiceGrpc;
import com.linkedin.venice.protocols.VeniceServerResponse;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceReadServiceImpl extends VeniceReadServiceGrpc.VeniceReadServiceImplBase {
  private static final Logger LOGGER = LogManager.getLogger(VeniceReadServiceImpl.class);
  StorageReadRequestsHandler storageReadRequestsHandler;

  public VeniceReadServiceImpl() {
    LOGGER.info("Created gRPC Server for VeniceReadService");
  }

  public VeniceReadServiceImpl(StorageReadRequestsHandler storageReadRequestsHandler) {
    LOGGER.info("Created gRPC Server for VeniceReadService");
    this.storageReadRequestsHandler = storageReadRequestsHandler;
  }

  @Override
  public void get(VeniceClientRequest request, StreamObserver<VeniceServerResponse> responseObserver) {
    VeniceServerResponse grpcResponse = handleSingleGetRequest(request);

    responseObserver.onNext(grpcResponse);
    responseObserver.onCompleted();
  }

  @Override
  public void batchGet(VeniceClientRequest request, StreamObserver<VeniceServerResponse> responseObserver) {
    VeniceServerResponse grpcBatchGetResponse = handleMultiGetRequest(request);

    responseObserver.onNext(grpcBatchGetResponse);
    responseObserver.onCompleted();
  }

  private VeniceServerResponse handleSingleGetRequest(VeniceClientRequest request) {
    GetRouterRequest getRouterRequest = GetRouterRequest.grpcGetRouterRequest(request);

    StorageResponseObject response =
        (StorageResponseObject) storageReadRequestsHandler.handleSingleGetGrpcRequest(getRouterRequest);

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

    ReadResponse readResponse = storageReadRequestsHandler.handleMultiGetGrpcRequest(multiGetRouterRequestWrapper);
    int schemaId = readResponse.getResponseSchemaIdHeader();
    ByteBuf data = readResponse.getResponseBody();
    CompressionStrategy compressionStrategy = readResponse.getCompressionStrategy();

    return VeniceServerResponse.newBuilder()
        .setData(ByteString.copyFrom(data.array()))
        .setSchemaId(schemaId)
        .setCompressionStrategy(compressionStrategy.getValue())
        .build();
  }
}
