package com.linkedin.venice.listener.grpc;

import com.google.protobuf.ByteString;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.venice.listener.StorageReadRequestsHandler;
import com.linkedin.venice.listener.request.GetRouterRequest;
import com.linkedin.venice.listener.response.StorageResponseObject;
import com.linkedin.venice.protocols.VeniceClientRequest;
import com.linkedin.venice.protocols.VeniceReadServiceGrpc;
import com.linkedin.venice.protocols.VeniceServerResponse;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceReadServiceImpl extends VeniceReadServiceGrpc.VeniceReadServiceImplBase {
  private static final Logger LOGGER = LogManager.getLogger(VeniceReadServiceImpl.class);
  StorageReadRequestsHandler storageReadRequestsHandler;

  public VeniceReadServiceImpl() {

  }

  public VeniceReadServiceImpl(StorageReadRequestsHandler storageReadRequestsHandler) {
    this.storageReadRequestsHandler = storageReadRequestsHandler;
  }

  @Override
  public void get(VeniceClientRequest request, StreamObserver<VeniceServerResponse> responseObserver) {
    GetRouterRequest getRouterRequest = GetRouterRequest.grpcGetRouterRequest(request);

    StorageResponseObject response =
        (StorageResponseObject) storageReadRequestsHandler.handleSingleGetGrpcRequest(getRouterRequest);

    ValueRecord valueRecord = response.getValueRecord();

    VeniceServerResponse grpcResponse = VeniceServerResponse.newBuilder()
        .setSchemaId(valueRecord.getSchemaId())
        .setDataSize(valueRecord.getDataSize())
        .setData(ByteString.copyFrom(valueRecord.getData().array(), 4, valueRecord.getDataSize())) // set offset of 4
                                                                                                   // since first 4
                                                                                                   // bytes are schema
                                                                                                   // id
        .setSerializedArr(ByteString.copyFrom(valueRecord.getData().array()))
        .build();

    responseObserver.onNext(grpcResponse);
    responseObserver.onCompleted();
  }
}
