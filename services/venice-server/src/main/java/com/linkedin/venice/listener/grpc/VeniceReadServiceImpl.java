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


public class VeniceReadServiceImpl extends VeniceReadServiceGrpc.VeniceReadServiceImplBase {
  StorageReadRequestsHandler _storageReadRequestsHandler;

  public VeniceReadServiceImpl() {
  }

  public VeniceReadServiceImpl(StorageReadRequestsHandler storageReadRequestsHandler) {
    _storageReadRequestsHandler = storageReadRequestsHandler;
  }

  @Override
  public void get(VeniceClientRequest request, StreamObserver<VeniceServerResponse> responseObserver) {
    GetRouterRequest getRouterRequest = GetRouterRequest.grpcGetRouterRequest(request);

    StorageResponseObject response =
        (StorageResponseObject) _storageReadRequestsHandler.handleSingleGetGrpcRequest(getRouterRequest);

    ValueRecord valueRecord = response.getValueRecord();

    VeniceServerResponse grpcResponse = VeniceServerResponse.newBuilder()
        .setSchemaId(valueRecord.getSchemaId())
        .setDataSize(valueRecord.getDataSize())
        .setData(ByteString.copyFrom(valueRecord.getData().array(), 4, valueRecord.getDataSize()))
        .setSerializedArr(ByteString.copyFrom(valueRecord.getData().array()))
        .build();

    responseObserver.onNext(grpcResponse);
    responseObserver.onCompleted();
  }
}
