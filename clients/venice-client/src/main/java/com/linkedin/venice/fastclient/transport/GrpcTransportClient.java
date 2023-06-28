package com.linkedin.venice.fastclient.transport;

import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.client.store.transport.TransportClientStreamingCallback;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.protocols.VeniceClientRequest;
import com.linkedin.venice.protocols.VeniceReadServiceGrpc;
import com.linkedin.venice.protocols.VeniceServerResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class GrpcTransportClient extends TransportClient {
  private static final Logger LOGGER = LogManager.getLogger(GrpcTransportClient.class);
  private ConcurrentMap<String, VeniceReadServiceGrpc.VeniceReadServiceStub> clientStubCache;
  private final Map<String, ManagedChannel> nettyServerToGrpcServerChannelMap;
  private final Map<String, String> nettyServerToGrpcDebug;

  public GrpcTransportClient(Map<String, String> nettyServerToGrpc) { // set default transport client to go around
                                                                      // metadata requests, can pass thru requests when
                                                                      // performing the get request
    nettyServerToGrpcServerChannelMap = new HashMap<>();
    nettyServerToGrpcDebug = new HashMap<>();

    for (Map.Entry<String, String> entry: nettyServerToGrpc.entrySet()) {
      String nettyServer = entry.getKey().split(":")[0]; // nettyServer is of the form "host:port"
      String grpcServer = nettyServer + ":" + entry.getValue();

      ManagedChannel channel = ManagedChannelBuilder.forTarget(grpcServer).usePlaintext().build();
      nettyServerToGrpcServerChannelMap.put(entry.getKey(), channel);
      nettyServerToGrpcDebug.put(entry.getKey(), grpcServer);
    }
  }

  @Override
  public CompletableFuture<TransportClientResponse> get(String requestPath, Map<String, String> headers) {
    String[] requestParts = requestPath.split("/");
    // 0: nettyAddr, 1: requestType, 2: resourceName, 3: partition, 4: key
    String nettyAddr = requestParts[2]; // nettyAddr is of the form "host:port//requestType"
    ManagedChannel requestChannel = nettyServerToGrpcServerChannelMap.get(nettyAddr);

    VeniceClientRequest request = VeniceClientRequest.newBuilder()
        .setStoreName(requestParts[4])
        .setResourceName(requestParts[4])
        .setPartition(Integer.parseInt(requestParts[5]))
        .setKeyString(requestParts[6])
        .build();

    VeniceReadServiceGrpc.VeniceReadServiceStub clientStub = VeniceReadServiceGrpc.newStub(requestChannel);

    GrpcTransportClientCallback callback = new GrpcTransportClientCallback(clientStub, request);
    CompletableFuture<TransportClientResponse> valueFuture = callback.get();

    return valueFuture;

    // GrpcTransportClientCallback response = new GrpcTransportClientCallback(new
    // CompletableFuture<TransportClientResponse>());
    // clientStub.get(request, response);
    //
    // return response.getValueFuture();
  }

  @Override
  public CompletableFuture<TransportClientResponse> post(
      String requestPath,
      Map<String, String> headers,
      byte[] requestBody) {
    return null;
  }

  @Override
  public void streamPost(
      String requestPath,
      Map<String, String> headers,
      byte[] requestBody,
      TransportClientStreamingCallback callback,
      int keyCount) {

  }

  @Override
  public void close() throws IOException {
    for (Map.Entry<String, ManagedChannel> entry: nettyServerToGrpcServerChannelMap.entrySet()) {
      entry.getValue().shutdown();
    }
  }

  public class GrpcTransportClientCallback {
    // start exception handling
    private final CompletableFuture<TransportClientResponse> valueFuture;
    private final VeniceReadServiceGrpc.VeniceReadServiceStub clientStub;
    private final VeniceClientRequest request;

    public GrpcTransportClientCallback(
        VeniceReadServiceGrpc.VeniceReadServiceStub clientStub,
        VeniceClientRequest request) {
      this.clientStub = clientStub;
      this.request = request;
      this.valueFuture = new CompletableFuture<>();
    }

    public CompletableFuture<TransportClientResponse> get() {
      clientStub.get(request, new StreamObserver<VeniceServerResponse>() {
        @Override
        public void onNext(VeniceServerResponse value) {
          valueFuture.complete(
              new TransportClientResponse(
                  value.getSchemaId(),
                  CompressionStrategy.NO_OP,
                  value.getData().toByteArray()));
        }

        @Override
        public void onError(Throwable t) {

        }

        @Override
        public void onCompleted() {
          LOGGER.info("Completed gRPC request");

        }
      });

      return valueFuture;
    }
  }

  // public class GrpcTransportClientCallback implements StreamObserver<VeniceServerResponse> {
  // // start exception handling
  // private final CompletableFuture<TransportClientResponse> valueFuture;
  // private TransportClientResponse response;
  // public GrpcTransportClientCallback(CompletableFuture<TransportClientResponse> valueFuture) {
  // this.valueFuture = valueFuture;
  //
  // }
  //
  // @Override
  // public void onNext(VeniceServerResponse value) {
  // LOGGER.info("Received response from grpc server: " + value);
  // int schemaId = value.getSchemaId();
  // byte [] valueBytes = value.getData().toByteArray();
  //
  // response = new TransportClientResponse(schemaId, CompressionStrategy.NO_OP, valueBytes);
  // //valuefuture.completeExceptionally(venicereadexception)
  // }
  //
  // @Override
  // public void onError(Throwable t) {
  //
  // }
  //
  // @Override
  // public void onCompleted() {
  // LOGGER.info("Completed response from grpc server");
  // valueFuture.complete(response);
  // }
  //
  // public CompletableFuture<TransportClientResponse> getValueFuture() {
  // return valueFuture;
  // }
  // }
}
