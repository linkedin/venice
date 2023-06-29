package com.linkedin.venice.fastclient.transport;

import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.client.store.transport.TransportClientStreamingCallback;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.protocols.VeniceClientRequest;
import com.linkedin.venice.protocols.VeniceReadServiceGrpc;
import com.linkedin.venice.protocols.VeniceServerResponse;
import com.linkedin.venice.request.RequestHelper;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class GrpcTransportClient extends TransportClient {
  private static final Logger LOGGER = LogManager.getLogger(GrpcTransportClient.class);
  private final TransportClient r2TransportClient; // used for metadata requests
  private final Map<String, ManagedChannel> nettyServerToGrpcServerChannelMap;

  public GrpcTransportClient(Map<String, String> nettyServerToGrpc, TransportClient r2TransportClient) {
    nettyServerToGrpcServerChannelMap = new HashMap<>();
    this.r2TransportClient = r2TransportClient;

    for (Map.Entry<String, String> entry: nettyServerToGrpc.entrySet()) {
      String nettyServer = entry.getKey().split(":")[0];
      String grpcServer = nettyServer + ":" + entry.getValue();

      ManagedChannel channel = ManagedChannelBuilder.forTarget(grpcServer).usePlaintext().build();
      nettyServerToGrpcServerChannelMap.put(entry.getKey(), channel);
    }
  }

  @Override
  public CompletableFuture<TransportClientResponse> get(String requestPath, Map<String, String> headers) {
    // TODO: refactor request parsing to use existing request parsing code
    String[] requestParts = RequestHelper.getRequestParts(requestPath);
    String requestPartForNetty = requestPath.split("/")[2]; // hacky way to get port for mapping to grpc server
    if (!requestParts[1].equals("storage")) {
      return r2TransportClient.get(requestPath, headers);
    }

    String nettyAddr = requestPartForNetty;
    ManagedChannel requestChannel = nettyServerToGrpcServerChannelMap.get(nettyAddr);

    VeniceClientRequest request = VeniceClientRequest.newBuilder()
        .setStoreName(requestParts[2])
        .setResourceName(requestParts[2])
        .setPartition(Integer.parseInt(requestParts[3]))
        .setKeyString(requestParts[4])
        .build();

    VeniceReadServiceGrpc.VeniceReadServiceStub clientStub = VeniceReadServiceGrpc.newStub(requestChannel);
    GrpcTransportClientCallback callback = new GrpcTransportClientCallback(clientStub, request);

    return callback.get();
  }

  @Override
  public CompletableFuture<TransportClientResponse> post(
      String requestPath,
      Map<String, String> headers,
      byte[] requestBody) {
    // TODO:
    return null;
  }

  @Override
  public void streamPost(
      String requestPath,
      Map<String, String> headers,
      byte[] requestBody,
      TransportClientStreamingCallback callback,
      int keyCount) {
    // TODO:
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
          LOGGER.error("Error in gRPC request", t);
          valueFuture.completeExceptionally(t);
        }

        @Override
        public void onCompleted() {
          LOGGER.info("Completed gRPC request");
        }
      });

      return valueFuture;
    }
  }
}
