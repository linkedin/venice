package com.linkedin.venice.fastclient.transport;

import com.google.protobuf.ByteString;
import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import com.linkedin.venice.client.exceptions.VeniceClientRateExceededException;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.fastclient.ClientConfig;
import com.linkedin.venice.grpc.GrpcErrorCodes;
import com.linkedin.venice.protocols.VeniceClientRequest;
import com.linkedin.venice.protocols.VeniceReadServiceGrpc;
import com.linkedin.venice.protocols.VeniceServerResponse;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class GrpcTransportClient extends InternalTransportClient {
  private static final Logger LOGGER = LogManager.getLogger(GrpcTransportClient.class);
  private static final String STORAGE_ACTION = "storage";
  private final VeniceConcurrentHashMap<String, ManagedChannel> serverGrpcChannels;
  private final Map<String, String> nettyAddressToGrpcAddressMap;
  // we cache stubs to avoid creating a new stub for each request, improves performance
  private final VeniceConcurrentHashMap<ManagedChannel, VeniceReadServiceGrpc.VeniceReadServiceStub> stubCache;
  private final TransportClient r2TransportClient; // used for non-storage related requests

  public GrpcTransportClient(ClientConfig clientConfig) {
    serverGrpcChannels = new VeniceConcurrentHashMap<>();
    stubCache = new VeniceConcurrentHashMap<>();
    nettyAddressToGrpcAddressMap = clientConfig.getNettyServerToGrpcAddressMap();
    this.r2TransportClient = new R2TransportClient(clientConfig.getR2Client());
  }

  protected ManagedChannel getChannel(String serverAddress) {
    if (!nettyAddressToGrpcAddressMap.containsKey(serverAddress)) {
      throw new VeniceException("No grpc server found for address: " + serverAddress);
    }

    return serverGrpcChannels.computeIfAbsent(
        serverAddress,
        k -> ManagedChannelBuilder.forTarget(nettyAddressToGrpcAddressMap.get(k)).usePlaintext().build());
  }

  protected VeniceReadServiceGrpc.VeniceReadServiceStub getStub(ManagedChannel channel) {
    return stubCache.computeIfAbsent(channel, VeniceReadServiceGrpc::newStub);
  }

  @Override
  public CompletableFuture<TransportClientResponse> get(String requestPath, Map<String, String> headers) {
    return handleRequest(requestPath, headers, null, true);
  }

  @Override
  public CompletableFuture<TransportClientResponse> post(
      String requestPath,
      Map<String, String> headers,
      byte[] requestBody) {
    return handleRequest(requestPath, headers, requestBody, false);
  }

  @Override
  public void close() throws IOException {
    for (Map.Entry<String, ManagedChannel> entry: serverGrpcChannels.entrySet()) {
      entry.getValue().shutdown();
    }

    r2TransportClient.close();
  }

  public CompletableFuture<TransportClientResponse> handleRequest(
      String requestPath,
      Map<String, String> headers,
      byte[] requestBody,
      boolean isSingleGet) {
    String[] requestParts = requestPath.split("/");
    // https://localhost:1234/storage/store_v1/0/keyString
    // ["https:", "", "localhost:1234", "storage", "store_v1", "0", "keyString"]

    if (!requestParts[3].equals(STORAGE_ACTION)) {
      LOGGER.debug(
          "performing unsupported gRPC transport client action ({}), passing request to R2 client",
          requestParts[3]);
      return isSingleGet
          ? r2TransportClient.get(requestPath, headers)
          : r2TransportClient.post(requestPath, headers, requestBody);
    }
    ManagedChannel channel = getChannel(requestParts[2]);
    VeniceClientRequest.Builder requestBuilder =
        VeniceClientRequest.newBuilder().setResourceName(requestParts[4]).setIsBatchRequest(!isSingleGet);

    if (isSingleGet) {
      requestBuilder.setKeyString(requestParts[6]);
      requestBuilder.setPartition(Integer.parseInt(requestParts[5]));
    } else {
      requestBuilder.setKeyBytes(ByteString.copyFrom(requestBody));
    }

    VeniceReadServiceGrpc.VeniceReadServiceStub clientStub = getStub(channel);
    GrpcTransportClientCallback callback = new GrpcTransportClientCallback(clientStub, requestBuilder.build());

    return isSingleGet ? callback.get() : callback.post();
  }

  private static class GrpcTransportClientCallback {
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
      if (request.getIsBatchRequest()) {
        throw new UnsupportedOperationException("Not a single get request, use batchGet() instead");
      }
      clientStub.get(request, new StreamObserver<VeniceServerResponse>() {
        @Override
        public void onNext(VeniceServerResponse value) {
          if (value.getErrorCode() != GrpcErrorCodes.OK)
            handleError(value);
          valueFuture.complete(
              new TransportClientResponse(
                  value.getSchemaId(),
                  CompressionStrategy.valueOf(value.getCompressionStrategy()),
                  value.getData().toByteArray()));
        }

        @Override
        public void onError(Throwable t) {
          valueFuture.completeExceptionally(t);
        }

        @Override
        public void onCompleted() {
          LOGGER.debug("Completed gRPC request");
        }
      });

      return valueFuture;
    }

    public CompletableFuture<TransportClientResponse> post() {
      if (!request.getIsBatchRequest()) {
        throw new UnsupportedOperationException("Not a batch get request, use get() instead");
      }
      clientStub.batchGet(request, new StreamObserver<VeniceServerResponse>() {
        @Override
        public void onNext(VeniceServerResponse value) {
          LOGGER.debug("Performing BatchGet in gRPC");
          if (value.getErrorCode() != GrpcErrorCodes.OK)
            handleError(value);
          valueFuture.complete(
              new TransportClientResponse(
                  value.getSchemaId(),
                  CompressionStrategy.valueOf(value.getCompressionStrategy()),
                  value.getData().toByteArray()));
        }

        @Override
        public void onError(Throwable t) {
          valueFuture.completeExceptionally(t);
        }

        @Override
        public void onCompleted() {
          LOGGER.debug("Completed batch get gRPC request");
        }
      });

      return valueFuture;
    }

    private void handleError(VeniceServerResponse response) {
      // Create new field in .proto for error codes :slight_smile:
      int statusCode = response.getErrorCode();
      String errorMessage = response.getErrorMessage();
      Exception exception;

      switch (statusCode) {
        case GrpcErrorCodes.BAD_REQUEST:
          exception = new VeniceClientHttpException(errorMessage, statusCode);
          break;
        case GrpcErrorCodes.TOO_MANY_REQUESTS:
          exception = new VeniceClientRateExceededException(errorMessage);
          break;
        case GrpcErrorCodes.KEY_NOT_FOUND:
          valueFuture.complete(null);
          return;
        default:
          exception = new VeniceException("grpc error occurred");
          break;
      }

      valueFuture.completeExceptionally(exception);
    }
  }
}
