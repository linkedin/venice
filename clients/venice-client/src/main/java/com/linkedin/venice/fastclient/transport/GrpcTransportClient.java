package com.linkedin.venice.fastclient.transport;

import com.google.protobuf.ByteString;
import com.linkedin.venice.HttpMethod;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import com.linkedin.venice.client.exceptions.VeniceClientRateExceededException;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.fastclient.GrpcClientConfig;
import com.linkedin.venice.grpc.GrpcErrorCodes;
import com.linkedin.venice.grpc.GrpcUtils;
import com.linkedin.venice.protocols.VeniceClientRequest;
import com.linkedin.venice.protocols.VeniceReadServiceGrpc;
import com.linkedin.venice.protocols.VeniceServerResponse;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.TlsChannelCredentials;
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
  // TODO: remove this transport client once non-storage related operations are supported over gRPC
  private final TransportClient r2TransportClientForNonStorageOps; // used for non-storage related requests
  private final GrpcClientConfig grpcClientConfig;
  private final SSLFactory sslFactory;
  private ChannelCredentials channelCredentials = InsecureChannelCredentials.create();

  public GrpcTransportClient(GrpcClientConfig grpcClientConfig) {
    this.grpcClientConfig = grpcClientConfig;

    serverGrpcChannels = new VeniceConcurrentHashMap<>();
    stubCache = new VeniceConcurrentHashMap<>();

    nettyAddressToGrpcAddressMap = grpcClientConfig.getNettyServerToGrpcAddressMap();
    r2TransportClientForNonStorageOps = new R2TransportClient(grpcClientConfig.getR2Client());

    sslFactory = grpcClientConfig.getSslFactory();

    if (sslFactory != null) {
      initChannelCredentials();
    }
  }

  private void initChannelCredentials() {
    try {
      TlsChannelCredentials.Builder tlsBuilder = TlsChannelCredentials.newBuilder()
          .keyManager(GrpcUtils.getKeyManagers(sslFactory))
          .trustManager(GrpcUtils.getTrustManagers(sslFactory));
      channelCredentials = tlsBuilder.build();
    } catch (Exception e) {
      throw new VeniceClientException(
          "Failed to initialize SSL channel credentials for Venice gRPC Transport Client",
          e);
    }
  }

  protected ManagedChannel getChannel(String serverAddress) {
    if (!nettyAddressToGrpcAddressMap.containsKey(serverAddress)) {
      throw new VeniceException("No grpc server found for address: " + serverAddress);
    }

    return serverGrpcChannels.computeIfAbsent(
        serverAddress,
        k -> Grpc.newChannelBuilder(nettyAddressToGrpcAddressMap.get(k), channelCredentials).build());
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

    r2TransportClientForNonStorageOps.close();
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
          ? r2TransportClientForNonStorageOps.get(requestPath, headers)
          : r2TransportClientForNonStorageOps.post(requestPath, headers, requestBody);
    }
    ManagedChannel channel = getChannel(requestParts[2]);
    VeniceClientRequest.Builder requestBuilder = VeniceClientRequest.newBuilder()
        .setResourceName(requestParts[4])
        .setIsBatchRequest(!isSingleGet)
        .setMethod(isSingleGet ? HttpMethod.GET.name() : HttpMethod.POST.name());

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
          if (value.getErrorCode() != GrpcErrorCodes.OK) {
            handleResponseError(value);
            return;
          }
          valueFuture.complete(
              new TransportClientResponse(
                  value.getSchemaId(),
                  CompressionStrategy.valueOf(value.getCompressionStrategy()),
                  value.getData().toByteArray()));
        }

        @Override
        public void onError(Throwable t) {
          handleGrpcError(t);
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
          if (value.getErrorCode() != GrpcErrorCodes.OK) {
            handleResponseError(value);
            return;
          }
          valueFuture.complete(
              new TransportClientResponse(
                  value.getSchemaId(),
                  CompressionStrategy.valueOf(value.getCompressionStrategy()),
                  value.getData().toByteArray()));
        }

        @Override
        public void onError(Throwable t) {
          handleGrpcError(t);
        }

        @Override
        public void onCompleted() {
          LOGGER.debug("Completed batch get gRPC request");
        }
      });

      return valueFuture;
    }

    // used for errors that are raised within the gRPC handler pipeline
    private void handleResponseError(VeniceServerResponse response) {
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
          exception = new VeniceClientException(
              String
                  .format("An unexpected error occurred with status code: %d, message: %s", statusCode, errorMessage));
          break;
      }

      valueFuture.completeExceptionally(exception);
    }

    // used for errors raised during the gRPC call itself
    private void handleGrpcError(Throwable t) {
      LOGGER.debug("gRPC error occurred", t);

      if (t instanceof StatusRuntimeException) {
        StatusRuntimeException statusRuntimeException = (StatusRuntimeException) t;
        Status status = statusRuntimeException.getStatus();
        Status.Code statusCode = status.getCode();

        String errorMessage =
            status.getDescription() != null ? status.getDescription() : statusRuntimeException.getMessage();
        int statusCodeValue = statusCode.value();
        LOGGER.error("gRPC error occurred with status code: {}, message: {}", statusCodeValue, errorMessage);
        Exception exception;
        switch (statusCode) {
          case PERMISSION_DENIED:
          case UNAUTHENTICATED:
          case INVALID_ARGUMENT:
            // these errors are purposefully raised by the server, and we provide a more specific message when they
            // occur
            exception = new VeniceClientHttpException(errorMessage, statusCodeValue);
            break;
          default:
            exception = new VeniceClientException(
                String.format(
                    "An unexpected gRPC error occurred with status code: %d, message: %s",
                    statusCodeValue,
                    errorMessage));
            break;
        }
        valueFuture.completeExceptionally(exception);
      } else {
        valueFuture.completeExceptionally(new VeniceException("A gRPC error occurred when completing this request", t));
      }
    }
  }
}
