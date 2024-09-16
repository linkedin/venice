package com.linkedin.venice.fastclient.transport;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.linkedin.venice.HttpMethod;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import com.linkedin.venice.client.exceptions.VeniceClientRateExceededException;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.fastclient.GrpcClientConfig;
import com.linkedin.venice.grpc.GrpcUtils;
import com.linkedin.venice.protocols.VeniceClientRequest;
import com.linkedin.venice.protocols.VeniceReadServiceGrpc;
import com.linkedin.venice.protocols.VeniceServerResponse;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.TlsChannelCredentials;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class GrpcTransportClient extends InternalTransportClient {
  private static final Logger LOGGER = LogManager.getLogger(GrpcTransportClient.class);
  private static final String STORAGE_ACTION = "storage";
  private static final String GRPC_ADDRESS_FORMAT = "%s:%s";
  private final VeniceConcurrentHashMap<String, ManagedChannel> serverGrpcChannels;
  private final Map<String, String> nettyServerToGrpcAddress;
  // we cache stubs to avoid creating a new stub for each request, improves performance
  private final VeniceConcurrentHashMap<ManagedChannel, VeniceReadServiceGrpc.VeniceReadServiceStub> stubCache;
  // TODO: remove this transport client once non-storage related operations are supported over gRPC
  private final TransportClient r2TransportClientForNonStorageOps; // used for non-storage related requests
  private final ChannelCredentials channelCredentials;

  private final int port;

  public GrpcTransportClient(GrpcClientConfig grpcClientConfig) {
    this(
        new R2TransportClient(grpcClientConfig.getR2Client()),
        /*
         * General principle to not mutate input parameters and wrapping it up using a new map since this
         * class updates the map with inferred grpc address to prevent redundant computation for mapping netty
         * to grpc address
         */
        new HashMap<>(grpcClientConfig.getNettyServerToGrpcAddress()),
        grpcClientConfig.getPort(),
        grpcClientConfig.getSslFactory());
  }

  @VisibleForTesting
  GrpcTransportClient(
      TransportClient transportClient,
      Map<String, String> nettyServerToGrpcAddress,
      int port,
      SSLFactory sslFactory) {
    this.r2TransportClientForNonStorageOps = transportClient;
    this.nettyServerToGrpcAddress = nettyServerToGrpcAddress;
    this.port = port;
    this.serverGrpcChannels = new VeniceConcurrentHashMap<>();
    this.stubCache = new VeniceConcurrentHashMap<>();
    this.channelCredentials = buildChannelCredentials(sslFactory);
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

  @VisibleForTesting
  ChannelCredentials buildChannelCredentials(SSLFactory sslFactory) {
    // TODO: Evaluate if this needs to fail instead since it depends on plain text support on server
    if (sslFactory == null) {
      return InsecureChannelCredentials.create();
    }

    try {
      TlsChannelCredentials.Builder tlsBuilder = TlsChannelCredentials.newBuilder()
          .keyManager(GrpcUtils.getKeyManagers(sslFactory))
          .trustManager(GrpcUtils.getTrustManagers(sslFactory));
      return tlsBuilder.build();
    } catch (Exception e) {
      throw new VeniceClientException(
          "Failed to initialize SSL channel credentials for Venice gRPC Transport Client",
          e);
    }
  }

  @VisibleForTesting
  VeniceClientRequest buildVeniceClientRequest(String[] requestParts, byte[] requestBody, boolean isSingleGet) {
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

    return requestBuilder.build();
  }

  @VisibleForTesting
  VeniceReadServiceGrpc.VeniceReadServiceStub getOrCreateStub(String serverAddress) {
    String grpcAddress = getGrpcAddressFromServerAddress(serverAddress);

    ManagedChannel channel = serverGrpcChannels
        .computeIfAbsent(serverAddress, k -> Grpc.newChannelBuilder(grpcAddress, channelCredentials).build());

    return stubCache.computeIfAbsent(channel, VeniceReadServiceGrpc::newStub);
  }

  /**
   * Maps the given server address to GRPC server address. Typically, the mapped server address only differs in the
   * port. Fetch the GRPC server address for a given server address if available in the #nettyServerToGrpcAddress
   * map if not, strip the port from the original server address and replace it with the grpc port.
   * <b>Note:</b> We have the GRPC port hard coded and operate with static port assignment for GRPC
   */
  @VisibleForTesting
  String getGrpcAddressFromServerAddress(String serverAddress) {
    Preconditions.checkState(StringUtils.isNotEmpty(serverAddress), "Server address cannot be empty ");

    nettyServerToGrpcAddress.computeIfAbsent(serverAddress, nettyAddress -> {
      String[] serverAddressParts = serverAddress.split(":");
      Preconditions.checkState(serverAddressParts.length == 2, "Invalid server address");

      return String.format(GRPC_ADDRESS_FORMAT, serverAddressParts[0], port);
    });

    return nettyServerToGrpcAddress.get(serverAddress);
  }

  @VisibleForTesting
  Map<String, String> getNettyServerToGrpcAddress() {
    return ImmutableMap.copyOf(nettyServerToGrpcAddress);
  }

  /**
   * Handles get and batch get requests using GRPC.
   *   1. Identify the type of query action and delegates the non-storage queries to R2 over http client and
   *      uses GRPC for storage queries
   *   2. Fetch the channel for the server based on the request uri
   *   2. Construct the request based on the query action (supported actions get, post)
   *   3. Use the client stub associated with the channel to send request
   *   The request path is of following format
   *   [protocol]://[URI]/[QUERY_ACTION]/[resource_name]_[version]/[partition]/[key_string]
   */
  @VisibleForTesting
  CompletableFuture<TransportClientResponse> handleRequest(
      String requestPath,
      Map<String, String> headers,
      byte[] requestBody,
      boolean isSingleGet) {
    String[] requestParts = requestPath.split("/");

    if (!isValidRequest(requestParts, isSingleGet)) {
      LOGGER.error("Failed to process request: {}", Arrays.toString(requestParts));
      // avoiding CompletableFuture.failedFuture to keep it JDK agnostic
      CompletableFuture<TransportClientResponse> failedFuture = new CompletableFuture<>();
      failedFuture.completeExceptionally(new VeniceClientException("Invalid request"));

      return failedFuture;
    }

    String queryAction = requestParts[3];
    CompletableFuture<TransportClientResponse> responseFuture;
    if (!STORAGE_ACTION.equalsIgnoreCase(queryAction)) {
      LOGGER.debug("Delegating unsupported query action ({}), to R2 client", queryAction);
      responseFuture = handleNonStorageQueries(requestPath, headers, requestBody, isSingleGet);
    } else {
      responseFuture = handleStorageQueries(requestParts, requestBody, isSingleGet);
    }

    return responseFuture;
  }

  @VisibleForTesting
  CompletableFuture<TransportClientResponse> handleNonStorageQueries(
      String requestPath,
      Map<String, String> headers,
      byte[] requestBody,
      boolean isSingleGet) {
    return isSingleGet
        ? r2TransportClientForNonStorageOps.get(requestPath, headers)
        : r2TransportClientForNonStorageOps.post(requestPath, headers, requestBody);
  }

  @VisibleForTesting
  CompletableFuture<TransportClientResponse> handleStorageQueries(
      String[] requestParts,
      byte[] requestBody,
      boolean isSingleGet) {
    CompletableFuture<TransportClientResponse> responseFuture = new CompletableFuture<>();
    VeniceClientRequest veniceClientRequest = buildVeniceClientRequest(requestParts, requestBody, isSingleGet);
    VeniceReadServiceGrpc.VeniceReadServiceStub clientStub = getOrCreateStub(requestParts[2]);

    if (isSingleGet) {
      clientStub.get(veniceClientRequest, new VeniceGrpcStreamObserver(responseFuture));
    } else {
      clientStub.batchGet(veniceClientRequest, new VeniceGrpcStreamObserver(responseFuture));
    }

    return responseFuture;
  }

  @VisibleForTesting
  boolean isValidRequest(String[] requestParts, boolean isSingleGet) {
    int requestPartsLength = requestParts.length;
    boolean validRequest = false;

    /*
     * Ensure the request path contain at least 4 parts.
     * For non-storage query action, delegate the validation to underlying r2 transport client and treat it as valid
     * For storage query action, ensure the length is 7 for single gets and 5 for multi get. Refer to #handleRequest
     * documentation for the format of request path.
     */
    if (requestPartsLength >= 4) {
      validRequest = !STORAGE_ACTION.equalsIgnoreCase(requestParts[3])
          || (isSingleGet ? requestPartsLength == 7 : requestPartsLength == 5);
    }

    return validRequest;
  }

  static class VeniceGrpcStreamObserver implements StreamObserver<VeniceServerResponse> {
    private final CompletableFuture<TransportClientResponse> responseFuture;

    public VeniceGrpcStreamObserver(CompletableFuture<TransportClientResponse> responseFuture) {
      Preconditions.checkArgument(!responseFuture.isDone());
      this.responseFuture = responseFuture;
    }

    @Override
    public void onNext(VeniceServerResponse value) {
      int statusCode = value.getErrorCode();
      // Successful response
      if (statusCode == VeniceReadResponseStatus.OK.getCode()) {
        complete(
            new TransportClientResponse(
                value.getSchemaId(),
                CompressionStrategy.valueOf(value.getCompressionStrategy()),
                value.getData().toByteArray()),
            null);
        return;
      }
      // Key not found is a valid response
      if (statusCode == VeniceReadResponseStatus.KEY_NOT_FOUND.getCode()) {
        complete(null, null);
        return;
      }
      // Handle the cases where the status code doesn't match healthy response codes
      handleResponseError(value);
    }

    @Override
    public void onError(Throwable t) {
      LOGGER.error("Encountered error when handling request due to", t);
      handleGrpcError(t);
    }

    @Override
    public void onCompleted() {
      LOGGER.debug("Completed gRPC request");
    }

    void complete(TransportClientResponse transportClientResponse, Throwable t) {
      if (!responseFuture.isDone()) {
        if (t != null) {
          responseFuture.completeExceptionally(t);
        } else {
          responseFuture.complete(transportClientResponse);
        }
      } else {
        LOGGER.warn("Attempting to complete a future that is already completed");
      }
    }

    @VisibleForTesting
    void handleResponseError(VeniceServerResponse response) {
      int statusCode = response.getErrorCode();
      String errorMessage = response.getErrorMessage();
      Exception exception;
      try {
        switch (VeniceReadResponseStatus.fromCode(statusCode)) {
          case BAD_REQUEST:
            exception = new VeniceClientHttpException(errorMessage, statusCode);
            break;
          case TOO_MANY_REQUESTS:
            exception = new VeniceClientRateExceededException(errorMessage);
            break;
          default:
            exception = new VeniceClientException(
                String.format(
                    "An unexpected error occurred with status code: %d, message: %s",
                    statusCode,
                    errorMessage));
            break;
        }
      } catch (IllegalArgumentException e) {
        // Handle the case where the status code doesn't match any known values
        exception = new VeniceClientException(
            String.format("Unknown status code: %d, message: %s", statusCode, errorMessage),
            e);
      }
      LOGGER.error("Received error response with status code: {}, message: {}", statusCode, errorMessage);
      complete(null, exception);
    }

    @VisibleForTesting
    void handleGrpcError(Throwable t) {
      Exception exception;
      Status errorStatus = Status.fromThrowable(t);
      int statusCode = errorStatus.getCode().value();
      String errorDescription = errorStatus.getDescription();

      switch (errorStatus.getCode()) {
        case PERMISSION_DENIED:
        case UNAUTHENTICATED:
        case INVALID_ARGUMENT:
          // these errors are purposefully raised by the server, and we provide a more specific message when they
          // occur
          exception = new VeniceClientHttpException(errorDescription, statusCode);
          break;
        default:
          exception = new VeniceClientException(
              String.format(
                  "An unexpected gRPC error occurred with status code: %d, message: %s",
                  statusCode,
                  errorDescription));
          break;
      }

      LOGGER.error("GRPC error occurred with status code: {}, message: {}", statusCode, errorDescription);
      complete(null, exception);
    }
  }
}
