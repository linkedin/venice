package com.linkedin.venice.grpc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.controllerapi.QueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.protocols.CreateStoreGrpcRequest;
import com.linkedin.venice.protocols.GetStoresInClusterGrpcRequest;
import com.linkedin.venice.protocols.QueryJobStatusGrpcRequest;
import com.linkedin.venice.protocols.VeniceControllerGrpcServiceGrpc;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.TlsChannelCredentials;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;


public class ControllerGrpcTransport implements AutoCloseable {
  private static final int PORT = 1234;
  private static final String GRPC_ADDRESS_FORMAT = "%s:%s";
  private final VeniceConcurrentHashMap<String, ManagedChannel> serverGrpcChannels;
  private final ChannelCredentials channelCredentials;
  private final VeniceConcurrentHashMap<ManagedChannel, VeniceControllerGrpcServiceGrpc.VeniceControllerGrpcServiceStub> stubCache;

  public ControllerGrpcTransport(Optional<SSLFactory> sslFactory) {
    this.stubCache = new VeniceConcurrentHashMap<>();
    this.serverGrpcChannels = new VeniceConcurrentHashMap<>();
    this.channelCredentials = buildChannelCredentials(sslFactory);
  }

  public <ResT> CompletionStage<ResT> request(
      String serverUrl,
      QueryParams params,
      Class<ResT> responseType,
      GrpcControllerRoute route) {

    VeniceControllerGrpcServiceGrpc.VeniceControllerGrpcServiceStub stub = getOrCreateStub(serverUrl);
    CompletableFuture<ResT> valueFuture = new CompletableFuture<>();

    if (GrpcControllerRoute.CREATE_STORE.equals(route)) {
      stub.createStore(
          (CreateStoreGrpcRequest) GrpcConverters.getRequestConverter(route.getRequestType()).convert(params),
          buildStreamObserver(valueFuture, responseType, route));
    } else if (GrpcControllerRoute.GET_STORES_IN_CLUSTER.equals(route)) {
      stub.getStoresInCluster(
          (GetStoresInClusterGrpcRequest) GrpcConverters.getRequestConverter(route.getRequestType()).convert(params),
          buildStreamObserver(valueFuture, responseType, route));
    } else if (GrpcControllerRoute.QUERY_JOB_STATUS.equals(route)) {
      stub.getJobStatus(
          (QueryJobStatusGrpcRequest) GrpcConverters.getRequestConverter(route.getRequestType()).convert(params),
          buildStreamObserver(valueFuture, responseType, route));
    } else {
      throw new VeniceException("Unknown gRPC route; Failing the request");
    }

    return valueFuture;
  }

  @VisibleForTesting
  <T, ResT> ControllerGrpcObserver<T, ResT> buildStreamObserver(
      CompletableFuture<ResT> future,
      Class<ResT> httpResponseType,
      GrpcControllerRoute route) {
    return new ControllerGrpcObserver<>(future, httpResponseType, route);
  }

  @Override
  public void close() throws IOException {
    for (Map.Entry<String, ManagedChannel> entry: serverGrpcChannels.entrySet()) {
      entry.getValue().shutdown();
    }
  }

  @VisibleForTesting
  ChannelCredentials buildChannelCredentials(Optional<SSLFactory> sslFactory) {
    SSLFactory factory = sslFactory.orElse(null);

    if (factory == null) {
      return InsecureChannelCredentials.create();
    }

    try {
      TlsChannelCredentials.Builder tlsBuilder = TlsChannelCredentials.newBuilder()
          .keyManager(GrpcUtils.getKeyManagers(factory))
          .trustManager(GrpcUtils.getTrustManagers(factory));
      return tlsBuilder.build();
    } catch (Exception e) {
      throw new VeniceClientException(
          "Failed to initialize SSL channel credentials for Venice gRPC Transport Client",
          e);
    }
  }

  VeniceControllerGrpcServiceGrpc.VeniceControllerGrpcServiceStub getOrCreateStub(String serverAddress) {
    String grpcAddress = getGrpcAddressFromServerAddress(serverAddress);

    ManagedChannel channel = serverGrpcChannels
        .computeIfAbsent(serverAddress, k -> Grpc.newChannelBuilder(grpcAddress, channelCredentials).build());

    return stubCache.computeIfAbsent(channel, VeniceControllerGrpcServiceGrpc::newStub);
  }

  @VisibleForTesting
  String getGrpcAddressFromServerAddress(String serverAddress) {
    String[] serverAddressParts = serverAddress.split(":");
    Preconditions.checkState(serverAddressParts.length == 2, "Invalid server address");

    return String.format(GRPC_ADDRESS_FORMAT, serverAddressParts[0], PORT);
  }

  static class ControllerGrpcObserver<ResT, HttpResT> implements StreamObserver<ResT> {
    private final CompletableFuture<HttpResT> responseFuture;
    private final Class<HttpResT> httpResponseType;

    private final GrpcControllerRoute route;

    public ControllerGrpcObserver(
        CompletableFuture<HttpResT> future,
        Class<HttpResT> httpResponseType,
        GrpcControllerRoute route) {
      this.httpResponseType = httpResponseType;
      this.responseFuture = future;
      this.route = route;
    }

    @Override
    public void onNext(ResT value) {
      if (!responseFuture.isDone()) {

        @SuppressWarnings("Unchecked")
        HttpResT result = ((GrpcToHttpResponseConverter<ResT, HttpResT>) GrpcConverters
            .getResponseConverter(route.getResponseType(), httpResponseType)).convert(value);

        responseFuture.complete(result);
      }
    }

    @Override
    public void onError(Throwable t) {
      responseFuture.completeExceptionally(t);
    }

    @Override
    public void onCompleted() {
      // do nothing
    }
  }
}
