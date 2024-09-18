package com.linkedin.venice.fastclient.transport.grpc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.fastclient.GetRequestContext;
import com.linkedin.venice.fastclient.GrpcClientConfig;
import com.linkedin.venice.fastclient.MultiKeyRequestContext;
import com.linkedin.venice.fastclient.transport.FastClientTransport;
import com.linkedin.venice.grpc.GrpcUtils;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.protocols.ComputeRequest;
import com.linkedin.venice.protocols.MultiGetRequest;
import com.linkedin.venice.protocols.MultiKeyRequestKey;
import com.linkedin.venice.protocols.RpcRequestHeader;
import com.linkedin.venice.protocols.SingleGetRequest;
import com.linkedin.venice.protocols.VeniceReadServiceGrpc;
import com.linkedin.venice.protocols.VeniceReadServiceGrpc.VeniceReadServiceStub;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.TlsChannelCredentials;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class GrpcFastClientTransportAdapter implements FastClientTransport {
  private static final Logger LOGGER = LogManager.getLogger(GrpcFastClientTransportAdapter.class);
  private static final String GRPC_ADDRESS_FORMAT = "%s:%s";
  private final VeniceConcurrentHashMap<String, ManagedChannel> serverGrpcChannels;
  private final Map<String, String> nettyServerToGrpcAddress;
  // we cache stubs to avoid creating a new stub for each request, improves performance
  private final VeniceConcurrentHashMap<ManagedChannel, VeniceReadServiceStub> stubCache;
  private final ChannelCredentials channelCredentials;

  private final int port;
  private GrpcClientConfig grpcClientConfig;

  public GrpcFastClientTransportAdapter(GrpcClientConfig grpcClientConfig) {
    /*
     * General principle to not mutate input parameters and wrapping it up using a new map since this
     * class updates the map with inferred grpc address to prevent redundant computation for mapping netty
     * to grpc address
     */
    this(
        new HashMap<>(grpcClientConfig.getNettyServerToGrpcAddress()),
        grpcClientConfig.getPort(),
        grpcClientConfig.getSslFactory(),
        grpcClientConfig);
  }

  GrpcFastClientTransportAdapter(
      Map<String, String> nettyServerToGrpcAddress,
      int port,
      SSLFactory sslFactory,
      GrpcClientConfig grpcClientConfig) {
    this.nettyServerToGrpcAddress = nettyServerToGrpcAddress;
    this.port = port;
    this.serverGrpcChannels = new VeniceConcurrentHashMap<>();
    this.stubCache = new VeniceConcurrentHashMap<>();
    this.channelCredentials = buildChannelCredentials(sslFactory);
    this.grpcClientConfig = grpcClientConfig;
  }

  @Override
  public CompletableFuture<TransportClientResponse> singleGet(String route, GetRequestContext getRequestContext) {
    SingleGetRequest singleGetRequest = SingleGetRequest.newBuilder()
        .setResourceName(getRequestContext.getResourceName())
        .setPartition(getRequestContext.getPartitionId())
        .setKey(getRequestContext.getEncodedKey())
        .setKeyEncodingType(getRequestContext.getKeyEncodingType())
        .build();
    VeniceReadServiceStub clientAsyncStub = getOrCreateStub(route);
    SingleGetResponseObserver singleGetResponseObserver = new SingleGetResponseObserver();
    clientAsyncStub.singleGet(singleGetRequest, singleGetResponseObserver);
    return singleGetResponseObserver.getFuture();
  }

  @Override
  public <K, V> CompletableFuture<TransportClientResponse> multiKeyStreamingRequest(
      String route,
      MultiKeyRequestContext<K, V> requestContext,
      List<MultiKeyRequestContext.KeyInfo<K>> keysForRoutes,
      Function<List<MultiKeyRequestContext.KeyInfo<K>>, byte[]> requestSerializer,
      Map<String, String> headers) {
    QueryAction queryAction = requestContext.getQueryAction();

    boolean isRetryRequest = false;

    if (queryAction == QueryAction.COMPUTE) {
      byte[] computeRequestBytes = requestSerializer.apply(Collections.emptyList());
      ComputeRequest.Builder computeRequestBuilder = ComputeRequest.newBuilder()
          .setResourceName(requestContext.getResourceName())
          .setKeyCount(keysForRoutes.size())
          .setComputeRequestBytes(GrpcUtils.toByteString(computeRequestBytes));
      for (int i = 0; i < keysForRoutes.size(); i++) {
        MultiKeyRequestContext.KeyInfo<K> keyInfo = keysForRoutes.get(i);
        MultiKeyRequestKey.Builder multiKeyRequestKeyBuilder = MultiKeyRequestKey.newBuilder()
            .setKeyIndex(i)
            .setKeyBytes(GrpcUtils.toByteString(keyInfo.getSerializedKey()))
            .setPartition(keyInfo.getPartitionId());
        computeRequestBuilder.addKeys(multiKeyRequestKeyBuilder.build());
      }
      for (Map.Entry<String, String> entry: headers.entrySet()) {
        if (HttpConstants.VENICE_RETRY.equals(entry.getKey())) {
          isRetryRequest = true;
          continue;
        }
        RpcRequestHeader.Builder rpcRequestHeaderBuilder =
            RpcRequestHeader.newBuilder().setKey(entry.getKey()).setValue(entry.getValue());
        computeRequestBuilder.addHeaders(rpcRequestHeaderBuilder.build());
      }
      computeRequestBuilder.setIsRetryRequest(isRetryRequest);

      VeniceReadServiceStub clientAsyncStub = getOrCreateStub(route);
      MultiKeyStreamingResponseObserver responseObserver = new MultiKeyStreamingResponseObserver();
      clientAsyncStub.compute(computeRequestBuilder.build(), null);
      return responseObserver.getFuture();

    }
    MultiGetRequest.Builder multiGetRequestBuilder = MultiGetRequest.newBuilder()
        .setResourceName(requestContext.getResourceName())
        .setIsRetryRequest(isRetryRequest)
        .setKeyCount(keysForRoutes.size());

    for (int i = 0; i < keysForRoutes.size(); i++) {
      MultiKeyRequestContext.KeyInfo<K> keyInfo = keysForRoutes.get(i);
      MultiKeyRequestKey.Builder multiKeyRequestKeyBuilder = MultiKeyRequestKey.newBuilder()
          .setKeyIndex(i)
          .setKeyBytes(GrpcUtils.toByteString(keyInfo.getSerializedKey()))
          .setPartition(keyInfo.getPartitionId());
      multiGetRequestBuilder.addKeys(multiKeyRequestKeyBuilder.build());
    }

    for (Map.Entry<String, String> entry: headers.entrySet()) {
      if (HttpConstants.VENICE_RETRY.equals(entry.getKey())) {
        isRetryRequest = true;
        continue;
      }
      RpcRequestHeader.Builder rpcRequestHeaderBuilder =
          RpcRequestHeader.newBuilder().setKey(entry.getKey()).setValue(entry.getValue());
      multiGetRequestBuilder.addHeaders(rpcRequestHeaderBuilder.build());
    }
    multiGetRequestBuilder.setIsRetryRequest(isRetryRequest);

    VeniceReadServiceStub clientAsyncStub = getOrCreateStub(route);
    MultiKeyStreamingResponseObserver responseObserver = new MultiKeyStreamingResponseObserver();
    clientAsyncStub.multiGet(multiGetRequestBuilder.build(), null);
    return responseObserver.getFuture();
  }

  @Override
  public void close() throws IOException {
    for (Map.Entry<String, ManagedChannel> entry: serverGrpcChannels.entrySet()) {
      entry.getValue().shutdown();
    }
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
  VeniceReadServiceStub getOrCreateStub(String serverAddress) {
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
}
