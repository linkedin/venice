package com.linkedin.venice.controllerapi.transport;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.controllerapi.LeaderControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.request.DiscoverLeaderControllerRequest;
import com.linkedin.venice.controllerapi.request.NewStoreRequest;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.protocols.CreateStoreGrpcRequest;
import com.linkedin.venice.protocols.CreateStoreGrpcResponse;
import com.linkedin.venice.protocols.LeaderControllerGrpcRequest;
import com.linkedin.venice.protocols.LeaderControllerGrpcResponse;
import com.linkedin.venice.protocols.VeniceControllerGrpcServiceGrpc;
import com.linkedin.venice.protocols.VeniceControllerGrpcServiceGrpc.VeniceControllerGrpcServiceBlockingStub;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ControllerGrpcTransport implements ControllerTransportAdapter {
  private static final Logger LOGGER = LogManager.getLogger(ControllerGrpcTransport.class);

  private final ControllerTransportAdapterConfigs transportAdapterConfigs;
  private final List<String> controllerDiscoveryUrls;
  private final ControllerHttpTransport fallbackHttpTransport;
  private final Map<String, ManagedChannel> grpcChannelCache;
  private final ChannelCredentials channelCredentials;

  public ControllerGrpcTransport(
      ControllerTransportAdapterConfigs transportAdapterConfigs,
      ControllerHttpTransport fallbackHttpTransport) {
    this.transportAdapterConfigs = transportAdapterConfigs;
    this.controllerDiscoveryUrls = transportAdapterConfigs.getControllerDiscoveryUrls();
    this.fallbackHttpTransport = fallbackHttpTransport;
    this.grpcChannelCache = new VeniceConcurrentHashMap<>(4);
    this.channelCredentials = InsecureChannelCredentials.create(); // TODO: Use secure channel credentials if configured
  }

  ManagedChannel getOrCreateChannel(String serverGrpcAddress) {
    return grpcChannelCache
        .computeIfAbsent(serverGrpcAddress, k -> Grpc.newChannelBuilder(serverGrpcAddress, channelCredentials).build());
  }

  @Override
  public LeaderControllerResponse discoverLeaderController(DiscoverLeaderControllerRequest request) {
    List<String> controllerDiscoveryUrls = new ArrayList<>(this.controllerDiscoveryUrls);
    Collections.shuffle(controllerDiscoveryUrls);

    Exception lastConnectException = null;
    Exception lastException = null;
    for (String url: controllerDiscoveryUrls) {
      try {
        LeaderControllerGrpcRequest grpcRequest =
            LeaderControllerGrpcRequest.newBuilder().setClusterName(request.getClusterName()).build();
        VeniceControllerGrpcServiceBlockingStub stub =
            VeniceControllerGrpcServiceGrpc.newBlockingStub(getOrCreateChannel(url));
        LeaderControllerGrpcResponse grpcResponse = stub.getLeaderController(grpcRequest);

        LeaderControllerResponse response = new LeaderControllerResponse();
        response.setCluster(grpcResponse.getClusterName());
        response.setGrpcUrl(grpcResponse.getGrpcUrl());
        response.setSecureGrpcUrl(grpcResponse.getSecureGrpcUrl());
        response.setUrl(grpcResponse.getHttpUrl());
        response.setSecureUrl(grpcResponse.getHttpsUrl());
        LOGGER.info("Discovered leader controller {} from {}", response, url);
        return response;
      } catch (Exception e) {
        Status status = Status.fromThrowable(e);
        LOGGER.warn("Unable to discover leader controller from {}. Status: {}", url, status);
        if (status.getCode() == Status.Code.UNAVAILABLE) {
          lastConnectException = e;
        } else {
          lastException = e;
        }
      }
    }

    if (lastException == null) {
      lastException = lastConnectException;
    }

    String message = "Unable to discover leader controller from " + controllerDiscoveryUrls;
    LOGGER.error(message, lastException);
    throw new VeniceException(message, lastException);
  }

  private String getLeaderControllerUrl(String clusterName) {
    return discoverLeaderController(new DiscoverLeaderControllerRequest(clusterName)).getGrpcUrl();
  }

  @Override
  public NewStoreResponse createNewStore(NewStoreRequest request) {
    try {
      // ControllerEndPointParamValidator.validateNewStoreRequest(request);
      VeniceControllerGrpcServiceBlockingStub stub = VeniceControllerGrpcServiceGrpc
          .newBlockingStub(getOrCreateChannel(getLeaderControllerUrl(request.getClusterName())));
      // convert the NewStoreRequest to a gRPC request using the utility class
      CreateStoreGrpcRequest grpcRequest = GrpcRequestResponseConverter.toGrpcCreateStoreGrpcRequest(request);

      // make gRPC request
      CreateStoreGrpcResponse grpcResponse = stub.createStore(grpcRequest);

      // convert the gRPC response to NewStoreResponse using the utility class
      return GrpcRequestResponseConverter.fromGrpcResponse(grpcResponse);
    } catch (StatusRuntimeException e) {
      LOGGER.error("Error creating store", e);
      throw GrpcRequestResponseConverter.handleGrpcError(e);
    } catch (Exception e) {
      LOGGER.error("Error creating store", e);
      throw new VeniceClientException("Error creating store", e);
    }
  }
}
