package com.linkedin.venice.controllerapi.transport;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.controllerapi.LeaderControllerResponse;
import com.linkedin.venice.controllerapi.MultiStoreResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.controllerapi.request.DiscoverLeaderControllerRequest;
import com.linkedin.venice.controllerapi.request.EmptyPushRequest;
import com.linkedin.venice.controllerapi.request.GetStoreRequest;
import com.linkedin.venice.controllerapi.request.ListStoresRequest;
import com.linkedin.venice.controllerapi.request.NewStoreRequest;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.protocols.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.CreateStoreGrpcRequest;
import com.linkedin.venice.protocols.CreateStoreGrpcResponse;
import com.linkedin.venice.protocols.EmptyPushGrpcRequest;
import com.linkedin.venice.protocols.EmptyPushGrpcResponse;
import com.linkedin.venice.protocols.GetStoreGrpcRequest;
import com.linkedin.venice.protocols.GetStoreGrpcResponse;
import com.linkedin.venice.protocols.LeaderControllerGrpcRequest;
import com.linkedin.venice.protocols.LeaderControllerGrpcResponse;
import com.linkedin.venice.protocols.ListStoresGrpcRequest;
import com.linkedin.venice.protocols.ListStoresGrpcResponse;
import com.linkedin.venice.protocols.VeniceControllerGrpcServiceGrpc;
import com.linkedin.venice.protocols.VeniceControllerGrpcServiceGrpc.VeniceControllerGrpcServiceBlockingStub;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.grpc.ChannelCredentials;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
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
  private final String controllerGrpcUrl;

  public ControllerGrpcTransport(
      ControllerTransportAdapterConfigs transportAdapterConfigs,
      ControllerHttpTransport fallbackHttpTransport) {
    this.transportAdapterConfigs = transportAdapterConfigs;
    // this.controllerDiscoveryUrls = transportAdapterConfigs.getControllerDiscoveryUrls();
    this.controllerDiscoveryUrls = Collections.singletonList(transportAdapterConfigs.getControllerGrpcUrl());
    this.fallbackHttpTransport = fallbackHttpTransport;
    this.grpcChannelCache = new VeniceConcurrentHashMap<>(4);
    this.channelCredentials = InsecureChannelCredentials.create(); // TODO: Use secure channel credentials if configured
    this.controllerGrpcUrl = transportAdapterConfigs.getControllerGrpcUrl();
  }

  ManagedChannel getOrCreateChannel(String serverGrpcAddress) {
    return grpcChannelCache.computeIfAbsent(serverGrpcAddress, k -> {
      ManagedChannel channel = ManagedChannelBuilder.forTarget(serverGrpcAddress)
          .usePlaintext()
          .defaultLoadBalancingPolicy("pick_first")
          .build();
      return channel;
    });
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
      throw new VeniceClientException("Error creating store", e); // change it http exception
    }
  }

  @Override
  public StoreResponse getStore(GetStoreRequest getStoreRequest) {
    try {
      // ControllerEndPointParamValidator.validateGetStoreRequest(getStoreRequest);
      VeniceControllerGrpcServiceBlockingStub stub = getBlockingStub(getStoreRequest.getClusterName());

      // convert the GetStoreRequest to a gRPC request using the utility class
      GetStoreGrpcRequest grpcRequest = GetStoreGrpcRequest.newBuilder()
          .setClusterStoreInfo(
              ClusterStoreGrpcInfo.newBuilder()
                  .setClusterName(getStoreRequest.getClusterName())
                  .setStoreName(getStoreRequest.getStoreName())
                  .build())
          .build();

      // make gRPC request
      GetStoreGrpcResponse grpcResponse = stub.getStore(grpcRequest);

      // convert the gRPC response to StoreResponse using the utility class
      StoreResponse response = new StoreResponse();
      response.setCluster(grpcResponse.getClusterStoreInfo().getClusterName());
      response.setName(grpcResponse.getClusterStoreInfo().getStoreName());
      response.setStore(GrpcRequestResponseConverter.toStoreInfo(grpcResponse.getStoreInfo()));
      return response;
    } catch (StatusRuntimeException e) {
      LOGGER.error("Error getting store", e);
      throw GrpcRequestResponseConverter.handleGrpcError(e);
    } catch (Exception e) {
      LOGGER.error("Error getting store", e);
      throw new VeniceClientException("Error getting store", e); // change it http exception
    }
  }

  private VeniceControllerGrpcServiceBlockingStub getBlockingStub(String clusterName) {
    String leaderControllerUrl = getLeaderControllerUrl(clusterName);
    ManagedChannel channel = getOrCreateChannel(leaderControllerUrl);
    return VeniceControllerGrpcServiceGrpc.newBlockingStub(channel);
  }

  @Override
  public VersionCreationResponse emptyPush(EmptyPushRequest emptyPushRequest) {
    String clusterName = emptyPushRequest.getClusterName();
    String storeName = emptyPushRequest.getStoreName();
    String pushJobId = emptyPushRequest.getPushJobId();
    try {
      EmptyPushGrpcRequest grpcRequest = EmptyPushGrpcRequest.newBuilder()
          .setClusterStoreInfo(GrpcRequestResponseConverter.getClusterStoreGrpcInfo(emptyPushRequest))
          .setPushJobId(pushJobId)
          .build();
      EmptyPushGrpcResponse grpcResponse = getBlockingStub(clusterName).emptyPush(grpcRequest);
      return GrpcRequestResponseConverter.toVersionCreationResponse(grpcResponse);
    } catch (StatusRuntimeException e) {
      LOGGER.error("Error empty pushing to store {} in cluster {}", storeName, clusterName, e);
      throw GrpcRequestResponseConverter.handleGrpcError(e);
    } catch (Exception e) {
      LOGGER.error("Error empty pushing to store {} in cluster {}", storeName, clusterName, e);
      throw new VeniceClientException("Error empty pushing", e); // change it http exception
    }
  }

  @Override
  public MultiStoreResponse listStores(ListStoresRequest listStoresRequest) {
    try {

      VeniceControllerGrpcServiceBlockingStub stub = getBlockingStub(listStoresRequest.getClusterName());
      // convert the ListStoresRequest to a gRPC request using the utility class
      ListStoresGrpcRequest.Builder builder = ListStoresGrpcRequest.newBuilder()
          .setClusterName(listStoresRequest.getClusterName())
          .setIncludeSystemStores(listStoresRequest.includeSystemStores());

      if (listStoresRequest.getStoreConfigNameFilter() != null) {
        builder.setStoreConfigNameFilter(listStoresRequest.getStoreConfigNameFilter());
      }
      if (listStoresRequest.getStoreConfigValueFilter() != null) {
        builder.setStoreConfigValueFilter(listStoresRequest.getStoreConfigValueFilter());
      }
      ListStoresGrpcResponse grpcResponse = stub.listStores(builder.build());
      MultiStoreResponse response = new MultiStoreResponse();
      response.setStores(grpcResponse.getStoreNameList().toArray(new String[0]));
      return response;
    } catch (StatusRuntimeException e) {
      LOGGER.error("Error listing stores", e);
      throw GrpcRequestResponseConverter.handleGrpcError(e);
    } catch (Exception e) {
      LOGGER.error("Error listing stores", e);
      throw new VeniceClientException("Error listing stores", e); // change it http exception
    }

  }
}
