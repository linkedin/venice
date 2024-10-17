package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_GRPC_SERVER_ENABLED;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_KEY_SCHEMA;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.protocols.controller.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.controller.CreateStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.CreateStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.DiscoverClusterGrpcRequest;
import com.linkedin.venice.protocols.controller.DiscoverClusterGrpcResponse;
import com.linkedin.venice.protocols.controller.LeaderControllerGrpcRequest;
import com.linkedin.venice.protocols.controller.LeaderControllerGrpcResponse;
import com.linkedin.venice.protocols.controller.VeniceControllerGrpcServiceGrpc;
import com.linkedin.venice.protocols.controller.VeniceControllerGrpcServiceGrpc.VeniceControllerGrpcServiceBlockingStub;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import java.util.Properties;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestControllerGrpcEndpoints {
  private VeniceClusterWrapper veniceCluster;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Properties properties = new Properties();
    properties.put(CONTROLLER_GRPC_SERVER_ENABLED, true);
    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfRouters(1)
        .numberOfServers(1)
        .extraProperties(properties)
        .build();
    veniceCluster = ServiceFactory.getVeniceCluster(options);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    Utils.closeQuietlyWithErrorLogged(veniceCluster);
  }

  @Test
  public void testGrpcEndpointsWithGrpcClient() {
    String storeName = Utils.getUniqueString("test_grpc_store");
    String controllerGrpcUrl = veniceCluster.getLeaderVeniceController().getControllerGrpcUrl();
    ManagedChannel channel = Grpc.newChannelBuilder(controllerGrpcUrl, InsecureChannelCredentials.create()).build();
    VeniceControllerGrpcServiceBlockingStub blockingStub = VeniceControllerGrpcServiceGrpc.newBlockingStub(channel);

    // Test 1: getLeaderControllerDetails
    LeaderControllerGrpcResponse grpcResponse = blockingStub.getLeaderController(
        LeaderControllerGrpcRequest.newBuilder().setClusterName(veniceCluster.getClusterName()).build());
    assertEquals(grpcResponse.getHttpUrl(), veniceCluster.getLeaderVeniceController().getControllerUrl());
    assertEquals(grpcResponse.getGrpcUrl(), veniceCluster.getLeaderVeniceController().getControllerGrpcUrl());
    assertEquals(
        grpcResponse.getSecureGrpcUrl(),
        veniceCluster.getLeaderVeniceController().getControllerSecureGrpcUrl());

    // Test 2: createStore
    CreateStoreGrpcRequest createStoreGrpcRequest = CreateStoreGrpcRequest.newBuilder()
        .setClusterStoreInfo(
            ClusterStoreGrpcInfo.newBuilder()
                .setClusterName(veniceCluster.getClusterName())
                .setStoreName(storeName)
                .build())
        .setOwner("owner")
        .setKeySchema(DEFAULT_KEY_SCHEMA)
        .setValueSchema("\"string\"")
        .build();

    CreateStoreGrpcResponse response = blockingStub.createStore(createStoreGrpcRequest);
    assertNotNull(response, "Response should not be null");
    assertNotNull(response.getClusterStoreInfo(), "ClusterStoreInfo should not be null");
    assertEquals(response.getClusterStoreInfo().getClusterName(), veniceCluster.getClusterName());
    assertEquals(response.getClusterStoreInfo().getStoreName(), storeName);

    veniceCluster.useControllerClient(controllerClient -> {
      StoreResponse storeResponse = TestUtils.assertCommand(controllerClient.getStore(storeName));
      assertNotNull(storeResponse.getStore(), "Store should not be null");
    });

    // Test 3: discover cluster
    DiscoverClusterGrpcRequest discoverClusterGrpcRequest =
        DiscoverClusterGrpcRequest.newBuilder().setStoreName(storeName).build();
    DiscoverClusterGrpcResponse discoverClusterGrpcResponse =
        blockingStub.discoverClusterForStore(discoverClusterGrpcRequest);
    assertNotNull(discoverClusterGrpcResponse, "Response should not be null");
    assertEquals(discoverClusterGrpcResponse.getStoreName(), storeName);
    assertEquals(discoverClusterGrpcResponse.getClusterName(), veniceCluster.getClusterName());
  }
}
