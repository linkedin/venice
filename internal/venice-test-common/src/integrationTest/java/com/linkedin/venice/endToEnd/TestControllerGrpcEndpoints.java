package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_GRPC_SERVER_ENABLED;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_KEY_SCHEMA;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.protocols.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.CreateStoreGrpcRequest;
import com.linkedin.venice.protocols.CreateStoreGrpcResponse;
import com.linkedin.venice.protocols.VeniceControllerGrpcServiceGrpc;
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
        .numberOfServers(2)
        .numberOfControllers(1)
        .sslToKafka(false)
        .sslToStorageNodes(false)
        .extraProperties(properties)
        .build();
    veniceCluster = ServiceFactory.getVeniceCluster(options);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    Utils.closeQuietlyWithErrorLogged(veniceCluster);
  }

  @Test
  public void testGrpcEndpoints() {
    String storeName = Utils.getUniqueString("store");
    try (ControllerClient controllerClient =
        new ControllerClient(veniceCluster.getClusterName(), veniceCluster.getAllControllersGrpcURLs(), true)) {
      TestUtils
          .assertCommand(controllerClient.createNewStore(storeName, "owner", DEFAULT_KEY_SCHEMA, "\"string\"", null));
    }
    try (ControllerClient controllerClient =
        new ControllerClient(veniceCluster.getClusterName(), veniceCluster.getAllControllersURLs(), false)) {
      StoreResponse storeResponse = TestUtils.assertCommand(controllerClient.getStore(storeName));
      System.out.println(storeResponse);
    }
  }

  @Test
  public void testGrpcEndpointsWithGrpcClient() {
    String storeName = Utils.getUniqueString("test_grpc_store");
    String controllerGrpcUrl = veniceCluster.getLeaderVeniceController().getControllerGrpcUrl();
    ManagedChannel channel = Grpc.newChannelBuilder(controllerGrpcUrl, InsecureChannelCredentials.create()).build();
    VeniceControllerGrpcServiceGrpc.VeniceControllerGrpcServiceBlockingStub controllerGrpcServiceBlockingStub =
        VeniceControllerGrpcServiceGrpc.newBlockingStub(channel);

    ClusterStoreGrpcInfo clusterStoreGrpcInfo = ClusterStoreGrpcInfo.newBuilder()
        .setClusterName(veniceCluster.getClusterName())
        .setStoreName(storeName)
        .build();
    CreateStoreGrpcRequest createStoreGrpcRequest = CreateStoreGrpcRequest.newBuilder()
        .setClusterStoreInfo(clusterStoreGrpcInfo)
        .setOwner("owner")
        .setKeySchema(DEFAULT_KEY_SCHEMA)
        .setValueSchema("\"string\"")
        .build();

    CreateStoreGrpcResponse response = controllerGrpcServiceBlockingStub.createStore(createStoreGrpcRequest);
    assertEquals(response.getClusterStoreInfo().getClusterName(), veniceCluster.getClusterName());
    assertEquals(response.getClusterStoreInfo().getStoreName(), storeName);

    try (ControllerClient controllerClient =
        new ControllerClient(veniceCluster.getClusterName(), veniceCluster.getAllControllersURLs())) {
      // controllerClient.createNewStore()
      StoreResponse storeResponse = TestUtils.assertCommand(controllerClient.getStore(storeName));
      System.out.println(storeResponse);
    }
  }
}
