package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_KEY_SCHEMA;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.protocols.CreateStoreGrpcRequest;
import com.linkedin.venice.protocols.CreateStoreGrpcResponse;
import com.linkedin.venice.protocols.VeniceControllerGrpcServiceGrpc;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestControllerGrpcEndpoints {
  private VeniceClusterWrapper veniceCluster;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfRouters(1)
        .numberOfServers(2)
        .numberOfControllers(1)
        .sslToKafka(false)
        .sslToStorageNodes(false)
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
    String controllerGrpcUrl = veniceCluster.getLeaderVeniceController().getControllerGrpcUrl();
    ManagedChannel channel = Grpc.newChannelBuilder(controllerGrpcUrl, InsecureChannelCredentials.create()).build();
    VeniceControllerGrpcServiceGrpc.VeniceControllerGrpcServiceBlockingStub controllerGrpcServiceBlockingStub =
        VeniceControllerGrpcServiceGrpc.newBlockingStub(channel);

    CreateStoreGrpcRequest createStoreGrpcRequest = CreateStoreGrpcRequest.newBuilder()
        .setClusterName(veniceCluster.getClusterName())
        .setStoreName(storeName)
        .setOwner("owner")
        .setKeySchema(DEFAULT_KEY_SCHEMA)
        .setValueSchema("\"string\"")
        .build();

    CreateStoreGrpcResponse response = controllerGrpcServiceBlockingStub.createStore(createStoreGrpcRequest);
    System.out.println(response.getStatusMessage());
    Assert.assertEquals(response.getStatusCode(), 200);

    try (ControllerClient controllerClient =
        new ControllerClient(veniceCluster.getClusterName(), veniceCluster.getAllControllersURLs())) {
      // controllerClient.createNewStore()
      StoreResponse storeResponse = TestUtils.assertCommand(controllerClient.getStore(storeName));
      System.out.println(storeResponse);
    }
  }
}
