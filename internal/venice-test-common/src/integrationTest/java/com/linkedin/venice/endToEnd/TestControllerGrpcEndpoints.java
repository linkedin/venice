package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_GRPC_SERVER_ENABLED;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_KEY_SCHEMA;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.AdminTool;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiStoreResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.protocols.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.CreateStoreGrpcRequest;
import com.linkedin.venice.protocols.CreateStoreGrpcResponse;
import com.linkedin.venice.protocols.VeniceControllerGrpcServiceGrpc;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
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
  public void testAdminTool() throws Exception {
    String[] adminToolArgs = { "--url", veniceCluster.getAllControllersURLs(), "--cluster",
        veniceCluster.getClusterName(), "--controller-grpc-url",
        veniceCluster.getLeaderVeniceController().getControllerGrpcUrl(), "--list-stores" };
    AdminTool.main(adminToolArgs);
  }

  @Test
  public void testGrpcEndpoints() {
    String storeName = Utils.getUniqueString("store");

    try (ControllerClient controllerClient = new ControllerClient(
        veniceCluster.getClusterName(),
        veniceCluster.getAllControllersURLs(),
        Optional.empty(),
        veniceCluster.getAllControllersGrpcURLs())) {
      TestUtils
          .assertCommand(controllerClient.createNewStore(storeName, "owner", DEFAULT_KEY_SCHEMA, "\"string\"", null));
      StoreResponse storeResponse = TestUtils.assertCommand(controllerClient.getStore(storeName));
      assertNotNull(storeResponse);
      StoreInfo storeInfo = storeResponse.getStore();
      assertNotNull(storeInfo);
      AdminTool.printObject(storeInfo);
      System.out.println("Before adding a version");

      VersionCreationResponse versionCreationResponse =
          TestUtils.assertCommand(controllerClient.emptyPush(storeName, "pushJobId", Integer.MAX_VALUE));
      System.out.println(versionCreationResponse);

      Utils.sleep(120_000); // sleep for 2 minutes to allow the version to be added

      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
        System.out.println("Waiting for version to be added");
        StoreResponse storeResponse1 = TestUtils.assertCommand(controllerClient.getStore(storeName));
        assertNotNull(storeResponse1);
        StoreInfo storeInfo1 = storeResponse1.getStore();
        assertNotNull(storeInfo1);
        AdminTool.printObject(storeInfo1);
        List<Version> versions1 = storeInfo1.getVersions();
        assertNotNull(versions1);
        assertEquals(versions1.size(), 1);
        System.out.println(versions1.get(0));
      });

      versionCreationResponse =
          TestUtils.assertCommand(controllerClient.emptyPush(storeName, "pushJobId-2", Integer.MAX_VALUE));
      System.out.println(versionCreationResponse);

      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
        System.out.println("Waiting for version to be added");
        StoreResponse storeResponse1 = TestUtils.assertCommand(controllerClient.getStore(storeName));
        assertNotNull(storeResponse1);
        StoreInfo storeInfo1 = storeResponse1.getStore();
        assertNotNull(storeInfo1);
        AdminTool.printObject(storeInfo1);
        List<Version> versions1 = storeInfo1.getVersions();
        assertNotNull(versions1);
        // assertEquals(versions1.size(), 1);
        assertEquals(storeInfo1.getCurrentVersion(), 2);
      });

      // create one more store
      String storeName2 = Utils.getUniqueString("store");
      TestUtils
          .assertCommand(controllerClient.createNewStore(storeName2, "owner", DEFAULT_KEY_SCHEMA, "\"string\"", null));
      StoreResponse storeResponse2 = TestUtils.assertCommand(controllerClient.getStore(storeName2));
      assertNotNull(storeResponse2);
      StoreInfo storeInfo2 = storeResponse2.getStore();
      assertNotNull(storeInfo2);
      AdminTool.printObject(storeInfo2);

      // list all stores
      MultiStoreResponse multiStoreResponse = TestUtils.assertCommand(controllerClient.queryStoreList());
      assertNotNull(multiStoreResponse);
      String[] stores = multiStoreResponse.getStores();
      assertNotNull(stores);
      assertEquals(stores.length, 2);
      System.out.println("Stores: " + stores[0] + ", " + stores[1]);
    }

    System.out.println("Store created successfully");
    try (ControllerClient controllerClient =
        new ControllerClient(veniceCluster.getClusterName(), veniceCluster.getAllControllersURLs())) {
      StoreResponse storeResponse = TestUtils.assertCommand(controllerClient.getStore(storeName));
      System.out.println(storeResponse);
      StoreInfo storeInfo = storeResponse.getStore();
      assertNotNull(storeInfo);
      System.out.println(storeInfo);
      AdminTool.printObject(storeInfo);
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
