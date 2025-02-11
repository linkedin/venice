package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_GRPC_SERVER_ENABLED;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_KEY_SCHEMA;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.acl.NoOpDynamicAccessController;
import com.linkedin.venice.authorization.Method;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.grpc.GrpcUtils;
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
import com.linkedin.venice.protocols.controller.StoreGrpcServiceGrpc;
import com.linkedin.venice.protocols.controller.VeniceControllerGrpcServiceGrpc;
import com.linkedin.venice.protocols.controller.VeniceControllerGrpcServiceGrpc.VeniceControllerGrpcServiceBlockingStub;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import java.security.cert.X509Certificate;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestControllerGrpcEndpoints {
  private VeniceClusterWrapper veniceCluster;
  private SSLFactory sslFactory;
  private MockDynamicAccessController mockDynamicAccessController;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    mockDynamicAccessController = new MockDynamicAccessController();
    sslFactory = SslUtils.getVeniceLocalSslFactory();
    Properties properties = new Properties();
    properties.put(CONTROLLER_GRPC_SERVER_ENABLED, true);
    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfRouters(1)
        .numberOfServers(1)
        .accessController(mockDynamicAccessController)
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
    StoreGrpcServiceGrpc.StoreGrpcServiceBlockingStub storeBlockingStub = StoreGrpcServiceGrpc.newBlockingStub(channel);

    // Test 1: getLeaderControllerDetails
    LeaderControllerGrpcResponse grpcResponse = blockingStub.getLeaderController(
        LeaderControllerGrpcRequest.newBuilder().setClusterName(veniceCluster.getClusterName()).build());
    assertEquals(grpcResponse.getHttpUrl(), veniceCluster.getLeaderVeniceController().getControllerUrl());
    assertEquals(grpcResponse.getGrpcUrl(), veniceCluster.getLeaderVeniceController().getControllerGrpcUrl());
    assertEquals(
        grpcResponse.getSecureGrpcUrl(),
        veniceCluster.getLeaderVeniceController().getControllerSecureGrpcUrl());

    // Test 2: createStore
    ClusterStoreGrpcInfo storeGrpcInfo = ClusterStoreGrpcInfo.newBuilder()
        .setClusterName(veniceCluster.getClusterName())
        .setStoreName(storeName)
        .build();
    CreateStoreGrpcRequest createStoreGrpcRequest = CreateStoreGrpcRequest.newBuilder()
        .setStoreInfo(storeGrpcInfo)
        .setOwner("owner")
        .setKeySchema(DEFAULT_KEY_SCHEMA)
        .setValueSchema("\"string\"")
        .build();

    CreateStoreGrpcResponse response = storeBlockingStub.createStore(createStoreGrpcRequest);
    assertNotNull(response, "Response should not be null");
    assertNotNull(response.getStoreInfo(), "ClusterStoreInfo should not be null");
    assertEquals(response.getStoreInfo().getClusterName(), veniceCluster.getClusterName());
    assertEquals(response.getStoreInfo().getStoreName(), storeName);

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

  @Test
  public void testCreateStoreOverSecureGrpcChannel() {
    String storeName = Utils.getUniqueString("test_grpc_store");
    String controllerSecureGrpcUrl = veniceCluster.getLeaderVeniceController().getControllerSecureGrpcUrl();
    ChannelCredentials credentials = GrpcUtils.buildChannelCredentials(sslFactory);
    ManagedChannel channel = Grpc.newChannelBuilder(controllerSecureGrpcUrl, credentials).build();
    StoreGrpcServiceGrpc.StoreGrpcServiceBlockingStub storeBlockingStub = StoreGrpcServiceGrpc.newBlockingStub(channel);

    CreateStoreGrpcRequest createStoreGrpcRequest = CreateStoreGrpcRequest.newBuilder()
        .setStoreInfo(
            ClusterStoreGrpcInfo.newBuilder()
                .setClusterName(veniceCluster.getClusterName())
                .setStoreName(storeName)
                .build())
        .setOwner("owner")
        .setKeySchema(DEFAULT_KEY_SCHEMA)
        .setValueSchema("\"string\"")
        .build();

    // Case 1: User not in allowlist for the resource
    mockDynamicAccessController.removeResourceFromAllowList(storeName);
    assertFalse(
        mockDynamicAccessController.isAllowlistUsers(null, storeName, Method.GET.name()),
        "User should not be in allowlist");
    StatusRuntimeException exception =
        Assert.expectThrows(StatusRuntimeException.class, () -> storeBlockingStub.createStore(createStoreGrpcRequest));
    assertEquals(exception.getStatus().getCode(), io.grpc.Status.Code.PERMISSION_DENIED);

    // Case 2: Allowlist user
    mockDynamicAccessController.addResourceToAllowList(storeName);
    CreateStoreGrpcResponse okResponse = storeBlockingStub.createStore(createStoreGrpcRequest);
    assertNotNull(okResponse, "Response should not be null");
    assertNotNull(okResponse.getStoreInfo(), "ClusterStoreInfo should not be null");
    assertEquals(okResponse.getStoreInfo().getClusterName(), veniceCluster.getClusterName());
    assertEquals(okResponse.getStoreInfo().getStoreName(), storeName);

    veniceCluster.useControllerClient(controllerClient -> {
      StoreResponse storeResponse = TestUtils.assertCommand(controllerClient.getStore(storeName));
      assertNotNull(storeResponse.getStore(), "Store should not be null");
    });
  }

  private static class MockDynamicAccessController extends NoOpDynamicAccessController {
    private final Set<String> resourcesInAllowList = ConcurrentHashMap.newKeySet();

    @Override
    public boolean isAllowlistUsers(X509Certificate clientCert, String resource, String method) {
      return resourcesInAllowList.contains(resource);
    }

    public void addResourceToAllowList(String resource) {
      resourcesInAllowList.add(resource);
    }

    public void removeResourceFromAllowList(String resource) {
      resourcesInAllowList.remove(resource);
    }
  }
}
