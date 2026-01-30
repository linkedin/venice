package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_GRPC_SERVER_ENABLED;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_KEY_SCHEMA;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.acl.NoOpDynamicAccessController;
import com.linkedin.venice.authorization.Method;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.grpc.GrpcUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.protocols.controller.ClusterAdminOpsGrpcServiceGrpc;
import com.linkedin.venice.protocols.controller.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.controller.CreateStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.CreateStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.DiscoverClusterGrpcRequest;
import com.linkedin.venice.protocols.controller.DiscoverClusterGrpcResponse;
import com.linkedin.venice.protocols.controller.GetKeySchemaGrpcRequest;
import com.linkedin.venice.protocols.controller.GetKeySchemaGrpcResponse;
import com.linkedin.venice.protocols.controller.GetRepushInfoGrpcRequest;
import com.linkedin.venice.protocols.controller.GetRepushInfoGrpcResponse;
import com.linkedin.venice.protocols.controller.GetValueSchemaGrpcRequest;
import com.linkedin.venice.protocols.controller.GetValueSchemaGrpcResponse;
import com.linkedin.venice.protocols.controller.LeaderControllerGrpcRequest;
import com.linkedin.venice.protocols.controller.LeaderControllerGrpcResponse;
import com.linkedin.venice.protocols.controller.ListStoresGrpcRequest;
import com.linkedin.venice.protocols.controller.ListStoresGrpcResponse;
import com.linkedin.venice.protocols.controller.SchemaGrpcServiceGrpc;
import com.linkedin.venice.protocols.controller.StoreGrpcServiceGrpc;
import com.linkedin.venice.protocols.controller.StoreMigrationCheckGrpcRequest;
import com.linkedin.venice.protocols.controller.StoreMigrationCheckGrpcResponse;
import com.linkedin.venice.protocols.controller.ValidateStoreDeletedGrpcRequest;
import com.linkedin.venice.protocols.controller.ValidateStoreDeletedGrpcResponse;
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
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestControllerGrpcEndpoints {
  private static final long TIMEOUT_MS = 60_000;
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

  @Test(timeOut = TIMEOUT_MS)
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
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      DiscoverClusterGrpcResponse discoverClusterGrpcResponse =
          blockingStub.discoverClusterForStore(discoverClusterGrpcRequest);
      assertNotNull(discoverClusterGrpcResponse, "Response should not be null");
      assertEquals(discoverClusterGrpcResponse.getStoreName(), storeName);
      assertEquals(discoverClusterGrpcResponse.getClusterName(), veniceCluster.getClusterName());
    });
  }

  @Test(timeOut = TIMEOUT_MS)
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

  @Test(timeOut = TIMEOUT_MS)
  public void testValidateStoreDeletedGrpcEndpoint() {
    String storeName = Utils.getUniqueString("test_validate_deleted_store");
    String controllerGrpcUrl = veniceCluster.getLeaderVeniceController().getControllerGrpcUrl();
    ManagedChannel channel = Grpc.newChannelBuilder(controllerGrpcUrl, InsecureChannelCredentials.create()).build();
    StoreGrpcServiceGrpc.StoreGrpcServiceBlockingStub storeBlockingStub = StoreGrpcServiceGrpc.newBlockingStub(channel);

    ClusterStoreGrpcInfo storeGrpcInfo = ClusterStoreGrpcInfo.newBuilder()
        .setClusterName(veniceCluster.getClusterName())
        .setStoreName(storeName)
        .build();

    // Step 1: Validate store deleted before creation - should return true (store doesn't exist)
    ValidateStoreDeletedGrpcRequest validateRequest =
        ValidateStoreDeletedGrpcRequest.newBuilder().setStoreInfo(storeGrpcInfo).build();
    ValidateStoreDeletedGrpcResponse validateResponse = storeBlockingStub.validateStoreDeleted(validateRequest);
    assertNotNull(validateResponse, "Response should not be null");
    assertEquals(validateResponse.getStoreInfo().getStoreName(), storeName);
    assertEquals(validateResponse.getStoreInfo().getClusterName(), veniceCluster.getClusterName());

    // Step 2: Create the store
    CreateStoreGrpcRequest createStoreGrpcRequest = CreateStoreGrpcRequest.newBuilder()
        .setStoreInfo(storeGrpcInfo)
        .setOwner("owner")
        .setKeySchema(DEFAULT_KEY_SCHEMA)
        .setValueSchema("\"string\"")
        .build();
    CreateStoreGrpcResponse createResponse = storeBlockingStub.createStore(createStoreGrpcRequest);
    assertNotNull(createResponse, "Response should not be null");
    assertEquals(createResponse.getStoreInfo().getStoreName(), storeName);

    // Step 3: Validate store deleted after creation - should return false (store exists)
    validateResponse = storeBlockingStub.validateStoreDeleted(validateRequest);
    assertNotNull(validateResponse, "Response should not be null");
    assertFalse(validateResponse.getStoreDeleted(), "Store should not be marked as deleted");

    // Step 4: Delete the store using controller client
    veniceCluster.useControllerClient(controllerClient -> {
      TestUtils.assertCommand(controllerClient.disableAndDeleteStore(storeName));
    });

    // Step 5: Validate store deleted after deletion - should return true
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      ValidateStoreDeletedGrpcResponse response = storeBlockingStub.validateStoreDeleted(validateRequest);
      assertNotNull(response, "Response should not be null");
      Assert.assertTrue(response.getStoreDeleted(), "Store should be marked as deleted after deletion");
    });
  }

  @Test(timeOut = TIMEOUT_MS)
  public void testIsStoreMigrationAllowedGrpcEndpoint() {
    String controllerGrpcUrl = veniceCluster.getLeaderVeniceController().getControllerGrpcUrl();
    ManagedChannel channel = Grpc.newChannelBuilder(controllerGrpcUrl, InsecureChannelCredentials.create()).build();
    ClusterAdminOpsGrpcServiceGrpc.ClusterAdminOpsGrpcServiceBlockingStub blockingStub =
        ClusterAdminOpsGrpcServiceGrpc.newBlockingStub(channel);

    StoreMigrationCheckGrpcRequest request =
        StoreMigrationCheckGrpcRequest.newBuilder().setClusterName(veniceCluster.getClusterName()).build();

    StoreMigrationCheckGrpcResponse response = blockingStub.isStoreMigrationAllowed(request);

    assertNotNull(response, "Response should not be null");
    assertEquals(response.getClusterName(), veniceCluster.getClusterName());
    // By default, store migration is allowed in test clusters
    assertTrue(response.getStoreMigrationAllowed(), "Store migration should be allowed by default");
  }

  @Test(timeOut = TIMEOUT_MS)
  public void testValidateStoreDeletedOverSecureGrpcChannel() {
    String storeName = Utils.getUniqueString("test_validate_deleted_secure");
    String controllerSecureGrpcUrl = veniceCluster.getLeaderVeniceController().getControllerSecureGrpcUrl();
    ChannelCredentials credentials = GrpcUtils.buildChannelCredentials(sslFactory);
    ManagedChannel channel = Grpc.newChannelBuilder(controllerSecureGrpcUrl, credentials).build();
    StoreGrpcServiceGrpc.StoreGrpcServiceBlockingStub storeBlockingStub = StoreGrpcServiceGrpc.newBlockingStub(channel);

    ClusterStoreGrpcInfo storeGrpcInfo = ClusterStoreGrpcInfo.newBuilder()
        .setClusterName(veniceCluster.getClusterName())
        .setStoreName(storeName)
        .build();
    ValidateStoreDeletedGrpcRequest validateRequest =
        ValidateStoreDeletedGrpcRequest.newBuilder().setStoreInfo(storeGrpcInfo).build();

    // Case 1: User not in allowlist for the resource - should get permission denied
    mockDynamicAccessController.removeResourceFromAllowList(storeName);
    assertFalse(
        mockDynamicAccessController.isAllowlistUsers(null, storeName, Method.GET.name()),
        "User should not be in allowlist");
    StatusRuntimeException exception = Assert
        .expectThrows(StatusRuntimeException.class, () -> storeBlockingStub.validateStoreDeleted(validateRequest));
    assertEquals(exception.getStatus().getCode(), io.grpc.Status.Code.PERMISSION_DENIED);

    // Case 2: User in allowlist - should succeed
    mockDynamicAccessController.addResourceToAllowList(storeName);
    ValidateStoreDeletedGrpcResponse response = storeBlockingStub.validateStoreDeleted(validateRequest);
    assertNotNull(response, "Response should not be null");
    assertEquals(response.getStoreInfo().getStoreName(), storeName);
    assertEquals(response.getStoreInfo().getClusterName(), veniceCluster.getClusterName());
  }

  @Test(timeOut = TIMEOUT_MS)
  public void testListStoresGrpcEndpoint() {
    String storeName1 = Utils.getUniqueString("test_list_stores_1");
    String storeName2 = Utils.getUniqueString("test_list_stores_2");
    String controllerGrpcUrl = veniceCluster.getLeaderVeniceController().getControllerGrpcUrl();
    ManagedChannel channel = Grpc.newChannelBuilder(controllerGrpcUrl, InsecureChannelCredentials.create()).build();
    StoreGrpcServiceGrpc.StoreGrpcServiceBlockingStub storeBlockingStub = StoreGrpcServiceGrpc.newBlockingStub(channel);

    // Step 1: Create two stores
    CreateStoreGrpcRequest createStoreRequest1 = CreateStoreGrpcRequest.newBuilder()
        .setStoreInfo(
            ClusterStoreGrpcInfo.newBuilder()
                .setClusterName(veniceCluster.getClusterName())
                .setStoreName(storeName1)
                .build())
        .setOwner("owner")
        .setKeySchema(DEFAULT_KEY_SCHEMA)
        .setValueSchema("\"string\"")
        .build();
    CreateStoreGrpcResponse createResponse1 = storeBlockingStub.createStore(createStoreRequest1);
    assertNotNull(createResponse1, "Response should not be null");

    CreateStoreGrpcRequest createStoreRequest2 = CreateStoreGrpcRequest.newBuilder()
        .setStoreInfo(
            ClusterStoreGrpcInfo.newBuilder()
                .setClusterName(veniceCluster.getClusterName())
                .setStoreName(storeName2)
                .build())
        .setOwner("owner")
        .setKeySchema(DEFAULT_KEY_SCHEMA)
        .setValueSchema("\"string\"")
        .build();
    CreateStoreGrpcResponse createResponse2 = storeBlockingStub.createStore(createStoreRequest2);
    assertNotNull(createResponse2, "Response should not be null");

    // Step 2: List all stores in the cluster
    ListStoresGrpcRequest listStoresRequest =
        ListStoresGrpcRequest.newBuilder().setClusterName(veniceCluster.getClusterName()).build();

    ListStoresGrpcResponse listStoresResponse = storeBlockingStub.listStores(listStoresRequest);
    assertNotNull(listStoresResponse, "Response should not be null");
    assertEquals(listStoresResponse.getClusterName(), veniceCluster.getClusterName());
    assertTrue(listStoresResponse.getStoreNamesList().contains(storeName1), "Store list should contain " + storeName1);
    assertTrue(listStoresResponse.getStoreNamesList().contains(storeName2), "Store list should contain " + storeName2);

    // Step 3: List stores excluding system stores
    ListStoresGrpcRequest listStoresNoSystemRequest = ListStoresGrpcRequest.newBuilder()
        .setClusterName(veniceCluster.getClusterName())
        .setIncludeSystemStores(false)
        .build();

    ListStoresGrpcResponse listStoresNoSystemResponse = storeBlockingStub.listStores(listStoresNoSystemRequest);
    assertNotNull(listStoresNoSystemResponse, "Response should not be null");
    assertTrue(
        listStoresNoSystemResponse.getStoreNamesList().contains(storeName1),
        "Store list should contain " + storeName1);
    assertTrue(
        listStoresNoSystemResponse.getStoreNamesList().contains(storeName2),
        "Store list should contain " + storeName2);
    // Verify no system stores are in the list
    for (String store: listStoresNoSystemResponse.getStoreNamesList()) {
      assertFalse(
          store.startsWith("venice_system_store") || store.contains("push_status"),
          "Store list should not contain system stores: " + store);
    }
  }

  @Test(timeOut = TIMEOUT_MS)
  public void testGetValueSchemaGrpcEndpoint() {
    String storeName = Utils.getUniqueString("test_get_value_schema_store");
    String valueSchema = "\"string\"";
    String controllerGrpcUrl = veniceCluster.getLeaderVeniceController().getControllerGrpcUrl();
    ManagedChannel channel = Grpc.newChannelBuilder(controllerGrpcUrl, InsecureChannelCredentials.create()).build();
    StoreGrpcServiceGrpc.StoreGrpcServiceBlockingStub storeBlockingStub = StoreGrpcServiceGrpc.newBlockingStub(channel);
    SchemaGrpcServiceGrpc.SchemaGrpcServiceBlockingStub schemaBlockingStub =
        SchemaGrpcServiceGrpc.newBlockingStub(channel);

    ClusterStoreGrpcInfo storeGrpcInfo = ClusterStoreGrpcInfo.newBuilder()
        .setClusterName(veniceCluster.getClusterName())
        .setStoreName(storeName)
        .build();

    // Step 1: Create the store
    CreateStoreGrpcRequest createStoreGrpcRequest = CreateStoreGrpcRequest.newBuilder()
        .setStoreInfo(storeGrpcInfo)
        .setOwner("owner")
        .setKeySchema(DEFAULT_KEY_SCHEMA)
        .setValueSchema(valueSchema)
        .build();
    CreateStoreGrpcResponse createResponse = storeBlockingStub.createStore(createStoreGrpcRequest);
    assertNotNull(createResponse, "Response should not be null");
    assertEquals(createResponse.getStoreInfo().getStoreName(), storeName);

    // Step 2: Get value schema by ID
    GetValueSchemaGrpcRequest getSchemaRequest =
        GetValueSchemaGrpcRequest.newBuilder().setStoreInfo(storeGrpcInfo).setSchemaId(1).build();

    GetValueSchemaGrpcResponse getSchemaResponse = schemaBlockingStub.getValueSchema(getSchemaRequest);
    assertNotNull(getSchemaResponse, "Response should not be null");
    assertEquals(getSchemaResponse.getStoreInfo().getStoreName(), storeName);
    assertEquals(getSchemaResponse.getStoreInfo().getClusterName(), veniceCluster.getClusterName());
    assertEquals(getSchemaResponse.getSchemaId(), 1);
    assertEquals(getSchemaResponse.getSchemaStr(), valueSchema);

    // Step 3: Try to get non-existent schema - should fail
    GetValueSchemaGrpcRequest invalidSchemaRequest =
        GetValueSchemaGrpcRequest.newBuilder().setStoreInfo(storeGrpcInfo).setSchemaId(99).build();

    StatusRuntimeException exception = Assert
        .expectThrows(StatusRuntimeException.class, () -> schemaBlockingStub.getValueSchema(invalidSchemaRequest));
    assertEquals(exception.getStatus().getCode(), io.grpc.Status.Code.INVALID_ARGUMENT);
  }

  @Test(timeOut = TIMEOUT_MS)
  public void testGetRepushInfoGrpcEndpoint() {
    String storeName = Utils.getUniqueString("test_get_repush_info_store");
    String controllerGrpcUrl = veniceCluster.getLeaderVeniceController().getControllerGrpcUrl();
    ManagedChannel channel = Grpc.newChannelBuilder(controllerGrpcUrl, InsecureChannelCredentials.create()).build();
    StoreGrpcServiceGrpc.StoreGrpcServiceBlockingStub storeBlockingStub = StoreGrpcServiceGrpc.newBlockingStub(channel);

    ClusterStoreGrpcInfo storeGrpcInfo = ClusterStoreGrpcInfo.newBuilder()
        .setClusterName(veniceCluster.getClusterName())
        .setStoreName(storeName)
        .build();

    // Step 1: Create the store
    CreateStoreGrpcRequest createStoreGrpcRequest = CreateStoreGrpcRequest.newBuilder()
        .setStoreInfo(storeGrpcInfo)
        .setOwner("owner")
        .setKeySchema(DEFAULT_KEY_SCHEMA)
        .setValueSchema("\"string\"")
        .build();
    CreateStoreGrpcResponse createResponse = storeBlockingStub.createStore(createStoreGrpcRequest);
    assertNotNull(createResponse, "Response should not be null");
    assertEquals(createResponse.getStoreInfo().getStoreName(), storeName);

    // Step 2: Do an empty push to create version 1
    veniceCluster.createVersion(storeName, 0);

    // Step 3: Get repush info using gRPC endpoint
    GetRepushInfoGrpcRequest getRepushInfoRequest =
        GetRepushInfoGrpcRequest.newBuilder().setStoreInfo(storeGrpcInfo).build();
    GetRepushInfoGrpcResponse getRepushInfoResponse = storeBlockingStub.getRepushInfo(getRepushInfoRequest);
    assertNotNull(getRepushInfoResponse, "Response should not be null");
    assertEquals(getRepushInfoResponse.getStoreInfo().getStoreName(), storeName);
    assertEquals(getRepushInfoResponse.getStoreInfo().getClusterName(), veniceCluster.getClusterName());
    assertNotNull(getRepushInfoResponse.getRepushInfo(), "Repush info should not be null");
    assertFalse(
        getRepushInfoResponse.getRepushInfo().getKafkaBrokerUrl().isEmpty(),
        "Kafka broker URL should not be empty");
    assertTrue(getRepushInfoResponse.getRepushInfo().hasVersion(), "Version info should be present");
    assertEquals(getRepushInfoResponse.getRepushInfo().getVersion().getNumber(), 1, "Version number should be 1");
  }

  @Test(timeOut = TIMEOUT_MS)
  public void testGetRepushInfoGrpcEndpointWithFabric() {
    String storeName = Utils.getUniqueString("test_get_repush_info_fabric_store");
    String controllerGrpcUrl = veniceCluster.getLeaderVeniceController().getControllerGrpcUrl();
    ManagedChannel channel = Grpc.newChannelBuilder(controllerGrpcUrl, InsecureChannelCredentials.create()).build();
    StoreGrpcServiceGrpc.StoreGrpcServiceBlockingStub storeBlockingStub = StoreGrpcServiceGrpc.newBlockingStub(channel);

    ClusterStoreGrpcInfo storeGrpcInfo = ClusterStoreGrpcInfo.newBuilder()
        .setClusterName(veniceCluster.getClusterName())
        .setStoreName(storeName)
        .build();

    // Step 1: Create the store
    CreateStoreGrpcRequest createStoreGrpcRequest = CreateStoreGrpcRequest.newBuilder()
        .setStoreInfo(storeGrpcInfo)
        .setOwner("owner")
        .setKeySchema(DEFAULT_KEY_SCHEMA)
        .setValueSchema("\"string\"")
        .build();
    CreateStoreGrpcResponse createResponse = storeBlockingStub.createStore(createStoreGrpcRequest);
    assertNotNull(createResponse, "Response should not be null");

    // Step 2: Do an empty push to create version 1
    veniceCluster.createVersion(storeName, 0);

    // Step 3: Get repush info with fabric parameter
    GetRepushInfoGrpcRequest getRepushInfoRequest =
        GetRepushInfoGrpcRequest.newBuilder().setStoreInfo(storeGrpcInfo).setFabric("test-fabric").build();
    GetRepushInfoGrpcResponse getRepushInfoResponse = storeBlockingStub.getRepushInfo(getRepushInfoRequest);
    assertNotNull(getRepushInfoResponse, "Response should not be null");
    assertNotNull(getRepushInfoResponse.getRepushInfo(), "Repush info should not be null");
    assertFalse(
        getRepushInfoResponse.getRepushInfo().getKafkaBrokerUrl().isEmpty(),
        "Kafka broker URL should not be empty");
  }

  @Test(timeOut = TIMEOUT_MS)
  public void testGetRepushInfoGrpcEndpointForNonExistentStore() {
    String storeName = Utils.getUniqueString("non_existent_store");
    String controllerGrpcUrl = veniceCluster.getLeaderVeniceController().getControllerGrpcUrl();
    ManagedChannel channel = Grpc.newChannelBuilder(controllerGrpcUrl, InsecureChannelCredentials.create()).build();
    StoreGrpcServiceGrpc.StoreGrpcServiceBlockingStub storeBlockingStub = StoreGrpcServiceGrpc.newBlockingStub(channel);

    ClusterStoreGrpcInfo storeGrpcInfo = ClusterStoreGrpcInfo.newBuilder()
        .setClusterName(veniceCluster.getClusterName())
        .setStoreName(storeName)
        .build();

    GetRepushInfoGrpcRequest getRepushInfoRequest =
        GetRepushInfoGrpcRequest.newBuilder().setStoreInfo(storeGrpcInfo).build();

    // Should fail with NOT_FOUND status
    StatusRuntimeException exception =
        Assert.expectThrows(StatusRuntimeException.class, () -> storeBlockingStub.getRepushInfo(getRepushInfoRequest));
    assertEquals(exception.getStatus().getCode(), io.grpc.Status.Code.NOT_FOUND);
  }

  @Test(timeOut = TIMEOUT_MS)
  public void testGetKeySchemaGrpcEndpoint() {
    String storeName = Utils.getUniqueString("test_get_key_schema_store");
    String controllerGrpcUrl = veniceCluster.getLeaderVeniceController().getControllerGrpcUrl();
    ManagedChannel channel = Grpc.newChannelBuilder(controllerGrpcUrl, InsecureChannelCredentials.create()).build();
    StoreGrpcServiceGrpc.StoreGrpcServiceBlockingStub storeBlockingStub = StoreGrpcServiceGrpc.newBlockingStub(channel);
    SchemaGrpcServiceGrpc.SchemaGrpcServiceBlockingStub schemaBlockingStub =
        SchemaGrpcServiceGrpc.newBlockingStub(channel);

    ClusterStoreGrpcInfo storeGrpcInfo = ClusterStoreGrpcInfo.newBuilder()
        .setClusterName(veniceCluster.getClusterName())
        .setStoreName(storeName)
        .build();

    // Step 1: Create the store
    CreateStoreGrpcRequest createStoreGrpcRequest = CreateStoreGrpcRequest.newBuilder()
        .setStoreInfo(storeGrpcInfo)
        .setOwner("owner")
        .setKeySchema(DEFAULT_KEY_SCHEMA)
        .setValueSchema("\"string\"")
        .build();
    CreateStoreGrpcResponse createResponse = storeBlockingStub.createStore(createStoreGrpcRequest);
    assertNotNull(createResponse, "Response should not be null");
    assertEquals(createResponse.getStoreInfo().getStoreName(), storeName);

    // Step 2: Get key schema using gRPC endpoint
    GetKeySchemaGrpcRequest getKeySchemaRequest =
        GetKeySchemaGrpcRequest.newBuilder().setStoreInfo(storeGrpcInfo).build();
    GetKeySchemaGrpcResponse getKeySchemaResponse = schemaBlockingStub.getKeySchema(getKeySchemaRequest);
    assertNotNull(getKeySchemaResponse, "Response should not be null");
    assertEquals(getKeySchemaResponse.getStoreInfo().getStoreName(), storeName);
    assertEquals(getKeySchemaResponse.getStoreInfo().getClusterName(), veniceCluster.getClusterName());
    assertEquals(getKeySchemaResponse.getSchemaStr(), DEFAULT_KEY_SCHEMA);
    assertEquals(getKeySchemaResponse.getSchemaId(), 1);
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
