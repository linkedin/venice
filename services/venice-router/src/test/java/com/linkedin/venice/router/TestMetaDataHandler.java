package com.linkedin.venice.router;

import static com.linkedin.venice.VeniceConstants.TYPE_STORE_STATE;
import static com.linkedin.venice.VeniceConstants.TYPE_STREAM_HYBRID_STORE_QUOTA;
import static com.linkedin.venice.VeniceConstants.TYPE_STREAM_REPROCESSING_HYBRID_STORE_QUOTA;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.LeaderControllerResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixHybridStoreQuotaRepository;
import com.linkedin.venice.helix.HelixReadOnlyStoreConfigRepository;
import com.linkedin.venice.helix.HelixReadOnlyStoreRepository;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.helix.StoreJSONSerializer;
import com.linkedin.venice.helix.SystemStoreJSONSerializer;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.ETLStoreConfigImpl;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.SerializableSystemStore;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.SystemStore;
import com.linkedin.venice.meta.SystemStoreAttributes;
import com.linkedin.venice.meta.SystemStoreAttributesImpl;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.HybridStoreQuotaStatus;
import com.linkedin.venice.routerapi.HybridStoreQuotaStatusResponse;
import com.linkedin.venice.routerapi.ReplicaState;
import com.linkedin.venice.routerapi.ResourceStateResponse;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestMetaDataHandler {
  private static final String ZK_ADDRESS = "localhost:1234";
  private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:1234";
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();
  private final HelixHybridStoreQuotaRepository hybridStoreQuotaRepository =
      Mockito.mock(HelixHybridStoreQuotaRepository.class);

  public FullHttpResponse passRequestToMetadataHandler(
      String requestUri,
      RoutingDataRepository routingDataRepository,
      ReadOnlySchemaRepository schemaRepo) throws IOException {
    return passRequestToMetadataHandler(
        requestUri,
        routingDataRepository,
        schemaRepo,
        Mockito.mock(HelixReadOnlyStoreConfigRepository.class),
        Collections.emptyMap(),
        null);
  }

  public FullHttpResponse passRequestToMetadataHandler(
      String requestUri,
      RoutingDataRepository routingDataRepository,
      ReadOnlySchemaRepository schemaRepo,
      HelixReadOnlyStoreConfigRepository storeConfigRepository,
      Map<String, String> clusterToD2ServiceMap) throws IOException {
    Store store = TestUtils.createTestStore("testStore", "test", System.currentTimeMillis());
    store.setCurrentVersion(1);
    HelixReadOnlyStoreRepository helixReadOnlyStoreRepository = Mockito.mock(HelixReadOnlyStoreRepository.class);
    Mockito.doReturn(store).when(helixReadOnlyStoreRepository).getStore(Mockito.anyString());
    return passRequestToMetadataHandler(
        requestUri,
        routingDataRepository,
        schemaRepo,
        storeConfigRepository,
        clusterToD2ServiceMap,
        helixReadOnlyStoreRepository);
  }

  public FullHttpResponse passRequestToMetadataHandler(
      String requestUri,
      RoutingDataRepository routing,
      ReadOnlySchemaRepository schemaRepo,
      HelixReadOnlyStoreConfigRepository storeConfigRepository,
      Map<String, String> clusterToD2ServiceMap,
      HelixReadOnlyStoreRepository helixReadOnlyStoreRepository) throws IOException {
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    FullHttpRequest httpRequest = Mockito.mock(FullHttpRequest.class);
    Mockito.doReturn(EmptyHttpHeaders.INSTANCE).when(httpRequest).headers();
    Mockito.doReturn(requestUri).when(httpRequest).uri();

    ReadOnlySchemaRepository schemaRepoToUse;
    if (schemaRepo == null) {
      schemaRepoToUse = Mockito.mock(ReadOnlySchemaRepository.class);
      Mockito.doReturn(null).when(schemaRepoToUse).getKeySchema(Mockito.anyString());
      Mockito.doReturn(null).when(schemaRepoToUse).getValueSchema(Mockito.anyString(), Mockito.anyInt());
      Mockito.doReturn(Collections.EMPTY_LIST).when(schemaRepoToUse).getValueSchemas(Mockito.anyString());
    } else {
      schemaRepoToUse = schemaRepo;
    }

    MetaDataHandler handler = new MetaDataHandler(
        routing,
        schemaRepoToUse,
        storeConfigRepository,
        clusterToD2ServiceMap,
        helixReadOnlyStoreRepository,
        Optional.of(hybridStoreQuotaRepository),
        "test-cluster",
        ZK_ADDRESS,
        KAFKA_BOOTSTRAP_SERVERS);
    handler.channelRead0(ctx, httpRequest);
    ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
    Mockito.verify(ctx).writeAndFlush(captor.capture());

    FullHttpResponse response = (FullHttpResponse) captor.getValue();
    return response;
  }

  @Test
  public void testControllerLookup() throws IOException {
    // Mock RoutingDataRepository
    RoutingDataRepository routingRepo = Mockito.mock(RoutingDataRepository.class);
    String leaderControllerHost = "myControllerHost";
    int leaderControllerPort = 1234;
    Instance leaderControllerInstance = new Instance("1", leaderControllerHost, leaderControllerPort);
    Mockito.doReturn(leaderControllerInstance).when(routingRepo).getLeaderController();

    FullHttpResponse response =
        passRequestToMetadataHandler("http://myRouterHost:4567/leader_controller", routingRepo, null);

    Assert.assertEquals(response.status().code(), 200);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), "application/json");
    LeaderControllerResponse controllerResponse =
        OBJECT_MAPPER.readValue(response.content().array(), LeaderControllerResponse.class);
    Assert.assertEquals(controllerResponse.getUrl(), "http://" + leaderControllerHost + ":" + leaderControllerPort);
  }

  // The deprecated non inclusive URL must also continue to work.
  @DataProvider(name = "controllerUrlProvider")
  public static Object[][] dataProvider() {
    // go/inclusivecode deprecated (alias="leader_controller")
    return new Object[][] { { "master_controller" }, { "leader_controller" } };
  }

  @Test(dataProvider = "controllerUrlProvider")
  public void testControllerLookupLegacy(String controllerUrl) throws IOException {
    // Mock RoutingDataRepository
    RoutingDataRepository routingRepo = Mockito.mock(RoutingDataRepository.class);
    String leaderControllerHost = "myControllerHost";
    int leaderControllerPort = 1234;
    Instance leaderControllerInstance = new Instance("1", leaderControllerHost, leaderControllerPort);
    Mockito.doReturn(leaderControllerInstance).when(routingRepo).getLeaderController();
    FullHttpResponse response =
        passRequestToMetadataHandler("http://myRouterHost:4567/" + controllerUrl, routingRepo, null);

    Assert.assertEquals(response.status().code(), 200);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), "application/json");
    LeaderControllerResponse controllerResponse =
        OBJECT_MAPPER.readValue(response.content().array(), LeaderControllerResponse.class);
    Assert.assertEquals(controllerResponse.getUrl(), "http://" + leaderControllerHost + ":" + leaderControllerPort);
  }

  @Test
  public void testKeySchemaLookup() throws IOException {
    String storeName = "test_store";
    String keySchemaStr = "\"string\"";
    String clusterName = "test-cluster";
    int keySchemaId = 1;
    // Mock ReadOnlySchemaRepository
    ReadOnlySchemaRepository schemaRepo = Mockito.mock(ReadOnlySchemaRepository.class);
    SchemaEntry keySchemaEntry = new SchemaEntry(keySchemaId, keySchemaStr);
    Mockito.doReturn(keySchemaEntry).when(schemaRepo).getKeySchema(storeName);

    FullHttpResponse response =
        passRequestToMetadataHandler("http://myRouterHost:4567/key_schema/" + storeName, null, schemaRepo);

    Assert.assertEquals(response.status().code(), 200);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), "application/json");
    SchemaResponse schemaResponse = OBJECT_MAPPER.readValue(response.content().array(), SchemaResponse.class);
    Assert.assertEquals(schemaResponse.getId(), keySchemaId);
    Assert.assertEquals(schemaResponse.getSchemaStr(), keySchemaStr);
    Assert.assertEquals(schemaResponse.getName(), storeName);
    Assert.assertEquals(schemaResponse.getCluster(), clusterName);
    Assert.assertFalse(schemaResponse.isError());
  }

  @Test
  public void testKeySchemaLookupWithKeySchemaDoesntExist() throws IOException {
    String storeName = "test_store";

    FullHttpResponse response =
        passRequestToMetadataHandler("http://myRouterHost:4567/key_schema/" + storeName, null, null);

    Assert.assertEquals(response.status().code(), 404);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), "text/plain");
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*Resource name required.*")
  public void testInvalidKeySchemaPath() throws IOException {
    passRequestToMetadataHandler("http://myRouterHost:4567/key_schema/", null, null);
  }

  @Test
  public void testSingleValueSchemaLookup() throws IOException {
    String storeName = "test_store";
    String valueSchemaStr = "\"string\"";
    String clusterName = "test-cluster";
    int valueSchemaId = 1;
    // Mock ReadOnlySchemaRepository
    ReadOnlySchemaRepository schemaRepo = Mockito.mock(ReadOnlySchemaRepository.class);
    SchemaEntry valueSchemaEntry = new SchemaEntry(valueSchemaId, valueSchemaStr);
    Mockito.doReturn(valueSchemaEntry).when(schemaRepo).getValueSchema(storeName, valueSchemaId);

    FullHttpResponse response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/value_schema/" + storeName + "/" + valueSchemaId,
        null,
        schemaRepo);

    Assert.assertEquals(response.status().code(), 200);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), "application/json");
    SchemaResponse schemaResponse = OBJECT_MAPPER.readValue(response.content().array(), SchemaResponse.class);
    Assert.assertEquals(schemaResponse.getId(), 1);
    Assert.assertEquals(schemaResponse.getSchemaStr(), valueSchemaStr);
    Assert.assertEquals(schemaResponse.getName(), storeName);
    Assert.assertEquals(schemaResponse.getCluster(), clusterName);
    Assert.assertFalse(schemaResponse.isError());
  }

  @Test
  public void testSingleValueSchemaLookupWithValueSchemaDoesntExist() throws IOException {
    String storeName = "test_store";
    int valueSchemaId = 1;

    FullHttpResponse response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/value_schema/" + storeName + "/" + valueSchemaId,
        null,
        null);

    Assert.assertEquals(response.status().code(), 404);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), "text/plain");
  }

  @Test
  public void testAllValueSchemaLookup() throws IOException {
    String storeName = "test_store";
    String valueSchemaStr1 = "\"string\"";
    String valueSchemaStr2 = "\"long\"";
    String clusterName = "test-cluster";
    int valueSchemaId1 = 1;
    int valueSchemaId2 = 2;
    // Mock ReadOnlySchemaRepository
    ReadOnlySchemaRepository schemaRepo = Mockito.mock(ReadOnlySchemaRepository.class);
    SchemaEntry valueSchemaEntry1 = new SchemaEntry(valueSchemaId1, valueSchemaStr1);
    SchemaEntry valueSchemaEntry2 = new SchemaEntry(valueSchemaId2, valueSchemaStr2);
    Mockito.doReturn(Arrays.asList(valueSchemaEntry1, valueSchemaEntry2)).when(schemaRepo).getValueSchemas(storeName);

    FullHttpResponse response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/value_schema/" + storeName,
        null,
        schemaRepo,
        Mockito.mock(HelixReadOnlyStoreConfigRepository.class),
        Collections.emptyMap());

    Assert.assertEquals(response.status().code(), 200);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), "application/json");
    MultiSchemaResponse multiSchemaResponse =
        OBJECT_MAPPER.readValue(response.content().array(), MultiSchemaResponse.class);

    Assert.assertEquals(multiSchemaResponse.getName(), storeName);
    Assert.assertEquals(multiSchemaResponse.getCluster(), clusterName);
    Assert.assertFalse(multiSchemaResponse.isError());
    MultiSchemaResponse.Schema[] schemas = multiSchemaResponse.getSchemas();
    Assert.assertEquals(schemas.length, 2);
    Assert.assertEquals(schemas[0].getId(), valueSchemaId1);
    Assert.assertEquals(schemas[0].getSchemaStr(), valueSchemaStr1);
    Assert.assertEquals(schemas[1].getId(), valueSchemaId2);
    Assert.assertEquals(schemas[1].getSchemaStr(), valueSchemaStr2);
  }

  @Test
  public void testAllValueSchemaLookupWithNoValueSchema() throws IOException {
    String storeName = "test_store";
    String clusterName = "test-cluster";

    FullHttpResponse response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/value_schema/" + storeName,
        null,
        null,
        Mockito.mock(HelixReadOnlyStoreConfigRepository.class),
        Collections.emptyMap());

    Assert.assertEquals(response.status().code(), 200);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), "application/json");
    MultiSchemaResponse multiSchemaResponse =
        OBJECT_MAPPER.readValue(response.content().array(), MultiSchemaResponse.class);

    Assert.assertEquals(multiSchemaResponse.getName(), storeName);
    Assert.assertEquals(multiSchemaResponse.getCluster(), clusterName);
    Assert.assertFalse(multiSchemaResponse.isError());
    MultiSchemaResponse.Schema[] schemas = multiSchemaResponse.getSchemas();
    Assert.assertEquals(schemas.length, 0);
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*Resource name required.*")
  public void testInvalidValueSchemaPath() throws IOException {
    passRequestToMetadataHandler("http://myRouterHost:4567/value_schema/", null, null);
  }

  @Test
  public void testUpdateSchemaLookup() throws IOException {
    String storeName = "test_store";
    String valueSchemaStr1 = "\"string\"";
    String updateSchemaStr1 = "\"long\"";
    String updateSchemaStr2 = "\"string\"";
    String clusterName = "test-cluster";
    int valueSchemaId1 = 1;
    // Mock ReadOnlySchemaRepository
    ReadOnlySchemaRepository schemaRepo = Mockito.mock(ReadOnlySchemaRepository.class);
    SchemaEntry valueSchemaEntry1 = new SchemaEntry(valueSchemaId1, valueSchemaStr1);
    Mockito.doReturn(Collections.singletonList(valueSchemaEntry1)).when(schemaRepo).getValueSchemas(storeName);
    SchemaEntry updateSchemaEntry1 = new DerivedSchemaEntry(valueSchemaId1, 1, updateSchemaStr1);
    SchemaEntry updateSchemaEntry2 = new DerivedSchemaEntry(valueSchemaId1, 2, updateSchemaStr2);
    Mockito.doReturn(Arrays.asList(updateSchemaEntry1, updateSchemaEntry2))
        .when(schemaRepo)
        .getDerivedSchemas(storeName);

    FullHttpResponse response =
        passRequestToMetadataHandler("http://myRouterHost:4567/update_schema/" + storeName + "/1", null, schemaRepo);

    Assert.assertEquals(response.status().code(), 200);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), "application/json");
    SchemaResponse schemaResponse = OBJECT_MAPPER.readValue(response.content().array(), SchemaResponse.class);

    Assert.assertEquals(schemaResponse.getName(), storeName);
    Assert.assertEquals(schemaResponse.getCluster(), clusterName);
    Assert.assertFalse(schemaResponse.isError());
    Assert.assertEquals(schemaResponse.getId(), valueSchemaId1);
    Assert.assertEquals(schemaResponse.getSchemaStr(), updateSchemaStr2);
    Assert.assertEquals(schemaResponse.getDerivedSchemaId(), 2);
  }

  @Test
  public void testD2ServiceLookup() throws IOException {
    String storeName = "test-store";
    String clusterName = "test-cluster";
    String d2Service = "test-d2-service";
    HelixReadOnlyStoreConfigRepository storeConfigRepository = Mockito.mock(HelixReadOnlyStoreConfigRepository.class);
    StoreConfig storeConfig = new StoreConfig(storeName);
    storeConfig.setCluster(clusterName);
    Mockito.doReturn(Optional.of(storeConfig)).when(storeConfigRepository).getStoreConfig(storeName);
    Map<String, String> clusterToD2Map = new HashMap<>();
    clusterToD2Map.put(clusterName, d2Service);
    FullHttpResponse response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/discover_cluster/" + storeName,
        null,
        null,
        storeConfigRepository,
        clusterToD2Map,
        null);

    Assert.assertEquals(response.status().code(), 200);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), "application/json");
    D2ServiceDiscoveryResponse d2ServiceResponse =
        OBJECT_MAPPER.readValue(response.content().array(), D2ServiceDiscoveryResponse.class);
    Assert.assertEquals(d2ServiceResponse.getCluster(), clusterName);
    Assert.assertEquals(d2ServiceResponse.getD2Service(), d2Service);
    Assert.assertEquals(d2ServiceResponse.getName(), storeName);
    Assert.assertFalse(d2ServiceResponse.isError());
  }

  @Test
  public void testD2ServiceLoopNoClusterFound() throws IOException {
    String storeName = "test-store";
    HelixReadOnlyStoreConfigRepository storeConfigRepository = Mockito.mock(HelixReadOnlyStoreConfigRepository.class);
    Mockito.doReturn(Optional.empty()).when(storeConfigRepository).getStoreConfig(storeName);
    FullHttpResponse response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/discover_cluster/" + storeName,
        null,
        null,
        storeConfigRepository,
        Collections.emptyMap(),
        null);

    Assert.assertEquals(response.status().code(), 404);
  }

  @Test
  public void testResourceStateLookup() throws IOException {
    String resourceName = "test-store_v1";
    RoutingDataRepository routingDataRepository = Mockito.mock(RoutingDataRepository.class);
    HelixReadOnlyStoreConfigRepository storeConfigRepository = Mockito.mock(HelixReadOnlyStoreConfigRepository.class);
    Mockito.doReturn(2).when(routingDataRepository).getNumberOfPartitions(resourceName);
    List<ReplicaState> replicaStates0 = new ArrayList<>();
    replicaStates0
        .add(new ReplicaState(0, "test-host_0", HelixState.LEADER_STATE, ExecutionStatus.COMPLETED.toString(), true));
    List<ReplicaState> replicaStates1 = new ArrayList<>();
    replicaStates1
        .add(new ReplicaState(1, "test-host_1", HelixState.LEADER_STATE, ExecutionStatus.COMPLETED.toString(), true));
    Mockito.doReturn(replicaStates0).when(routingDataRepository).getReplicaStates(resourceName, 0);
    Mockito.doReturn(replicaStates1).when(routingDataRepository).getReplicaStates(resourceName, 1);

    FullHttpResponse response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/resource_state/" + resourceName,
        routingDataRepository,
        null,
        storeConfigRepository,
        Collections.emptyMap());
    Assert.assertEquals(response.status().code(), 200);
    ResourceStateResponse resourceStateResponse =
        OBJECT_MAPPER.readValue(response.content().array(), ResourceStateResponse.class);
    Assert.assertTrue(resourceStateResponse.isReadyToServe());
    // Add a not ready to serve replica in partition 0 which should put the version to not ready to serve.
    replicaStates0
        .add(new ReplicaState(0, "test-host_2", HelixState.STANDBY_STATE, ExecutionStatus.STARTED.toString(), false));
    replicaStates1
        .add(new ReplicaState(1, "test-host_3", HelixState.STANDBY_STATE, ExecutionStatus.COMPLETED.toString(), true));
    response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/resource_state/" + resourceName,
        routingDataRepository,
        null,
        storeConfigRepository,
        Collections.emptyMap());
    Assert.assertEquals(response.status().code(), 200);
    resourceStateResponse = OBJECT_MAPPER.readValue(response.content().array(), ResourceStateResponse.class);
    Assert.assertFalse(resourceStateResponse.isReadyToServe());
    // Add one more ready to serve replica in each partition which should put the version to ready to serve.
    replicaStates0
        .add(new ReplicaState(0, "test-host_4", HelixState.STANDBY_STATE, ExecutionStatus.COMPLETED.toString(), true));
    replicaStates1
        .add(new ReplicaState(1, "test-host_5", HelixState.STANDBY_STATE, ExecutionStatus.COMPLETED.toString(), true));
    response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/resource_state/" + resourceName,
        routingDataRepository,
        null,
        storeConfigRepository,
        Collections.emptyMap());
    Assert.assertEquals(response.status().code(), 200);
    resourceStateResponse = OBJECT_MAPPER.readValue(response.content().array(), ResourceStateResponse.class);
    Assert.assertTrue(resourceStateResponse.isReadyToServe());
  }

  @Test
  public void testResourceStateLookupOfSystemStores() throws IOException {
    String storeName = "regular-test-store";
    HelixReadOnlyStoreConfigRepository storeConfigRepository = Mockito.mock(HelixReadOnlyStoreConfigRepository.class);
    RoutingDataRepository routingDataRepository = Mockito.mock(RoutingDataRepository.class);
    List<String> systemStoreResources = new ArrayList<>();
    List<ReplicaState> replicaStates = new ArrayList<>();
    replicaStates
        .add(new ReplicaState(0, "test-host_0", HelixState.ONLINE_STATE, ExecutionStatus.COMPLETED.toString(), true));
    for (VeniceSystemStoreType systemStoreType: VeniceSystemStoreType.values()) {
      String resourceName = Version.composeKafkaTopic(systemStoreType.getSystemStoreName(storeName), 1);
      systemStoreResources.add(resourceName);
      Mockito.doReturn(1).when(routingDataRepository).getNumberOfPartitions(resourceName);
      Mockito.doReturn(replicaStates).when(routingDataRepository).getReplicaStates(resourceName, 0);
    }
    for (String systemStoreResource: systemStoreResources) {
      FullHttpResponse response = passRequestToMetadataHandler(
          "http://myRouterHost:4567/resource_state/" + systemStoreResource,
          routingDataRepository,
          null,
          storeConfigRepository,
          Collections.emptyMap());
      Assert.assertEquals(response.status().code(), 200);
      ResourceStateResponse resourceStateResponse =
          OBJECT_MAPPER.readValue(response.content().array(), ResourceStateResponse.class);
      Assert.assertTrue(resourceStateResponse.isReadyToServe());
      List<ReplicaState> responseReplicaStates = resourceStateResponse.getReplicaStates();
      for (int i = 0; i < replicaStates.size(); i++) {
        Assert.assertEquals(responseReplicaStates.get(i).toString(), replicaStates.get(i).toString());
      }
    }
  }

  @Test
  public void testResourceStateLookupWithErrors() throws IOException {
    String resourceName = "test-store_v1";
    HelixReadOnlyStoreRepository helixReadOnlyStoreRepository = Mockito.mock(HelixReadOnlyStoreRepository.class);
    RoutingDataRepository routingDataRepository = Mockito.mock(RoutingDataRepository.class);
    HelixReadOnlyStoreConfigRepository storeConfigRepository = Mockito.mock(HelixReadOnlyStoreConfigRepository.class);
    Mockito.doReturn(Optional.empty())
        .when(storeConfigRepository)
        .getStoreConfig(Version.parseStoreFromKafkaTopicName(resourceName));
    Mockito.doReturn(2).when(routingDataRepository).getNumberOfPartitions(resourceName);
    FullHttpResponse response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/resource_state/" + resourceName,
        routingDataRepository,
        null,
        storeConfigRepository,
        Collections.emptyMap(),
        helixReadOnlyStoreRepository);
    Assert.assertEquals(response.status().code(), 404);
    Mockito.doReturn(Optional.of(new StoreConfig(Version.parseStoreFromKafkaTopicName(resourceName))))
        .when(storeConfigRepository)
        .getStoreConfig(Version.parseStoreFromKafkaTopicName(resourceName));
    List<ReplicaState> replicaStates0 = new ArrayList<>();
    replicaStates0
        .add(new ReplicaState(0, "test-host_0", HelixState.LEADER_STATE, ExecutionStatus.COMPLETED.toString(), true));
    Mockito.doReturn(replicaStates0).when(routingDataRepository).getReplicaStates(resourceName, 0);
    Mockito.doReturn(Collections.emptyList()).when(routingDataRepository).getReplicaStates(resourceName, 1);
    response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/resource_state/" + resourceName,
        routingDataRepository,
        null,
        storeConfigRepository,
        Collections.emptyMap());
    Assert.assertEquals(response.status().code(), 200);
    ResourceStateResponse resourceStateResponse =
        OBJECT_MAPPER.readValue(response.content().array(), ResourceStateResponse.class);
    Assert.assertTrue(resourceStateResponse.isError());
    Assert.assertEquals(resourceStateResponse.getUnretrievablePartitions().iterator().next().intValue(), 1);
  }

  @Test
  public void testHybridQuotaInRouter() throws IOException {
    String resourceName = "test-store_v1";
    String storeName = Version.parseStoreFromKafkaTopicName(resourceName);
    RoutingDataRepository routingDataRepository = Mockito.mock(RoutingDataRepository.class);
    HelixReadOnlyStoreConfigRepository storeConfigRepository = Mockito.mock(HelixReadOnlyStoreConfigRepository.class);
    Mockito.doReturn(Optional.of(new StoreConfig(storeName))).when(storeConfigRepository).getStoreConfig(storeName);
    Mockito.doReturn(2).when(routingDataRepository).getNumberOfPartitions(resourceName);

    // Router returns QUOTA_NOT_VIOLATED state
    Mockito.doReturn(HybridStoreQuotaStatus.QUOTA_NOT_VIOLATED)
        .when(hybridStoreQuotaRepository)
        .getHybridStoreQuotaStatus(resourceName);
    FullHttpResponse response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/" + TYPE_STREAM_HYBRID_STORE_QUOTA + "/" + storeName,
        routingDataRepository,
        null,
        storeConfigRepository,
        Collections.emptyMap());
    Assert.assertEquals(response.status().code(), 200);
    HybridStoreQuotaStatusResponse pushStatusResponse =
        OBJECT_MAPPER.readValue(response.content().array(), HybridStoreQuotaStatusResponse.class);
    Assert.assertEquals(pushStatusResponse.getQuotaStatus(), HybridStoreQuotaStatus.QUOTA_NOT_VIOLATED);

    // Router returns QUOTA_VIOLATED state
    Mockito.doReturn(HybridStoreQuotaStatus.QUOTA_VIOLATED)
        .when(hybridStoreQuotaRepository)
        .getHybridStoreQuotaStatus(resourceName);
    response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/" + TYPE_STREAM_HYBRID_STORE_QUOTA + "/" + storeName,
        routingDataRepository,
        null,
        storeConfigRepository,
        Collections.emptyMap());
    Assert.assertEquals(response.status().code(), 200);
    pushStatusResponse = OBJECT_MAPPER.readValue(response.content().array(), HybridStoreQuotaStatusResponse.class);
    Assert.assertEquals(pushStatusResponse.getQuotaStatus(), HybridStoreQuotaStatus.QUOTA_VIOLATED);
  }

  @Test
  public void testStreamReprocessingHybridQuotaInRouter() throws IOException {
    String resourceName = "test-store_v3";
    String storeName = Version.parseStoreFromKafkaTopicName(resourceName);
    RoutingDataRepository routingDataRepository = Mockito.mock(RoutingDataRepository.class);
    HelixReadOnlyStoreConfigRepository storeConfigRepository = Mockito.mock(HelixReadOnlyStoreConfigRepository.class);
    Mockito.doReturn(Optional.of(new StoreConfig(storeName))).when(storeConfigRepository).getStoreConfig(storeName);
    Mockito.doReturn(2).when(routingDataRepository).getNumberOfPartitions(resourceName);

    // Router returns QUOTA_NOT_VIOLATED state
    Mockito.doReturn(HybridStoreQuotaStatus.QUOTA_NOT_VIOLATED)
        .when(hybridStoreQuotaRepository)
        .getHybridStoreQuotaStatus(resourceName);
    FullHttpResponse response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/" + TYPE_STREAM_REPROCESSING_HYBRID_STORE_QUOTA + "/" + resourceName,
        routingDataRepository,
        null,
        storeConfigRepository,
        Collections.emptyMap());
    System.out.println(response);
    Assert.assertEquals(response.status().code(), 200);
    HybridStoreQuotaStatusResponse pushStatusResponse =
        OBJECT_MAPPER.readValue(response.content().array(), HybridStoreQuotaStatusResponse.class);
    Assert.assertEquals(pushStatusResponse.getQuotaStatus(), HybridStoreQuotaStatus.QUOTA_NOT_VIOLATED);

    // Router returns QUOTA_VIOLATED state
    Mockito.doReturn(HybridStoreQuotaStatus.QUOTA_VIOLATED)
        .when(hybridStoreQuotaRepository)
        .getHybridStoreQuotaStatus(resourceName);
    response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/" + TYPE_STREAM_REPROCESSING_HYBRID_STORE_QUOTA + "/" + resourceName,
        routingDataRepository,
        null,
        storeConfigRepository,
        Collections.emptyMap());
    Assert.assertEquals(response.status().code(), 200);
    pushStatusResponse = OBJECT_MAPPER.readValue(response.content().array(), HybridStoreQuotaStatusResponse.class);
    Assert.assertEquals(pushStatusResponse.getQuotaStatus(), HybridStoreQuotaStatus.QUOTA_VIOLATED);
  }

  @Test
  public void testStorageRequest() throws IOException {
    String storeName = "test_store";
    String clusterName = "test-cluster";

    // Mock Request
    FullHttpRequest httpRequest = Mockito.mock(FullHttpRequest.class);
    Mockito.doReturn("http://myRouterHost:4567/storage/" + storeName + "/abc").when(httpRequest).uri();

    // Mock ChannelHandlerContext
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    HelixReadOnlyStoreRepository helixReadOnlyStoreRepository = Mockito.mock(HelixReadOnlyStoreRepository.class);

    MetaDataHandler handler = new MetaDataHandler(
        null,
        null,
        null,
        Collections.emptyMap(),
        helixReadOnlyStoreRepository,
        Optional.of(hybridStoreQuotaRepository),
        clusterName,
        ZK_ADDRESS,
        KAFKA_BOOTSTRAP_SERVERS);
    handler.channelRead0(ctx, httpRequest);
    // '/storage' request should be handled by upstream, instead of current MetaDataHandler
    Mockito.verify(ctx, Mockito.times(1)).fireChannelRead(Mockito.any());
  }

  @Test
  public void testStoreStateLookup() throws IOException {
    String storeName = "testStore";
    String metaSystemStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName);
    HelixReadOnlyStoreRepository mockStoreRepository = Mockito.mock(HelixReadOnlyStoreRepository.class);
    Store testStore = TestUtils.createTestStore(storeName, "test", System.currentTimeMillis());
    String pushId = "test-push-job-id";
    testStore.addVersion(new VersionImpl(storeName, 1, pushId));
    testStore.setCurrentVersion(1);
    testStore.setEtlStoreConfig(new ETLStoreConfigImpl());
    SystemStoreAttributes systemStoreAttributes = new SystemStoreAttributesImpl();
    systemStoreAttributes.setCurrentVersion(2);
    List<Version> versions = new ArrayList<>();
    versions.add(new VersionImpl(metaSystemStoreName, 2, pushId));
    systemStoreAttributes.setVersions(versions);
    testStore.putSystemStore(VeniceSystemStoreType.META_STORE, systemStoreAttributes);
    Store zkSharedStore = TestUtils.createTestStore(
        AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE.getSystemStoreName(),
        "test",
        System.currentTimeMillis());
    HybridStoreConfig hybridStoreConfig = new HybridStoreConfigImpl(
        Time.SECONDS_PER_DAY,
        1,
        TimeUnit.MINUTES.toSeconds(1),
        DataReplicationPolicy.NON_AGGREGATE,
        BufferReplayPolicy.REWIND_FROM_EOP);
    zkSharedStore.setHybridStoreConfig(hybridStoreConfig);
    SystemStore systemStore = new SystemStore(zkSharedStore, VeniceSystemStoreType.META_STORE, testStore);
    Mockito.doReturn(testStore).when(mockStoreRepository).getStore(storeName);
    Mockito.doReturn(systemStore).when(mockStoreRepository).getStore(metaSystemStoreName);

    FullHttpResponse response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/" + TYPE_STORE_STATE + "/" + storeName,
        null,
        null,
        null,
        Collections.emptyMap(),
        mockStoreRepository);
    Assert.assertEquals(response.status().code(), 200);
    StoreJSONSerializer storeSerializer = new StoreJSONSerializer();
    Store store = storeSerializer.deserialize(response.content().array(), null);
    Assert.assertEquals(store.getCurrentVersion(), 1);
    Assert.assertEquals(store.getVersion(1).get().getPushJobId(), pushId);

    response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/" + TYPE_STORE_STATE + "/" + metaSystemStoreName,
        null,
        null,
        null,
        Collections.emptyMap(),
        mockStoreRepository);
    Assert.assertEquals(response.status().code(), 200);
    SystemStoreJSONSerializer systemStoreSerializer = new SystemStoreJSONSerializer();
    SerializableSystemStore serializableSystemStore =
        systemStoreSerializer.deserialize(response.content().array(), null);
    Store metaSystemStore = new SystemStore(
        serializableSystemStore.getZkSharedStore(),
        serializableSystemStore.getSystemStoreType(),
        serializableSystemStore.getVeniceStore());
    Assert.assertEquals(metaSystemStore.getCurrentVersion(), 2);
    Assert.assertEquals(metaSystemStore.getVersion(2).get().getPushJobId(), pushId);
    Assert.assertEquals(metaSystemStore.getHybridStoreConfig(), hybridStoreConfig);

    FullHttpResponse notFoundResponse = passRequestToMetadataHandler(
        "http://myRouterHost:4567/" + TYPE_STORE_STATE + "/" + "notFound",
        null,
        null,
        null,
        Collections.emptyMap(),
        mockStoreRepository);
    Assert.assertEquals(notFoundResponse.status().code(), 404);
  }
}
