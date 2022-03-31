package com.linkedin.venice.router;

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
import com.linkedin.venice.meta.OnlineInstanceFinderDelegator;
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
import com.linkedin.venice.pushmonitor.PartitionStatusOnlineInstanceFinder;
import com.linkedin.venice.routerapi.HybridStoreQuotaStatusResponse;
import com.linkedin.venice.routerapi.PushStatusResponse;
import com.linkedin.venice.routerapi.ReplicaState;
import com.linkedin.venice.routerapi.ResourceStateResponse;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
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
import org.codehaus.jackson.map.ObjectMapper;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.linkedin.venice.VeniceConstants.*;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.*;

public class TestMetaDataHandler {
  private static final String ZK_ADDRESS = "localhost:1234";
  private static final String KAFKA_ZK_ADDRESS = "localhost:1234";
  private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:1234";
  private static ObjectMapper mapper = new ObjectMapper();
  private HelixHybridStoreQuotaRepository hybridStoreQuotaRepository = Mockito.mock(HelixHybridStoreQuotaRepository.class);
  public FullHttpResponse passRequestToMetadataHandler(String requestUri, RoutingDataRepository routing, ReadOnlySchemaRepository schemaRepo)
      throws IOException {
    return passRequestToMetadataHandler(requestUri, routing, schemaRepo,
        Mockito.mock(HelixReadOnlyStoreConfigRepository.class), Collections.emptyMap(), null);
  }

  public FullHttpResponse passRequestToMetadataHandler(String requestUri, RoutingDataRepository routing,
      ReadOnlySchemaRepository schemaRepo, HelixReadOnlyStoreConfigRepository storeConfigRepository,
      Map<String,String> clusterToD2ServiceMap, OnlineInstanceFinderDelegator onlineInstanceFinder)
      throws IOException {
    Store store = TestUtils.createTestStore("testStore", "test", System.currentTimeMillis());
    store.setCurrentVersion(1);
    HelixReadOnlyStoreRepository helixReadOnlyStoreRepository = Mockito.mock(HelixReadOnlyStoreRepository.class);
    Mockito.doReturn(store).when(helixReadOnlyStoreRepository).getStore(Mockito.anyString());
    return passRequestToMetadataHandler(requestUri, routing, schemaRepo, storeConfigRepository, clusterToD2ServiceMap,
        onlineInstanceFinder, helixReadOnlyStoreRepository);
  }

  public FullHttpResponse passRequestToMetadataHandler(String requestUri, RoutingDataRepository routing,
      ReadOnlySchemaRepository schemaRepo, HelixReadOnlyStoreConfigRepository storeConfigRepository,
      Map<String,String> clusterToD2ServiceMap, OnlineInstanceFinderDelegator onlineInstanceFinder,
      HelixReadOnlyStoreRepository helixReadOnlyStoreRepository)
      throws IOException {
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    FullHttpRequest httpRequest = Mockito.mock(FullHttpRequest.class);
    Mockito.doReturn(EmptyHttpHeaders.INSTANCE).when(httpRequest).headers();
    Mockito.doReturn(requestUri).when(httpRequest).uri();

    ReadOnlySchemaRepository schemaRepoToUse = null;
    if (schemaRepo == null){
      schemaRepoToUse = Mockito.mock(ReadOnlySchemaRepository.class);
      Mockito.doReturn(null).when(schemaRepoToUse).getKeySchema(Mockito.anyString());
      Mockito.doReturn(null).when(schemaRepoToUse).getValueSchema(Mockito.anyString(), Mockito.anyInt());
      Mockito.doReturn(Collections.EMPTY_LIST).when(schemaRepoToUse).getValueSchemas(Mockito.anyString());
    } else {
      schemaRepoToUse = schemaRepo;
    }

    MetaDataHandler handler = new MetaDataHandler(routing, schemaRepoToUse,
        storeConfigRepository , clusterToD2ServiceMap, onlineInstanceFinder, helixReadOnlyStoreRepository,
        Optional.of(hybridStoreQuotaRepository), "test-cluster", ZK_ADDRESS, KAFKA_ZK_ADDRESS, KAFKA_BOOTSTRAP_SERVERS);
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

    FullHttpResponse response = passRequestToMetadataHandler("http://myRouterHost:4567/leader_controller", routingRepo, null);

    Assert.assertEquals(response.status().code(), 200);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), "application/json");
    LeaderControllerResponse controllerResponse = mapper.readValue(response.content().array(),
        LeaderControllerResponse.class);
    Assert.assertEquals(controllerResponse.getUrl(), "http://" + leaderControllerHost + ":" + leaderControllerPort);
  }

  // The deprecated non inclusive URL must also continue to work.
  @DataProvider(name = "controllerUrlProvider")
  private static Object[][] dataProvider() {
    // go/inclusivecode deprecated (alias="leader_controller")
    return new Object[][] {{"/master_controller"},
        {"/leader_controller"}};
  }

  @Test(dataProvider = "controllerUrlProvider")
  public void testControllerLookupLegacy(String controllerUrl) throws IOException {
    // Mock RoutingDataRepository
    RoutingDataRepository routingRepo = Mockito.mock(RoutingDataRepository.class);
    String leaderControllerHost = "myControllerHost";
    int leaderControllerPort = 1234;
    Instance leaderControllerInstance = new Instance("1", leaderControllerHost, leaderControllerPort);
    Mockito.doReturn(leaderControllerInstance).when(routingRepo).getLeaderController();
    FullHttpResponse response = passRequestToMetadataHandler("http://myRouterHost:4567" + controllerUrl,
        routingRepo, null);

    Assert.assertEquals(response.status().code(), 200);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), "application/json");
    LeaderControllerResponse controllerResponse = mapper.readValue(response.content().array(),
        LeaderControllerResponse.class);
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

    FullHttpResponse response = passRequestToMetadataHandler("http://myRouterHost:4567/key_schema/" + storeName, null, schemaRepo);

    Assert.assertEquals(response.status().code(), 200);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), "application/json");
    SchemaResponse schemaResponse = mapper.readValue(response.content().array(), SchemaResponse.class);
    Assert.assertEquals(schemaResponse.getId(), keySchemaId);
    Assert.assertEquals(schemaResponse.getSchemaStr(), keySchemaStr);
    Assert.assertEquals(schemaResponse.getName(), storeName);
    Assert.assertEquals(schemaResponse.getCluster(), clusterName);
    Assert.assertFalse(schemaResponse.isError());
  }

  @Test
  public void testKeySchemaLookupWithKeySchemaDoesntExist() throws IOException {
    String storeName = "test_store";

    FullHttpResponse response = passRequestToMetadataHandler("http://myRouterHost:4567/key_schema/" + storeName, null, null);

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

    FullHttpResponse response = passRequestToMetadataHandler("http://myRouterHost:4567/value_schema/" + storeName + "/" + valueSchemaId, null, schemaRepo);

    Assert.assertEquals(response.status().code(), 200);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), "application/json");
    SchemaResponse schemaResponse = mapper.readValue(response.content().array(), SchemaResponse.class);
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

    FullHttpResponse response = passRequestToMetadataHandler("http://myRouterHost:4567/value_schema/" + storeName + "/" + valueSchemaId, null, null);

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

    FullHttpResponse response = passRequestToMetadataHandler("http://myRouterHost:4567/value_schema/" + storeName, null, schemaRepo);

    Assert.assertEquals(response.status().code(), 200);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), "application/json");
    MultiSchemaResponse multiSchemaResponse = mapper.readValue(response.content().array(), MultiSchemaResponse.class);

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

    FullHttpResponse response = passRequestToMetadataHandler("http://myRouterHost:4567/value_schema/" + storeName, null, null);

    Assert.assertEquals(response.status().code(), 200);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), "application/json");
    MultiSchemaResponse multiSchemaResponse = mapper.readValue(response.content().array(), MultiSchemaResponse.class);

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
  public void testD2ServiceLookup()
      throws IOException {
    String storeName = "test-store";
    String clusterName = "test-cluster";
    String d2Service = "test-d2-service";
    HelixReadOnlyStoreConfigRepository storeConfigRepository = Mockito.mock(HelixReadOnlyStoreConfigRepository.class);
    StoreConfig storeConfig = new StoreConfig(storeName);
    storeConfig.setCluster(clusterName);
    Mockito.doReturn(Optional.of(storeConfig)).when(storeConfigRepository).getStoreConfig(storeName);
    Map<String, String> clusterToD2Map = new HashMap<>();
    clusterToD2Map.put(clusterName, d2Service);
    FullHttpResponse response =
        passRequestToMetadataHandler("http://myRouterHost:4567/discover_cluster/" + storeName, null, null,
            storeConfigRepository, clusterToD2Map, null);

    Assert.assertEquals(response.status().code(), 200);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), "application/json");
    D2ServiceDiscoveryResponse d2ServiceResponse = mapper.readValue(response.content().array(), D2ServiceDiscoveryResponse.class);
    Assert.assertEquals(d2ServiceResponse.getCluster(), clusterName);
    Assert.assertEquals(d2ServiceResponse.getD2Service(), d2Service);
    Assert.assertEquals(d2ServiceResponse.getName(), storeName);
    Assert.assertFalse(d2ServiceResponse.isError());
  }

  @Test
  public void testD2ServiceLoopNoClusterFound()
      throws IOException {
    String storeName = "test-store";
    HelixReadOnlyStoreConfigRepository storeConfigRepository = Mockito.mock(HelixReadOnlyStoreConfigRepository.class);
    Mockito.doReturn(Optional.empty()).when(storeConfigRepository).getStoreConfig(storeName);
    Map<String, String> clusterToD2Map = new HashMap<>();
    FullHttpResponse response =
        passRequestToMetadataHandler("http://myRouterHost:4567/discover_cluster/" + storeName, null, null,
            storeConfigRepository, Collections.emptyMap(), null);

    Assert.assertEquals(response.status().code(), 404);
  }

  @Test
  public void testResourceStateLookup() throws IOException {
    String resourceName = "test-store_v1";
    OnlineInstanceFinderDelegator mockOnlineInstanceFinder = Mockito.mock(OnlineInstanceFinderDelegator.class);
    HelixReadOnlyStoreConfigRepository storeConfigRepository = Mockito.mock(HelixReadOnlyStoreConfigRepository.class);
    Mockito.doReturn(2).when(mockOnlineInstanceFinder).getNumberOfPartitions(resourceName);
    List<ReplicaState> replicaStates0 = new ArrayList<>();
    replicaStates0.add(new ReplicaState(0, "test-host_0", HelixState.LEADER_STATE,
        ExecutionStatus.COMPLETED.toString(), true));
    List<ReplicaState> replicaStates1 = new ArrayList<>();
    replicaStates1.add(new ReplicaState(1, "test-host_1", HelixState.LEADER_STATE,
        ExecutionStatus.COMPLETED.toString(), true));
    Mockito.doReturn(replicaStates0).when(mockOnlineInstanceFinder).getReplicaStates(resourceName, 0);
    Mockito.doReturn(replicaStates1).when(mockOnlineInstanceFinder).getReplicaStates(resourceName, 1);

    FullHttpResponse response =
        passRequestToMetadataHandler("http://myRouterHost:4567/resource_state/" + resourceName, null,
            null, storeConfigRepository, Collections.emptyMap(), mockOnlineInstanceFinder);
    Assert.assertEquals(response.status().code(), 200);
    ResourceStateResponse resourceStateResponse = mapper.readValue(response.content().array(), ResourceStateResponse.class);
    Assert.assertTrue(resourceStateResponse.isReadyToServe());
    // Add a not ready to serve replica in partition 0 which should put the version to not ready to serve.
    replicaStates0.add(new ReplicaState(0, "test-host_2", HelixState.STANDBY_STATE,
        ExecutionStatus.STARTED.toString(), false));
    replicaStates1.add(new ReplicaState(1, "test-host_3", HelixState.STANDBY_STATE,
        ExecutionStatus.COMPLETED.toString(), true));
    response =
        passRequestToMetadataHandler("http://myRouterHost:4567/resource_state/" + resourceName, null,
            null, storeConfigRepository, Collections.emptyMap(), mockOnlineInstanceFinder);
    Assert.assertEquals(response.status().code(), 200);
    resourceStateResponse = mapper.readValue(response.content().array(), ResourceStateResponse.class);
    Assert.assertFalse(resourceStateResponse.isReadyToServe());
    // Add one more ready to serve replica in each partition which should put the version to ready to serve.
    replicaStates0.add(new ReplicaState(0, "test-host_4", HelixState.STANDBY_STATE,
        ExecutionStatus.COMPLETED.toString(), true));
    replicaStates1.add(new ReplicaState(1, "test-host_5", HelixState.STANDBY_STATE,
        ExecutionStatus.COMPLETED.toString(), true));
    response =
        passRequestToMetadataHandler("http://myRouterHost:4567/resource_state/" + resourceName, null,
            null, storeConfigRepository, Collections.emptyMap(), mockOnlineInstanceFinder);
    Assert.assertEquals(response.status().code(), 200);
    resourceStateResponse = mapper.readValue(response.content().array(), ResourceStateResponse.class);
    Assert.assertTrue(resourceStateResponse.isReadyToServe());
  }

  @Test
  public void testResourceStateLookupOfSystemStores() throws IOException {
    String storeName = "regular-test-store";
    HelixReadOnlyStoreConfigRepository storeConfigRepository = Mockito.mock(HelixReadOnlyStoreConfigRepository.class);
    OnlineInstanceFinderDelegator mockOnlineInstanceFinder = Mockito.mock(OnlineInstanceFinderDelegator.class);
    List<String> systemStoreResources = new ArrayList<>();
    List<ReplicaState> replicaStates = new ArrayList<>();
    replicaStates.add(
        new ReplicaState(0, "test-host_0", HelixState.ONLINE_STATE, ExecutionStatus.COMPLETED.toString(), true));
    for (VeniceSystemStoreType systemStoreType : VeniceSystemStoreType.values()) {
      String resourceName = Version.composeKafkaTopic(systemStoreType.getSystemStoreName(storeName), 1);
      systemStoreResources.add(resourceName);
      Mockito.doReturn(1).when(mockOnlineInstanceFinder).getNumberOfPartitions(resourceName);
      Mockito.doReturn(replicaStates).when(mockOnlineInstanceFinder).getReplicaStates(resourceName, 0);
    }
    for (String systemStoreResource : systemStoreResources) {
      FullHttpResponse response =
          passRequestToMetadataHandler("http://myRouterHost:4567/resource_state/" + systemStoreResource, null,
              null, storeConfigRepository, Collections.emptyMap(), mockOnlineInstanceFinder);
      Assert.assertEquals(response.status().code(), 200);
      ResourceStateResponse resourceStateResponse = mapper.readValue(response.content().array(), ResourceStateResponse.class);
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
    OnlineInstanceFinderDelegator mockOnlineInstanceFinder = Mockito.mock(OnlineInstanceFinderDelegator.class);
    HelixReadOnlyStoreConfigRepository storeConfigRepository = Mockito.mock(HelixReadOnlyStoreConfigRepository.class);
    Mockito.doReturn(Optional.empty())
        .when(storeConfigRepository).getStoreConfig(Version.parseStoreFromKafkaTopicName(resourceName));
    Mockito.doReturn(2).when(mockOnlineInstanceFinder).getNumberOfPartitions(resourceName);
    FullHttpResponse response =
        passRequestToMetadataHandler("http://myRouterHost:4567/resource_state/" + resourceName, null,
            null, storeConfigRepository, Collections.emptyMap(), mockOnlineInstanceFinder, helixReadOnlyStoreRepository);
    Assert.assertEquals(response.status().code(), 404);
    Mockito.doReturn(Optional.of(new StoreConfig(Version.parseStoreFromKafkaTopicName(resourceName))))
        .when(storeConfigRepository).getStoreConfig(Version.parseStoreFromKafkaTopicName(resourceName));
    List<ReplicaState> replicaStates0 = new ArrayList<>();
    replicaStates0.add(new ReplicaState(0, "test-host_0", HelixState.LEADER_STATE,
        ExecutionStatus.COMPLETED.toString(), true));
    Mockito.doReturn(replicaStates0).when(mockOnlineInstanceFinder).getReplicaStates(resourceName, 0);
    Mockito.doReturn(Collections.emptyList()).when(mockOnlineInstanceFinder).getReplicaStates(resourceName, 1);
    response =
        passRequestToMetadataHandler("http://myRouterHost:4567/resource_state/" + resourceName, null,
            null, storeConfigRepository, Collections.emptyMap(), mockOnlineInstanceFinder);
    Assert.assertEquals(response.status().code(), 200);
    ResourceStateResponse resourceStateResponse = mapper.readValue(response.content().array(), ResourceStateResponse.class);
    Assert.assertTrue(resourceStateResponse.isError());
    Assert.assertEquals(resourceStateResponse.getUnretrievablePartitions().iterator().next().intValue(), 1);
  }

  @Test
  public void testPushStatusLookupInRouter() throws IOException {
    String resourceName = "test-store_v1";
    OnlineInstanceFinderDelegator mockOnlineInstanceFinder = Mockito.mock(OnlineInstanceFinderDelegator.class);
    HelixReadOnlyStoreConfigRepository storeConfigRepository = Mockito.mock(HelixReadOnlyStoreConfigRepository.class);
    Mockito.doReturn(Optional.of(new StoreConfig(Version.parseStoreFromKafkaTopicName(resourceName))))
        .when(storeConfigRepository).getStoreConfig(Version.parseStoreFromKafkaTopicName(resourceName));
    Mockito.doReturn(2).when(mockOnlineInstanceFinder).getNumberOfPartitions(resourceName);

    PartitionStatusOnlineInstanceFinder partitionStatusOnlineInstanceFinder = Mockito.mock(PartitionStatusOnlineInstanceFinder.class);
    Mockito.doReturn(partitionStatusOnlineInstanceFinder).when(mockOnlineInstanceFinder).getInstanceFinder(resourceName);

    // Router returns COMPLETED state
    Mockito.doReturn(ExecutionStatus.COMPLETED).when(partitionStatusOnlineInstanceFinder).getPushJobStatus(resourceName);
    FullHttpResponse response =
        passRequestToMetadataHandler("http://myRouterHost:4567/" + TYPE_PUSH_STATUS + "/" + resourceName,
            null, null, storeConfigRepository, Collections.emptyMap(), mockOnlineInstanceFinder);
    Assert.assertEquals(response.status().code(), 200);
    PushStatusResponse pushStatusResponse = mapper.readValue(response.content().array(), PushStatusResponse.class);
    Assert.assertEquals(pushStatusResponse.getExecutionStatus(), ExecutionStatus.COMPLETED);

    // Router returns ERROR state
    Mockito.doReturn(ExecutionStatus.ERROR).when(partitionStatusOnlineInstanceFinder).getPushJobStatus(resourceName);
    response =
        passRequestToMetadataHandler("http://myRouterHost:4567/" + TYPE_PUSH_STATUS + "/" + resourceName,
            null, null, storeConfigRepository, Collections.emptyMap(), mockOnlineInstanceFinder);
    Assert.assertEquals(response.status().code(), 200);
    pushStatusResponse = mapper.readValue(response.content().array(), PushStatusResponse.class);
    Assert.assertEquals(pushStatusResponse.getExecutionStatus(), ExecutionStatus.ERROR);

    // Router returns STARTED state
    Mockito.doReturn(ExecutionStatus.STARTED).when(partitionStatusOnlineInstanceFinder).getPushJobStatus(resourceName);
    response =
        passRequestToMetadataHandler("http://myRouterHost:4567/" + TYPE_PUSH_STATUS + "/" + resourceName,
            null, null, storeConfigRepository, Collections.emptyMap(), mockOnlineInstanceFinder);
    Assert.assertEquals(response.status().code(), 200);
    pushStatusResponse = mapper.readValue(response.content().array(), PushStatusResponse.class);
    Assert.assertEquals(pushStatusResponse.getExecutionStatus(), ExecutionStatus.STARTED);
  }

  @Test
  public void testHybridQuotaInRouter() throws IOException {
    String resourceName = "test-store_v1";
    String storeName = Version.parseStoreFromKafkaTopicName(resourceName);
    OnlineInstanceFinderDelegator mockOnlineInstanceFinder = Mockito.mock(OnlineInstanceFinderDelegator.class);
    HelixReadOnlyStoreConfigRepository storeConfigRepository = Mockito.mock(HelixReadOnlyStoreConfigRepository.class);
    Mockito.doReturn(Optional.of(new StoreConfig(storeName))).when(storeConfigRepository).getStoreConfig(storeName);
    Mockito.doReturn(2).when(mockOnlineInstanceFinder).getNumberOfPartitions(resourceName);

    // Router returns QUOTA_NOT_VIOLATED state
    Mockito.doReturn(HybridStoreQuotaStatus.QUOTA_NOT_VIOLATED).when(hybridStoreQuotaRepository).getHybridStoreQuotaStatus(resourceName);
    FullHttpResponse response =
        passRequestToMetadataHandler("http://myRouterHost:4567/" + TYPE_STREAM_HYBRID_STORE_QUOTA
                + "/" + storeName, null, null, storeConfigRepository, Collections.emptyMap(),
            mockOnlineInstanceFinder);
    Assert.assertEquals(response.status().code(), 200);
    HybridStoreQuotaStatusResponse pushStatusResponse = mapper.readValue(response.content().array(), HybridStoreQuotaStatusResponse.class);
    Assert.assertEquals(pushStatusResponse.getQuotaStatus(), HybridStoreQuotaStatus.QUOTA_NOT_VIOLATED);

    // Router returns QUOTA_VIOLATED state
    Mockito.doReturn(HybridStoreQuotaStatus.QUOTA_VIOLATED).when(hybridStoreQuotaRepository).getHybridStoreQuotaStatus(resourceName);
    response =
        passRequestToMetadataHandler("http://myRouterHost:4567/" + TYPE_STREAM_HYBRID_STORE_QUOTA
                + "/" + storeName, null, null, storeConfigRepository, Collections.emptyMap(),
            mockOnlineInstanceFinder);
    Assert.assertEquals(response.status().code(), 200);
    pushStatusResponse = mapper.readValue(response.content().array(), HybridStoreQuotaStatusResponse.class);
    Assert.assertEquals(pushStatusResponse.getQuotaStatus(), HybridStoreQuotaStatus.QUOTA_VIOLATED);
  }

  @Test
  public void testStreamReprocessingHybridQuotaInRouter() throws IOException {
    String resourceName = "test-store_v3";
    String storeName = Version.parseStoreFromKafkaTopicName(resourceName);
    OnlineInstanceFinderDelegator mockOnlineInstanceFinder = Mockito.mock(OnlineInstanceFinderDelegator.class);
    HelixReadOnlyStoreConfigRepository storeConfigRepository = Mockito.mock(HelixReadOnlyStoreConfigRepository.class);
    Mockito.doReturn(Optional.of(new StoreConfig(storeName))).when(storeConfigRepository).getStoreConfig(storeName);
    Mockito.doReturn(2).when(mockOnlineInstanceFinder).getNumberOfPartitions(resourceName);

    // Router returns QUOTA_NOT_VIOLATED state
    Mockito.doReturn(HybridStoreQuotaStatus.QUOTA_NOT_VIOLATED).when(hybridStoreQuotaRepository).getHybridStoreQuotaStatus(resourceName);
    FullHttpResponse response =
        passRequestToMetadataHandler("http://myRouterHost:4567/" + TYPE_STREAM_REPROCESSING_HYBRID_STORE_QUOTA
                + "/" + resourceName, null, null, storeConfigRepository, Collections.emptyMap(),
            mockOnlineInstanceFinder);
    System.out.println(response);
    Assert.assertEquals(response.status().code(), 200);
    HybridStoreQuotaStatusResponse pushStatusResponse = mapper.readValue(response.content().array(), HybridStoreQuotaStatusResponse.class);
    Assert.assertEquals(pushStatusResponse.getQuotaStatus(), HybridStoreQuotaStatus.QUOTA_NOT_VIOLATED);

    // Router returns QUOTA_VIOLATED state
    Mockito.doReturn(HybridStoreQuotaStatus.QUOTA_VIOLATED).when(hybridStoreQuotaRepository).getHybridStoreQuotaStatus(resourceName);
    response =
        passRequestToMetadataHandler("http://myRouterHost:4567/" + TYPE_STREAM_REPROCESSING_HYBRID_STORE_QUOTA
                + "/" + resourceName, null, null, storeConfigRepository, Collections.emptyMap(),
            mockOnlineInstanceFinder);
    Assert.assertEquals(response.status().code(), 200);
    pushStatusResponse = mapper.readValue(response.content().array(), HybridStoreQuotaStatusResponse.class);
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

    MetaDataHandler handler = new MetaDataHandler(null, null, null, Collections.emptyMap(), null,
        helixReadOnlyStoreRepository, Optional.of(hybridStoreQuotaRepository),  "test-cluster", ZK_ADDRESS,
        KAFKA_ZK_ADDRESS, KAFKA_BOOTSTRAP_SERVERS);
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
    Store zkSharedStore =
        TestUtils.createTestStore(AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE.getSystemStoreName(), "test",
            System.currentTimeMillis());
    HybridStoreConfig hybridStoreConfig = new HybridStoreConfigImpl(Time.SECONDS_PER_DAY, 1,
        TimeUnit.MINUTES.toSeconds(1), DataReplicationPolicy.NON_AGGREGATE, BufferReplayPolicy.REWIND_FROM_EOP);
    zkSharedStore.setHybridStoreConfig(hybridStoreConfig);
    SystemStore systemStore = new SystemStore(zkSharedStore, VeniceSystemStoreType.META_STORE, testStore);
    Mockito.doReturn(testStore).when(mockStoreRepository).getStore(storeName);
    Mockito.doReturn(systemStore).when(mockStoreRepository).getStore(metaSystemStoreName);

    FullHttpResponse response =
        passRequestToMetadataHandler("http://myRouterHost:4567/" + TYPE_STORE_STATE + "/"
            + storeName, null, null, null, Collections.emptyMap(), null, mockStoreRepository);
    Assert.assertEquals(response.status().code(), 200);
    StoreJSONSerializer storeSerializer = new StoreJSONSerializer();
    Store store = storeSerializer.deserialize(response.content().array(), null);
    Assert.assertEquals(store.getCurrentVersion(), 1);
    Assert.assertEquals(store.getVersion(1).get().getPushJobId(), pushId);

    response =
        passRequestToMetadataHandler("http://myRouterHost:4567/" + TYPE_STORE_STATE + "/"
            + metaSystemStoreName, null, null, null, Collections.emptyMap(), null, mockStoreRepository);
    Assert.assertEquals(response.status().code(), 200);
    SystemStoreJSONSerializer systemStoreSerializer = new SystemStoreJSONSerializer();
    SerializableSystemStore serializableSystemStore = systemStoreSerializer.deserialize(response.content().array(), null);
    Store metaSystemStore = new SystemStore(serializableSystemStore.getZkSharedStore(),
        serializableSystemStore.getSystemStoreType(), serializableSystemStore.getVeniceStore());
    Assert.assertEquals(metaSystemStore.getCurrentVersion(), 2);
    Assert.assertEquals(metaSystemStore.getVersion(2).get().getPushJobId(), pushId);
    Assert.assertEquals(metaSystemStore.getHybridStoreConfig(), hybridStoreConfig);

    FullHttpResponse notFoundResponse =
        passRequestToMetadataHandler("http://myRouterHost:4567/" + TYPE_STORE_STATE + "/"
            + "notFound", null, null, null, Collections.emptyMap(), null, mockStoreRepository);
    Assert.assertEquals(notFoundResponse.status().code(), 404);
  }
}
