package com.linkedin.venice.router;

import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.MasterControllerResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.helix.HelixReadOnlyStoreConfigRepository;
import com.linkedin.venice.helix.HelixReadOnlyStoreRepository;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OnlineInstanceFinder;
import com.linkedin.venice.meta.OnlineInstanceFinderDelegator;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.PartitionStatusOnlineInstanceFinder;
import com.linkedin.venice.router.api.VenicePathParser;
import com.linkedin.venice.routerapi.PushStatusResponse;
import com.linkedin.venice.routerapi.ReplicaState;
import com.linkedin.venice.routerapi.ResourceStateResponse;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.utils.TestUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.codehaus.jackson.map.ObjectMapper;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.*;

public class TestMetaDataHandler {
  private static ObjectMapper mapper = new ObjectMapper();

  public FullHttpResponse passRequestToMetadataHandler(String requestUri, RoutingDataRepository routing, ReadOnlySchemaRepository schemaRepo)
      throws IOException {
    return passRequestToMetadataHandler(requestUri, routing, schemaRepo,
        Mockito.mock(HelixReadOnlyStoreConfigRepository.class), Collections.emptyMap(), null);
  }

  public FullHttpResponse passRequestToMetadataHandler(String requestUri, RoutingDataRepository routing,
      ReadOnlySchemaRepository schemaRepo, HelixReadOnlyStoreConfigRepository storeConfigRepository,
      Map<String,String> clusterToD2ServiceMap, OnlineInstanceFinderDelegator onlineInstanceFinder)
      throws IOException {
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Store store = TestUtils.createTestStore("testStore", "test", System.currentTimeMillis());

    HttpRequest httpRequest = Mockito.mock(HttpRequest.class);
    HelixReadOnlyStoreRepository helixReadOnlyStoreRepository = Mockito.mock(HelixReadOnlyStoreRepository.class);
    Mockito.doReturn(store).when(helixReadOnlyStoreRepository).getStore(Mockito.anyString());
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

    MetaDataHandler handler = new MetaDataHandler(routing, schemaRepoToUse, "test-cluster",
        storeConfigRepository , clusterToD2ServiceMap, onlineInstanceFinder, helixReadOnlyStoreRepository);
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
    String masterControllerHost = "myControllerHost";
    int masterControllerPort = 1234;
    Instance masterControllerInstance = new Instance("1", masterControllerHost, masterControllerPort);
    Mockito.doReturn(masterControllerInstance).when(routingRepo).getMasterController();

    FullHttpResponse response = passRequestToMetadataHandler("http://myRouterHost:4567/master_controller", routingRepo, null);

    Assert.assertEquals(response.status().code(), 200);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), "application/json");
    MasterControllerResponse controllerResponse = mapper.readValue(response.content().array(),
        MasterControllerResponse.class);
    Assert.assertEquals(controllerResponse.getUrl(), "http://" + masterControllerHost + ":" + masterControllerPort);
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
    Mockito.doReturn(Optional.of(new StoreConfig(Version.parseStoreFromKafkaTopicName(resourceName))))
        .when(storeConfigRepository).getStoreConfig(Version.parseStoreFromKafkaTopicName(resourceName));
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
  public void testResourceStateLookupWithErrors() throws IOException {
    String resourceName = "test-store_v1";
    OnlineInstanceFinderDelegator mockOnlineInstanceFinder = Mockito.mock(OnlineInstanceFinderDelegator.class);
    HelixReadOnlyStoreConfigRepository storeConfigRepository = Mockito.mock(HelixReadOnlyStoreConfigRepository.class);
    Mockito.doReturn(Optional.empty())
        .when(storeConfigRepository).getStoreConfig(Version.parseStoreFromKafkaTopicName(resourceName));
    Mockito.doReturn(2).when(mockOnlineInstanceFinder).getNumberOfPartitions(resourceName);
    FullHttpResponse response =
        passRequestToMetadataHandler("http://myRouterHost:4567/resource_state/" + resourceName, null,
            null, storeConfigRepository, Collections.emptyMap(), mockOnlineInstanceFinder);
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
        passRequestToMetadataHandler("http://myRouterHost:4567/" + VenicePathParser.TYPE_PUSH_STATUS + "/" + resourceName,
            null, null, storeConfigRepository, Collections.emptyMap(), mockOnlineInstanceFinder);
    Assert.assertEquals(response.status().code(), 200);
    PushStatusResponse pushStatusResponse = mapper.readValue(response.content().array(), PushStatusResponse.class);
    Assert.assertEquals(pushStatusResponse.getExecutionStatus(), ExecutionStatus.COMPLETED);

    // Router returns ERROR state
    Mockito.doReturn(ExecutionStatus.ERROR).when(partitionStatusOnlineInstanceFinder).getPushJobStatus(resourceName);
    response =
        passRequestToMetadataHandler("http://myRouterHost:4567/" + VenicePathParser.TYPE_PUSH_STATUS + "/" + resourceName,
            null, null, storeConfigRepository, Collections.emptyMap(), mockOnlineInstanceFinder);
    Assert.assertEquals(response.status().code(), 200);
    pushStatusResponse = mapper.readValue(response.content().array(), PushStatusResponse.class);
    Assert.assertEquals(pushStatusResponse.getExecutionStatus(), ExecutionStatus.ERROR);

    // Router returns STARTED state
    Mockito.doReturn(ExecutionStatus.STARTED).when(partitionStatusOnlineInstanceFinder).getPushJobStatus(resourceName);
    response =
        passRequestToMetadataHandler("http://myRouterHost:4567/" + VenicePathParser.TYPE_PUSH_STATUS + "/" + resourceName,
            null, null, storeConfigRepository, Collections.emptyMap(), mockOnlineInstanceFinder);
    Assert.assertEquals(response.status().code(), 200);
    pushStatusResponse = mapper.readValue(response.content().array(), PushStatusResponse.class);
    Assert.assertEquals(pushStatusResponse.getExecutionStatus(), ExecutionStatus.STARTED);
  }

  @Test
  public void testStorageRequest() throws IOException {
    String storeName = "test_store";
    String clusterName = "test-cluster";

    // Mock Request
    HttpRequest httpRequest = Mockito.mock(HttpRequest.class);
    Mockito.doReturn("http://myRouterHost:4567/storage/" + storeName + "/abc").when(httpRequest).uri();

    // Mock ChannelHandlerContext
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    HelixReadOnlyStoreRepository helixReadOnlyStoreRepository = Mockito.mock(HelixReadOnlyStoreRepository.class);

    MetaDataHandler handler = new MetaDataHandler(null, null, clusterName, null, Collections.emptyMap(), null,
        helixReadOnlyStoreRepository);
    handler.channelRead0(ctx, httpRequest);
    // '/storage' request should be handled by upstream, instead of current MetaDataHandler
    Mockito.verify(ctx, Mockito.times(1)).fireChannelRead(Mockito.any());
  }
}
