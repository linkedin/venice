package com.linkedin.venice.router;

import static com.linkedin.venice.HttpConstants.JSON;
import static com.linkedin.venice.HttpConstants.TEXT_PLAIN;
import static com.linkedin.venice.VeniceConstants.TYPE_STORE_STATE;
import static com.linkedin.venice.VeniceConstants.TYPE_STREAM_HYBRID_STORE_QUOTA;
import static com.linkedin.venice.VeniceConstants.TYPE_STREAM_REPROCESSING_HYBRID_STORE_QUOTA;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PARTITIONERS;
import static com.linkedin.venice.router.MetaDataHandler.REQUEST_BLOB_DISCOVERY_ERROR_INVALID_SETTINGS;
import static com.linkedin.venice.router.MetaDataHandler.REQUEST_BLOB_DISCOVERY_ERROR_PUSH_STORE;
import static com.linkedin.venice.router.MetaDataHandler.REQUEST_BLOB_DISCOVERY_MISSING_QUERY_PARAMS;
import static com.linkedin.venice.router.MetaDataHandler.REQUEST_ERROR_STORE_NOT_FOUND_IN_CLUSTER;
import static com.linkedin.venice.router.MetaDataHandler.REQUEST_TOPIC_ERROR_BATCH_ONLY_STORE;
import static com.linkedin.venice.router.MetaDataHandler.REQUEST_TOPIC_ERROR_CURRENT_VERSION_NOT_HYBRID;
import static com.linkedin.venice.router.MetaDataHandler.REQUEST_TOPIC_ERROR_FORMAT_UNSUPPORTED_PARTITIONER;
import static com.linkedin.venice.router.MetaDataHandler.REQUEST_TOPIC_ERROR_MISSING_CURRENT_VERSION;
import static com.linkedin.venice.router.MetaDataHandler.REQUEST_TOPIC_ERROR_NO_CURRENT_VERSION;
import static com.linkedin.venice.router.MetaDataHandler.REQUEST_TOPIC_ERROR_UNSUPPORTED_REPLICATION_POLICY;
import static com.linkedin.venice.router.MetaDataHandler.REQUEST_TOPIC_ERROR_WRITES_DISABLED;
import static com.linkedin.venice.router.api.VenicePathParser.TYPE_BLOB_DISCOVERY;
import static com.linkedin.venice.router.api.VenicePathParser.TYPE_REQUEST_TOPIC;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.blobtransfer.BlobPeersDiscoveryResponse;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.LeaderControllerResponse;
import com.linkedin.venice.controllerapi.MultiSchemaIdResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.helix.HelixHybridStoreQuotaRepository;
import com.linkedin.venice.helix.HelixReadOnlyStoreConfigRepository;
import com.linkedin.venice.helix.HelixReadOnlyStoreRepository;
import com.linkedin.venice.helix.StoreJSONSerializer;
import com.linkedin.venice.helix.SystemStoreJSONSerializer;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.ETLStoreConfigImpl;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.PartitionerConfigImpl;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.SerializableSystemStore;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.SystemStore;
import com.linkedin.venice.meta.SystemStoreAttributes;
import com.linkedin.venice.meta.SystemStoreAttributesImpl;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.HybridStoreQuotaStatus;
import com.linkedin.venice.pushstatushelper.PushStatusStoreReader;
import com.linkedin.venice.routerapi.HybridStoreQuotaStatusResponse;
import com.linkedin.venice.routerapi.ReplicaState;
import com.linkedin.venice.routerapi.ResourceStateResponse;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
      HelixCustomizedViewOfflinePushRepository routingDataRepository,
      ReadOnlySchemaRepository schemaRepo) throws IOException {
    return passRequestToMetadataHandler(
        requestUri,
        routingDataRepository,
        schemaRepo,
        Mockito.mock(HelixReadOnlyStoreConfigRepository.class),
        Collections.emptyMap(),
        Collections.emptyMap(),
        null);
  }

  public FullHttpResponse passRequestToMetadataHandler(
      String requestUri,
      HelixCustomizedViewOfflinePushRepository routingDataRepository,
      ReadOnlySchemaRepository schemaRepo,
      HelixReadOnlyStoreConfigRepository storeConfigRepository,
      Map<String, String> clusterToD2ServiceMap,
      Map<String, String> clusterToServerD2ServiceMap) throws IOException {
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
        clusterToServerD2ServiceMap,
        helixReadOnlyStoreRepository);
  }

  public FullHttpResponse passRequestToMetadataHandler(
      String requestUri,
      HelixCustomizedViewOfflinePushRepository routing,
      ReadOnlySchemaRepository schemaRepo,
      HelixReadOnlyStoreConfigRepository storeConfigRepository,
      Map<String, String> clusterToD2ServiceMap,
      Map<String, String> clusterToServerD2ServiceMap,
      HelixReadOnlyStoreRepository helixReadOnlyStoreRepository) throws IOException {
    PushStatusStoreReader pushStatusStoreReader = Mockito.mock(PushStatusStoreReader.class);
    return passRequestToMetadataHandler(
        requestUri,
        routing,
        schemaRepo,
        storeConfigRepository,
        clusterToD2ServiceMap,
        clusterToServerD2ServiceMap,
        helixReadOnlyStoreRepository,
        pushStatusStoreReader);
  }

  public FullHttpResponse passRequestToMetadataHandler(
      String requestUri,
      HelixCustomizedViewOfflinePushRepository routing,
      ReadOnlySchemaRepository schemaRepo,
      HelixReadOnlyStoreConfigRepository storeConfigRepository,
      Map<String, String> clusterToD2ServiceMap,
      Map<String, String> clusterToServerD2ServiceMap,
      HelixReadOnlyStoreRepository helixReadOnlyStoreRepository,
      PushStatusStoreReader pushStatusStoreReader) throws IOException {
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
        clusterToServerD2ServiceMap,
        helixReadOnlyStoreRepository,
        Optional.of(hybridStoreQuotaRepository),
        "test-cluster",
        ZK_ADDRESS,
        KAFKA_BOOTSTRAP_SERVERS,
        false,
        null,
        pushStatusStoreReader);
    handler.channelRead0(ctx, httpRequest);
    ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
    Mockito.verify(ctx).writeAndFlush(captor.capture());

    FullHttpResponse response = (FullHttpResponse) captor.getValue();
    return response;
  }

  @Test
  public void testControllerLookup() throws IOException {
    // Mock RoutingDataRepository
    HelixCustomizedViewOfflinePushRepository routingRepo = Mockito.mock(HelixCustomizedViewOfflinePushRepository.class);
    String leaderControllerHost = "myControllerHost";
    int leaderControllerPort = 1234;
    Instance leaderControllerInstance = new Instance("1", leaderControllerHost, leaderControllerPort);
    Mockito.doReturn(leaderControllerInstance).when(routingRepo).getLeaderController();

    FullHttpResponse response =
        passRequestToMetadataHandler("http://myRouterHost:4567/leader_controller", routingRepo, null);

    Assert.assertEquals(response.status(), HttpResponseStatus.OK);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), "application/json");
    LeaderControllerResponse controllerResponse =
        OBJECT_MAPPER.readValue(response.content().array(), LeaderControllerResponse.class);
    Assert.assertEquals(controllerResponse.getUrl(), "http://" + leaderControllerHost + ":" + leaderControllerPort);
  }

  // The deprecated non-inclusive URL must also continue to work.
  @DataProvider(name = "controllerUrlProvider")
  public static Object[][] dataProvider() {
    // go/inclusivecode deprecated (alias="leader_controller")
    return new Object[][] { { "master_controller" }, { "leader_controller" } };
  }

  @DataProvider(name = "blobDiscoveryMissingParamsProvider")
  public static Object[][] blobDiscoveryMissingParamsProvider() {
    return new Object[][] { { "", "3", "30" }, { "store1", "", "30" }, { "store1", "3", "" }, { "", "", "" } };
  }

  @DataProvider(name = "blobDiscoverySettingsProvider")
  public static Object[][] blobDiscoverySettingsProvider() {
    return new Object[][] { { true, true }, { false, false }, { true, false }, { false, false } };
  }

  @Test(dataProvider = "controllerUrlProvider")
  public void testControllerLookupLegacy(String controllerUrl) throws IOException {
    // Mock RoutingDataRepository
    HelixCustomizedViewOfflinePushRepository routingRepo = Mockito.mock(HelixCustomizedViewOfflinePushRepository.class);
    String leaderControllerHost = "myControllerHost";
    int leaderControllerPort = 1234;
    Instance leaderControllerInstance = new Instance("1", leaderControllerHost, leaderControllerPort);
    Mockito.doReturn(leaderControllerInstance).when(routingRepo).getLeaderController();
    FullHttpResponse response =
        passRequestToMetadataHandler("http://myRouterHost:4567/" + controllerUrl, routingRepo, null);

    Assert.assertEquals(response.status(), HttpResponseStatus.OK);
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

    Assert.assertEquals(response.status(), HttpResponseStatus.OK);
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

    Assert.assertEquals(response.status(), HttpResponseStatus.NOT_FOUND);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), "text/plain");
  }

  @Test
  public void testInvalidKeySchemaPath() throws IOException {
    FullHttpResponse response = passRequestToMetadataHandler("http://myRouterHost:4567/key_schema/", null, null);
    Assert.assertEquals(response.status(), HttpResponseStatus.BAD_REQUEST);
    Assert
        .assertTrue(new String(response.content().array(), StandardCharsets.UTF_8).contains("Resource name required"));
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

    Assert.assertEquals(response.status(), HttpResponseStatus.OK);
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

    Assert.assertEquals(response.status(), HttpResponseStatus.NOT_FOUND);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), "text/plain");
  }

  @Test
  public void testAllValueSchemaIdLookup() throws IOException {
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
    SchemaEntry valueSchemaEntry3 = new SchemaEntry(-1, valueSchemaStr2);
    Mockito.doReturn(Arrays.asList(valueSchemaEntry1, valueSchemaEntry2, valueSchemaEntry3))
        .when(schemaRepo)
        .getValueSchemas(storeName);
    FullHttpResponse response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/all_value_schema_ids/" + storeName,
        null,
        schemaRepo,
        Mockito.mock(HelixReadOnlyStoreConfigRepository.class),
        Collections.emptyMap(),
        Collections.emptyMap());

    Assert.assertEquals(response.status(), HttpResponseStatus.OK);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), "application/json");
    MultiSchemaIdResponse multiSchemaIdResponse =
        OBJECT_MAPPER.readValue(response.content().array(), MultiSchemaIdResponse.class);

    Assert.assertEquals(multiSchemaIdResponse.getName(), storeName);
    Assert.assertEquals(multiSchemaIdResponse.getCluster(), clusterName);
    Assert.assertFalse(multiSchemaIdResponse.isError());
    Assert.assertEquals(multiSchemaIdResponse.getSchemaIdSet().size(), 2);
    Assert.assertTrue(multiSchemaIdResponse.getSchemaIdSet().contains(1));
    Assert.assertTrue(multiSchemaIdResponse.getSchemaIdSet().contains(2));
    Assert.assertEquals(multiSchemaIdResponse.getSuperSetSchemaId(), SchemaData.INVALID_VALUE_SCHEMA_ID);
  }

  @Test
  public void testAllValueSchemaIdLookupWithNoValueSchema() throws IOException {
    String storeName = "test_store";
    String clusterName = "test-cluster";

    FullHttpResponse response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/value_schema/" + storeName,
        null,
        null,
        Mockito.mock(HelixReadOnlyStoreConfigRepository.class),
        Collections.emptyMap(),
        Collections.emptyMap());

    Assert.assertEquals(response.status(), HttpResponseStatus.OK);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), "application/json");
    MultiSchemaIdResponse multiSchemaIdResponse =
        OBJECT_MAPPER.readValue(response.content().array(), MultiSchemaIdResponse.class);

    Assert.assertEquals(multiSchemaIdResponse.getName(), storeName);
    Assert.assertEquals(multiSchemaIdResponse.getCluster(), clusterName);
    Assert.assertFalse(multiSchemaIdResponse.isError());
    Assert.assertEquals(multiSchemaIdResponse.getSchemaIdSet().size(), 0);
    Assert.assertEquals(multiSchemaIdResponse.getSuperSetSchemaId(), SchemaData.INVALID_VALUE_SCHEMA_ID);

  }

  @Test
  public void testAllValueSchemaLookup() throws IOException {
    String storeName = "test_store";
    String valueSchemaStr1 = "\"string\"";
    String valueSchemaStr2 = "\"long\"";
    String valueSchemaStr3 = "\"int\"";
    String clusterName = "test-cluster";
    int valueSchemaId1 = 1;
    int valueSchemaId2 = 2;
    int valueSchemaId3 = 3;
    // Mock ReadOnlySchemaRepository
    ReadOnlySchemaRepository schemaRepo = Mockito.mock(ReadOnlySchemaRepository.class);
    SchemaEntry valueSchemaEntry1 = new SchemaEntry(valueSchemaId1, valueSchemaStr1);
    SchemaEntry valueSchemaEntry2 = new SchemaEntry(valueSchemaId2, valueSchemaStr2);
    SchemaEntry valueSchemaEntry3 = new SchemaEntry(valueSchemaId3, valueSchemaStr3);
    SchemaEntry valueSchemaEntry4 = new SchemaEntry(-1, valueSchemaStr3);
    Mockito.doReturn(Arrays.asList(valueSchemaEntry1, valueSchemaEntry2, valueSchemaEntry3, valueSchemaEntry4))
        .when(schemaRepo)
        .getValueSchemas(storeName);

    FullHttpResponse response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/value_schema/" + storeName,
        null,
        schemaRepo,
        Mockito.mock(HelixReadOnlyStoreConfigRepository.class),
        Collections.emptyMap(),
        Collections.emptyMap());

    Assert.assertEquals(response.status(), HttpResponseStatus.OK);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), "application/json");
    MultiSchemaResponse multiSchemaResponse =
        OBJECT_MAPPER.readValue(response.content().array(), MultiSchemaResponse.class);

    Assert.assertEquals(multiSchemaResponse.getName(), storeName);
    Assert.assertEquals(multiSchemaResponse.getCluster(), clusterName);
    Assert.assertFalse(multiSchemaResponse.isError());
    MultiSchemaResponse.Schema[] schemas = multiSchemaResponse.getSchemas();
    Assert.assertEquals(schemas.length, 3);
    Assert.assertEquals(schemas[0].getId(), valueSchemaId1);
    Assert.assertEquals(schemas[0].getSchemaStr(), valueSchemaStr1);
    Assert.assertEquals(schemas[1].getId(), valueSchemaId2);
    Assert.assertEquals(schemas[1].getSchemaStr(), valueSchemaStr2);

    // mimic deletion of schema 1 by returning 2 schemas
    Mockito.doReturn(Arrays.asList(valueSchemaEntry2, valueSchemaEntry3)).when(schemaRepo).getValueSchemas(storeName);
    response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/value_schema/" + storeName,
        null,
        schemaRepo,
        Mockito.mock(HelixReadOnlyStoreConfigRepository.class),
        Collections.emptyMap(),
        Collections.emptyMap());
    multiSchemaResponse = OBJECT_MAPPER.readValue(response.content().array(), MultiSchemaResponse.class);
    schemas = multiSchemaResponse.getSchemas();
    Assert.assertEquals(schemas.length, 2);

    Assert.assertEquals(schemas[0].getId(), valueSchemaId2);
    Assert.assertEquals(schemas[0].getSchemaStr(), valueSchemaStr2);
    Assert.assertEquals(schemas[1].getId(), valueSchemaId3);
    Assert.assertEquals(schemas[1].getSchemaStr(), valueSchemaStr3);
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
        Collections.emptyMap(),
        Collections.emptyMap());

    Assert.assertEquals(response.status(), HttpResponseStatus.OK);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), "application/json");
    MultiSchemaResponse multiSchemaResponse =
        OBJECT_MAPPER.readValue(response.content().array(), MultiSchemaResponse.class);

    Assert.assertEquals(multiSchemaResponse.getName(), storeName);
    Assert.assertEquals(multiSchemaResponse.getCluster(), clusterName);
    Assert.assertFalse(multiSchemaResponse.isError());
    MultiSchemaResponse.Schema[] schemas = multiSchemaResponse.getSchemas();
    Assert.assertEquals(schemas.length, 0);
  }

  @Test
  public void testInvalidValueSchemaPath() throws IOException {
    FullHttpResponse response = passRequestToMetadataHandler("http://myRouterHost:4567/value_schema/", null, null);
    Assert.assertEquals(response.status(), HttpResponseStatus.BAD_REQUEST);
    Assert
        .assertTrue(new String(response.content().array(), StandardCharsets.UTF_8).contains("Resource name required"));
  }

  @Test
  public void testLatestValueSchemaLookup() throws IOException {
    String storeName = "test_store";
    String valueSchemaStr = "\"string\"";
    String clusterName = "test-cluster";
    int valueSchemaId = 1;
    // Mock ReadOnlySchemaRepository
    ReadOnlySchemaRepository schemaRepo = Mockito.mock(ReadOnlySchemaRepository.class);
    SchemaEntry latestSchemaEntry = new SchemaEntry(valueSchemaId, valueSchemaStr);
    Mockito.doReturn(latestSchemaEntry).when(schemaRepo).getSupersetOrLatestValueSchema(storeName);

    FullHttpResponse response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/latest_value_schema/" + storeName,
        null,
        schemaRepo,
        Mockito.mock(HelixReadOnlyStoreConfigRepository.class),
        Collections.emptyMap(),
        Collections.emptyMap());

    Assert.assertEquals(response.status(), HttpResponseStatus.OK);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), "application/json");
    SchemaResponse schemaResponse = OBJECT_MAPPER.readValue(response.content().array(), SchemaResponse.class);

    Assert.assertEquals(schemaResponse.getName(), storeName);
    Assert.assertEquals(schemaResponse.getCluster(), clusterName);
    Assert.assertFalse(schemaResponse.isError());
    Assert.assertEquals(schemaResponse.getId(), valueSchemaId);
    Assert.assertEquals(schemaResponse.getSchemaStr(), valueSchemaStr);
  }

  @Test
  public void testLatestValueSchemaLookupWithNoLatestSchema() throws IOException {
    String storeName = "test_store";

    // Mock ReadOnlySchemaRepository
    ReadOnlySchemaRepository schemaRepo = Mockito.mock(ReadOnlySchemaRepository.class);
    Mockito.doReturn(null).when(schemaRepo).getSupersetOrLatestValueSchema(storeName);

    FullHttpResponse response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/latest_value_schema/" + storeName,
        null,
        schemaRepo,
        Mockito.mock(HelixReadOnlyStoreConfigRepository.class),
        Collections.emptyMap(),
        Collections.emptyMap());

    Assert.assertEquals(response.status(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
  }

  @Test
  public void testInvalidSupersetSchemaPath() throws IOException {
    FullHttpResponse response =
        passRequestToMetadataHandler("http://myRouterHost:4567/latest_value_schema/", null, null);
    Assert.assertEquals(response.status(), HttpResponseStatus.BAD_REQUEST);
    Assert
        .assertTrue(new String(response.content().array(), StandardCharsets.UTF_8).contains("Resource name required"));
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

    Assert.assertEquals(response.status(), HttpResponseStatus.OK);
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
  public void testAllUpdateSchemaLookup() throws IOException {
    String storeName = "test_store";
    String valueSchemaStr1 = "\"string\"";
    String valueSchemaStr2 = "\"long\"";
    String updateSchemaStr1 = "\"long\"";
    String updateSchemaStr2 = "\"string\"";
    String clusterName = "test-cluster";
    int valueSchemaId1 = 1;
    int valueSchemaId2 = 2;
    // Mock ReadOnlySchemaRepository
    ReadOnlySchemaRepository schemaRepo = Mockito.mock(ReadOnlySchemaRepository.class);
    SchemaEntry valueSchemaEntry1 = new SchemaEntry(valueSchemaId1, valueSchemaStr1);
    SchemaEntry valueSchemaEntry2 = new SchemaEntry(valueSchemaId2, valueSchemaStr2);
    Mockito.doReturn(Arrays.asList(valueSchemaEntry1, valueSchemaEntry2)).when(schemaRepo).getValueSchemas(storeName);

    DerivedSchemaEntry updateSchemaEntry1 = new DerivedSchemaEntry(valueSchemaId1, 1, updateSchemaStr1);
    DerivedSchemaEntry updateSchemaEntry2 = new DerivedSchemaEntry(valueSchemaId2, 1, updateSchemaStr2);
    Mockito.doReturn(Arrays.asList(updateSchemaEntry1, updateSchemaEntry2))
        .when(schemaRepo)
        .getDerivedSchemas(storeName);

    FullHttpResponse response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/update_schema/" + storeName,
        null,
        schemaRepo,
        Mockito.mock(HelixReadOnlyStoreConfigRepository.class),
        Collections.emptyMap(),
        Collections.emptyMap());

    Assert.assertEquals(response.status(), HttpResponseStatus.OK);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), "application/json");
    MultiSchemaResponse multiSchemaResponse =
        OBJECT_MAPPER.readValue(response.content().array(), MultiSchemaResponse.class);

    Assert.assertEquals(multiSchemaResponse.getName(), storeName);
    Assert.assertEquals(multiSchemaResponse.getCluster(), clusterName);
    Assert.assertFalse(multiSchemaResponse.isError());
    MultiSchemaResponse.Schema[] schemas = multiSchemaResponse.getSchemas();
    Assert.assertEquals(schemas.length, 2);
    Assert.assertEquals(schemas[0].getId(), valueSchemaId1);
    Assert.assertEquals(schemas[0].getSchemaStr(), updateSchemaStr1);
    Assert.assertEquals(schemas[0].getDerivedSchemaId(), 1);
    Assert.assertEquals(schemas[1].getId(), valueSchemaId2);
    Assert.assertEquals(schemas[1].getSchemaStr(), updateSchemaStr2);
    Assert.assertEquals(schemas[1].getDerivedSchemaId(), 1);
  }

  @Test
  public void testAllUpdateSchemaLookupWithNoValueSchema() throws IOException {
    String storeName = "test_store";
    String clusterName = "test-cluster";

    FullHttpResponse response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/update_schema/" + storeName,
        null,
        null,
        Mockito.mock(HelixReadOnlyStoreConfigRepository.class),
        Collections.emptyMap(),
        Collections.emptyMap());

    Assert.assertEquals(response.status(), HttpResponseStatus.OK);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), "application/json");
    MultiSchemaResponse multiSchemaResponse =
        OBJECT_MAPPER.readValue(response.content().array(), MultiSchemaResponse.class);

    Assert.assertEquals(multiSchemaResponse.getName(), storeName);
    Assert.assertEquals(multiSchemaResponse.getCluster(), clusterName);
    Assert.assertFalse(multiSchemaResponse.isError());
    MultiSchemaResponse.Schema[] schemas = multiSchemaResponse.getSchemas();
    Assert.assertEquals(schemas.length, 0);
  }

  @Test
  public void testD2ServiceLookup() throws IOException {
    String storeName = "test-store";
    String clusterName = "test-cluster";
    String d2Service = "test-d2-service";
    String serverD2Service = "test-server-d2-service";
    HelixReadOnlyStoreConfigRepository storeConfigRepository = Mockito.mock(HelixReadOnlyStoreConfigRepository.class);
    StoreConfig storeConfig = new StoreConfig(storeName);
    storeConfig.setCluster(clusterName);
    Mockito.doReturn(Optional.of(storeConfig)).when(storeConfigRepository).getStoreConfig(storeName);
    Map<String, String> clusterToD2Map = new HashMap<>();
    clusterToD2Map.put(clusterName, d2Service);
    Map<String, String> clusterToServerD2Map = new HashMap<>();
    clusterToServerD2Map.put(clusterName, serverD2Service);
    FullHttpResponse response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/discover_cluster/" + storeName,
        null,
        null,
        storeConfigRepository,
        clusterToD2Map,
        clusterToServerD2Map);

    Assert.assertEquals(response.status(), HttpResponseStatus.OK);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), "application/json");
    D2ServiceDiscoveryResponse d2ServiceResponse =
        OBJECT_MAPPER.readValue(response.content().array(), D2ServiceDiscoveryResponse.class);
    Assert.assertEquals(d2ServiceResponse.getCluster(), clusterName);
    Assert.assertEquals(d2ServiceResponse.getD2Service(), d2Service);
    Assert.assertEquals(d2ServiceResponse.getName(), storeName);
    Assert.assertFalse(d2ServiceResponse.isError());

    FullHttpResponse response2 = passRequestToMetadataHandler(
        "http://myRouterHost:4567/discover_cluster?store_name=" + storeName,
        null,
        null,
        storeConfigRepository,
        clusterToD2Map,
        clusterToServerD2Map);

    Assert.assertEquals(response2.status(), HttpResponseStatus.OK);
    Assert.assertEquals(response2.headers().get(CONTENT_TYPE), "application/json");
    D2ServiceDiscoveryResponse d2ServiceResponse2 =
        OBJECT_MAPPER.readValue(response2.content().array(), D2ServiceDiscoveryResponse.class);
    Assert.assertEquals(d2ServiceResponse2.getCluster(), clusterName);
    Assert.assertEquals(d2ServiceResponse2.getD2Service(), d2Service);
    Assert.assertEquals(d2ServiceResponse2.getName(), storeName);
    Assert.assertFalse(d2ServiceResponse2.isError());
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
        Collections.emptyMap());

    Assert.assertEquals(response.status(), HttpResponseStatus.NOT_FOUND);
  }

  @Test
  public void testResourceStateLookup() throws IOException {
    String resourceName = "test-store_v1";
    HelixCustomizedViewOfflinePushRepository routingDataRepository =
        Mockito.mock(HelixCustomizedViewOfflinePushRepository.class);
    HelixReadOnlyStoreConfigRepository storeConfigRepository = Mockito.mock(HelixReadOnlyStoreConfigRepository.class);
    Mockito.doReturn(2).when(routingDataRepository).getNumberOfPartitions(resourceName);
    List<ReplicaState> replicaStates0 = new ArrayList<>();
    replicaStates0.add(new ReplicaState(0, "test-host_0", ExecutionStatus.COMPLETED));
    List<ReplicaState> replicaStates1 = new ArrayList<>();
    replicaStates1.add(new ReplicaState(1, "test-host_1", ExecutionStatus.COMPLETED));
    Mockito.doReturn(replicaStates0).when(routingDataRepository).getReplicaStates(resourceName, 0);
    Mockito.doReturn(replicaStates1).when(routingDataRepository).getReplicaStates(resourceName, 1);

    FullHttpResponse response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/resource_state/" + resourceName,
        routingDataRepository,
        null,
        storeConfigRepository,
        Collections.emptyMap(),
        Collections.emptyMap());
    Assert.assertEquals(response.status(), HttpResponseStatus.OK);
    ResourceStateResponse resourceStateResponse =
        OBJECT_MAPPER.readValue(response.content().array(), ResourceStateResponse.class);
    Assert.assertTrue(resourceStateResponse.isReadyToServe());
    // Add a not ready to serve replica in partition 0 which should put the version to not ready to serve.
    replicaStates0.add(new ReplicaState(0, "test-host_2", ExecutionStatus.STARTED));
    replicaStates1.add(new ReplicaState(1, "test-host_3", ExecutionStatus.COMPLETED));
    response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/resource_state/" + resourceName,
        routingDataRepository,
        null,
        storeConfigRepository,
        Collections.emptyMap(),
        Collections.emptyMap());
    Assert.assertEquals(response.status(), HttpResponseStatus.OK);
    resourceStateResponse = OBJECT_MAPPER.readValue(response.content().array(), ResourceStateResponse.class);
    Assert.assertFalse(resourceStateResponse.isReadyToServe());
    // Add one more ready to serve replica in each partition which should put the version to ready to serve.
    replicaStates0.add(new ReplicaState(0, "test-host_4", ExecutionStatus.COMPLETED));
    replicaStates1.add(new ReplicaState(1, "test-host_5", ExecutionStatus.COMPLETED));
    response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/resource_state/" + resourceName,
        routingDataRepository,
        null,
        storeConfigRepository,
        Collections.emptyMap(),
        Collections.emptyMap());
    Assert.assertEquals(response.status(), HttpResponseStatus.OK);
    resourceStateResponse = OBJECT_MAPPER.readValue(response.content().array(), ResourceStateResponse.class);
    Assert.assertTrue(resourceStateResponse.isReadyToServe());
  }

  @Test
  public void testResourceStateLookupOfSystemStores() throws IOException {
    String storeName = "regular-test-store";
    HelixReadOnlyStoreConfigRepository storeConfigRepository = Mockito.mock(HelixReadOnlyStoreConfigRepository.class);
    HelixCustomizedViewOfflinePushRepository routingDataRepository =
        Mockito.mock(HelixCustomizedViewOfflinePushRepository.class);
    List<String> systemStoreResources = new ArrayList<>();
    List<ReplicaState> replicaStates = new ArrayList<>();
    replicaStates.add(new ReplicaState(0, "test-host_0", ExecutionStatus.COMPLETED));
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
          Collections.emptyMap(),
          Collections.emptyMap());
      Assert.assertEquals(response.status(), HttpResponseStatus.OK);
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
    HelixCustomizedViewOfflinePushRepository routingDataRepository =
        Mockito.mock(HelixCustomizedViewOfflinePushRepository.class);
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
        Collections.emptyMap(),
        helixReadOnlyStoreRepository);
    Assert.assertEquals(response.status(), HttpResponseStatus.NOT_FOUND);
    Mockito.doReturn(Optional.of(new StoreConfig(Version.parseStoreFromKafkaTopicName(resourceName))))
        .when(storeConfigRepository)
        .getStoreConfig(Version.parseStoreFromKafkaTopicName(resourceName));
    List<ReplicaState> replicaStates0 = new ArrayList<>();
    replicaStates0.add(new ReplicaState(0, "test-host_0", ExecutionStatus.COMPLETED));
    Mockito.doReturn(replicaStates0).when(routingDataRepository).getReplicaStates(resourceName, 0);
    Mockito.doReturn(Collections.emptyList()).when(routingDataRepository).getReplicaStates(resourceName, 1);
    response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/resource_state/" + resourceName,
        routingDataRepository,
        null,
        storeConfigRepository,
        Collections.emptyMap(),
        Collections.emptyMap());
    Assert.assertEquals(response.status(), HttpResponseStatus.OK);
    ResourceStateResponse resourceStateResponse =
        OBJECT_MAPPER.readValue(response.content().array(), ResourceStateResponse.class);
    Assert.assertTrue(resourceStateResponse.isError());
    Assert.assertEquals(resourceStateResponse.getUnretrievablePartitions().iterator().next().intValue(), 1);
  }

  @Test
  public void testHybridQuotaInRouter() throws IOException {
    String resourceName = "test-store_v1";
    String storeName = Version.parseStoreFromKafkaTopicName(resourceName);
    HelixCustomizedViewOfflinePushRepository routingDataRepository =
        Mockito.mock(HelixCustomizedViewOfflinePushRepository.class);
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
        Collections.emptyMap(),
        Collections.emptyMap());
    Assert.assertEquals(response.status(), HttpResponseStatus.OK);
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
        Collections.emptyMap(),
        Collections.emptyMap());
    Assert.assertEquals(response.status(), HttpResponseStatus.OK);
    pushStatusResponse = OBJECT_MAPPER.readValue(response.content().array(), HybridStoreQuotaStatusResponse.class);
    Assert.assertEquals(pushStatusResponse.getQuotaStatus(), HybridStoreQuotaStatus.QUOTA_VIOLATED);
  }

  @Test
  public void testStreamReprocessingHybridQuotaInRouter() throws IOException {
    String resourceName = "test-store_v3";
    String storeName = Version.parseStoreFromKafkaTopicName(resourceName);
    HelixCustomizedViewOfflinePushRepository routingDataRepository =
        Mockito.mock(HelixCustomizedViewOfflinePushRepository.class);
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
        Collections.emptyMap(),
        Collections.emptyMap());
    Assert.assertEquals(response.status(), HttpResponseStatus.OK);
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
        Collections.emptyMap(),
        Collections.emptyMap());
    Assert.assertEquals(response.status(), HttpResponseStatus.OK);
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
    PushStatusStoreReader pushStatusStoreReader = Mockito.mock(PushStatusStoreReader.class);

    MetaDataHandler handler = new MetaDataHandler(
        null,
        null,
        null,
        Collections.emptyMap(),
        Collections.emptyMap(),
        helixReadOnlyStoreRepository,
        Optional.of(hybridStoreQuotaRepository),
        clusterName,
        ZK_ADDRESS,
        KAFKA_BOOTSTRAP_SERVERS,
        false,
        null,
        pushStatusStoreReader);
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
        Collections.emptyMap(),
        mockStoreRepository);
    Assert.assertEquals(response.status(), HttpResponseStatus.OK);
    StoreJSONSerializer storeSerializer = new StoreJSONSerializer();
    Store store = storeSerializer.deserialize(response.content().array(), null);
    Assert.assertEquals(store.getCurrentVersion(), 1);
    Assert.assertEquals(store.getVersion(1).getPushJobId(), pushId);

    response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/" + TYPE_STORE_STATE + "/" + metaSystemStoreName,
        null,
        null,
        null,
        Collections.emptyMap(),
        Collections.emptyMap(),
        mockStoreRepository);
    Assert.assertEquals(response.status(), HttpResponseStatus.OK);
    SystemStoreJSONSerializer systemStoreSerializer = new SystemStoreJSONSerializer();
    SerializableSystemStore serializableSystemStore =
        systemStoreSerializer.deserialize(response.content().array(), null);
    Store metaSystemStore = new SystemStore(
        serializableSystemStore.getZkSharedStore(),
        serializableSystemStore.getSystemStoreType(),
        serializableSystemStore.getVeniceStore());
    Assert.assertEquals(metaSystemStore.getCurrentVersion(), 2);
    Assert.assertEquals(metaSystemStore.getVersion(2).getPushJobId(), pushId);
    Assert.assertEquals(metaSystemStore.getHybridStoreConfig(), hybridStoreConfig);

    FullHttpResponse notFoundResponse = passRequestToMetadataHandler(
        "http://myRouterHost:4567/" + TYPE_STORE_STATE + "/" + "notFound",
        null,
        null,
        null,
        Collections.emptyMap(),
        Collections.emptyMap(),
        mockStoreRepository);
    Assert.assertEquals(notFoundResponse.status(), HttpResponseStatus.NOT_FOUND);
  }

  @Test
  public void testRequestTopicForStoreWithWritesDisabled() throws IOException {
    HelixReadOnlyStoreRepository storeRepository = Mockito.mock(HelixReadOnlyStoreRepository.class);
    HelixReadOnlyStoreConfigRepository storeConfigRepository = Mockito.mock(HelixReadOnlyStoreConfigRepository.class);

    String storeName = "test-store";
    Store store = Mockito.mock(Store.class);
    Mockito.doReturn(false).when(store).isEnableWrites();

    Mockito.doReturn(store).when(storeRepository).getStore(storeName);

    FullHttpResponse response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/" + TYPE_REQUEST_TOPIC + "/" + storeName,
        null,
        null,
        storeConfigRepository,
        Collections.emptyMap(),
        Collections.emptyMap(),
        storeRepository);
    Assert.assertEquals(response.status(), HttpResponseStatus.BAD_REQUEST);
    Assert.assertEquals(
        new String(response.content().array(), StandardCharsets.UTF_8),
        REQUEST_TOPIC_ERROR_WRITES_DISABLED);
  }

  @Test
  public void testRequestTopicForBatchOnlyStore() throws IOException {
    HelixReadOnlyStoreRepository storeRepository = Mockito.mock(HelixReadOnlyStoreRepository.class);
    HelixReadOnlyStoreConfigRepository storeConfigRepository = Mockito.mock(HelixReadOnlyStoreConfigRepository.class);

    String storeName = "test-store";
    Store store = Mockito.mock(Store.class);
    Mockito.doReturn(true).when(store).isEnableWrites();
    Mockito.doReturn(store).when(storeRepository).getStore(storeName);

    FullHttpResponse response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/" + TYPE_REQUEST_TOPIC + "/" + storeName,
        null,
        null,
        storeConfigRepository,
        Collections.emptyMap(),
        Collections.emptyMap(),
        storeRepository);
    Assert.assertEquals(response.status(), HttpResponseStatus.BAD_REQUEST);
    Assert.assertEquals(
        new String(response.content().array(), StandardCharsets.UTF_8),
        REQUEST_TOPIC_ERROR_BATCH_ONLY_STORE);
  }

  @Test
  public void testRequestTopicForStoreWithNoCurrentVersion() throws IOException {
    HelixReadOnlyStoreRepository storeRepository = Mockito.mock(HelixReadOnlyStoreRepository.class);
    HelixReadOnlyStoreConfigRepository storeConfigRepository = Mockito.mock(HelixReadOnlyStoreConfigRepository.class);

    String storeName = "test-store";
    Store store = Mockito.mock(Store.class);
    Mockito.doReturn(true).when(store).isEnableWrites();

    HybridStoreConfig badCurrentVersionStoreConfig = new HybridStoreConfigImpl(
        Time.SECONDS_PER_DAY,
        1,
        TimeUnit.MINUTES.toSeconds(1),
        DataReplicationPolicy.NON_AGGREGATE,
        BufferReplayPolicy.REWIND_FROM_EOP);
    Mockito.doReturn(true).when(store).isHybrid();
    Mockito.doReturn(badCurrentVersionStoreConfig).when(store).getHybridStoreConfig();

    Mockito.doReturn(Store.NON_EXISTING_VERSION).when(store).getCurrentVersion();

    Mockito.doReturn(store).when(storeRepository).getStore(storeName);
    FullHttpResponse response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/" + TYPE_REQUEST_TOPIC + "/" + storeName,
        null,
        null,
        storeConfigRepository,
        Collections.emptyMap(),
        Collections.emptyMap(),
        storeRepository);
    Assert.assertEquals(response.status(), HttpResponseStatus.BAD_REQUEST);
    Assert.assertEquals(
        new String(response.content().array(), StandardCharsets.UTF_8),
        REQUEST_TOPIC_ERROR_NO_CURRENT_VERSION);
  }

  @Test
  public void testRequestTopicForStoreWithMissingCurrentVersion() throws IOException {
    HelixReadOnlyStoreRepository storeRepository = Mockito.mock(HelixReadOnlyStoreRepository.class);
    HelixReadOnlyStoreConfigRepository storeConfigRepository = Mockito.mock(HelixReadOnlyStoreConfigRepository.class);

    String storeName = "test-store";
    Store store = Mockito.mock(Store.class);
    Mockito.doReturn(true).when(store).isEnableWrites();

    HybridStoreConfig badCurrentVersionStoreConfig = new HybridStoreConfigImpl(
        Time.SECONDS_PER_DAY,
        1,
        TimeUnit.MINUTES.toSeconds(1),
        DataReplicationPolicy.NON_AGGREGATE,
        BufferReplayPolicy.REWIND_FROM_EOP);
    Mockito.doReturn(true).when(store).isHybrid();
    Mockito.doReturn(badCurrentVersionStoreConfig).when(store).getHybridStoreConfig();

    Mockito.doReturn(1).when(store).getCurrentVersion();
    Mockito.doReturn(null).when(store).getVersion(1);

    Mockito.doReturn(store).when(storeRepository).getStore(storeName);
    FullHttpResponse response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/" + TYPE_REQUEST_TOPIC + "/" + storeName,
        null,
        null,
        storeConfigRepository,
        Collections.emptyMap(),
        Collections.emptyMap(),
        storeRepository);
    Assert.assertEquals(response.status(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
    Assert.assertEquals(
        new String(response.content().array(), StandardCharsets.UTF_8),
        REQUEST_TOPIC_ERROR_MISSING_CURRENT_VERSION);
  }

  @Test
  public void testRequestTopicForStoreWithNonHybridCurrentVersion() throws IOException {
    HelixReadOnlyStoreRepository storeRepository = Mockito.mock(HelixReadOnlyStoreRepository.class);
    HelixReadOnlyStoreConfigRepository storeConfigRepository = Mockito.mock(HelixReadOnlyStoreConfigRepository.class);

    String storeName = "test-store";
    Store store = Mockito.mock(Store.class);
    Mockito.doReturn(true).when(store).isEnableWrites();

    HybridStoreConfig aggStoreConfig = new HybridStoreConfigImpl(
        Time.SECONDS_PER_DAY,
        1,
        TimeUnit.MINUTES.toSeconds(1),
        DataReplicationPolicy.AGGREGATE,
        BufferReplayPolicy.REWIND_FROM_EOP);
    Mockito.doReturn(true).when(store).isHybrid();
    Mockito.doReturn(aggStoreConfig).when(store).getHybridStoreConfig();

    Version currentVersion = Mockito.mock(Version.class);
    Mockito.doReturn(true).when(currentVersion).isUseVersionLevelHybridConfig();
    Mockito.doReturn(null).when(currentVersion).getHybridStoreConfig();
    Mockito.doReturn(1).when(currentVersion).getNumber();

    Mockito.doReturn(1).when(store).getCurrentVersion();
    Mockito.doReturn(currentVersion).when(store).getVersion(1);

    Mockito.doReturn(store).when(storeRepository).getStore(storeName);
    FullHttpResponse response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/" + TYPE_REQUEST_TOPIC + "/" + storeName,
        null,
        null,
        storeConfigRepository,
        Collections.emptyMap(),
        Collections.emptyMap(),
        storeRepository);
    Assert.assertEquals(response.status(), HttpResponseStatus.BAD_REQUEST);
    Assert.assertEquals(
        new String(response.content().array(), StandardCharsets.UTF_8),
        REQUEST_TOPIC_ERROR_CURRENT_VERSION_NOT_HYBRID);
  }

  @Test
  public void testRequestTopicForHybridStoreWithAggregateReplicationPolicy() throws IOException {
    HelixReadOnlyStoreRepository storeRepository = Mockito.mock(HelixReadOnlyStoreRepository.class);
    HelixReadOnlyStoreConfigRepository storeConfigRepository = Mockito.mock(HelixReadOnlyStoreConfigRepository.class);

    String storeName = "test-store";
    Store store = Mockito.mock(Store.class);
    Mockito.doReturn(true).when(store).isEnableWrites();

    HybridStoreConfig aggStoreConfig = new HybridStoreConfigImpl(
        Time.SECONDS_PER_DAY,
        1,
        TimeUnit.MINUTES.toSeconds(1),
        DataReplicationPolicy.AGGREGATE,
        BufferReplayPolicy.REWIND_FROM_EOP);
    Mockito.doReturn(true).when(store).isHybrid();
    Mockito.doReturn(aggStoreConfig).when(store).getHybridStoreConfig();

    Version currentVersion = Mockito.mock(Version.class);
    Mockito.doReturn(aggStoreConfig).when(currentVersion).getHybridStoreConfig();
    Mockito.doReturn(1).when(currentVersion).getNumber();

    Mockito.doReturn(1).when(store).getCurrentVersion();
    Mockito.doReturn(currentVersion).when(store).getVersion(1);

    Mockito.doReturn(store).when(storeRepository).getStore(storeName);
    FullHttpResponse response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/" + TYPE_REQUEST_TOPIC + "/" + storeName,
        null,
        null,
        storeConfigRepository,
        Collections.emptyMap(),
        Collections.emptyMap(),
        storeRepository);
    Assert.assertEquals(response.status(), HttpResponseStatus.BAD_REQUEST);
    Assert.assertEquals(
        new String(response.content().array(), StandardCharsets.UTF_8),
        REQUEST_TOPIC_ERROR_UNSUPPORTED_REPLICATION_POLICY);
  }

  @Test
  public void testRequestTopicForStoreWithNonAggregateReplicationPolicy() throws IOException {
    String clusterName = "test-cluster";
    HelixReadOnlyStoreRepository storeRepository = Mockito.mock(HelixReadOnlyStoreRepository.class);
    HelixReadOnlyStoreConfigRepository storeConfigRepository = Mockito.mock(HelixReadOnlyStoreConfigRepository.class);

    String storeName = "test-store";
    Store store = Mockito.mock(Store.class);
    Mockito.doReturn(true).when(store).isEnableWrites();

    HybridStoreConfig nonAggStoreConfig = new HybridStoreConfigImpl(
        Time.SECONDS_PER_DAY,
        1,
        TimeUnit.MINUTES.toSeconds(1),
        DataReplicationPolicy.NON_AGGREGATE,
        BufferReplayPolicy.REWIND_FROM_EOP);
    Mockito.doReturn(true).when(store).isHybrid();
    Mockito.doReturn(nonAggStoreConfig).when(store).getHybridStoreConfig();

    PartitionerConfig partitionerConfig = new PartitionerConfigImpl();
    Mockito.doReturn(partitionerConfig).when(store).getPartitionerConfig();

    Version currentVersion = Mockito.mock(Version.class);
    Mockito.doReturn(nonAggStoreConfig).when(currentVersion).getHybridStoreConfig();
    Mockito.doReturn(1).when(currentVersion).getNumber();
    Mockito.doReturn(10).when(currentVersion).getPartitionCount();
    Mockito.doReturn(true).when(currentVersion).isUseVersionLevelHybridConfig();

    Mockito.doReturn(1).when(store).getCurrentVersion();
    Mockito.doReturn(currentVersion).when(store).getVersion(1);
    Mockito.doReturn(storeName).when(store).getName();

    Mockito.doReturn(store).when(storeRepository).getStore(storeName);
    FullHttpResponse response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/" + TYPE_REQUEST_TOPIC + "/" + storeName,
        null,
        null,
        storeConfigRepository,
        Collections.emptyMap(),
        Collections.emptyMap(),
        storeRepository);
    Assert.assertEquals(response.status(), HttpResponseStatus.OK);
    VersionCreationResponse versionCreationResponse =
        OBJECT_MAPPER.readValue(response.content().array(), VersionCreationResponse.class);
    Assert.assertEquals(versionCreationResponse.getName(), storeName);
    Assert.assertEquals(versionCreationResponse.getCluster(), clusterName);
    Assert.assertEquals(versionCreationResponse.getKafkaTopic(), Utils.getRealTimeTopicName(store));
    Assert.assertEquals(versionCreationResponse.getKafkaBootstrapServers(), KAFKA_BOOTSTRAP_SERVERS);
    Assert.assertEquals(versionCreationResponse.getAmplificationFactor(), 1);
    Assert.assertEquals(versionCreationResponse.getPartitions(), 10);
    Assert
        .assertEquals(versionCreationResponse.getPartitionerClass(), DefaultVenicePartitioner.class.getCanonicalName());
  }

  @Test
  public void testRequestTopicForStoreWithActiveActiveStore() throws IOException {
    String clusterName = "test-cluster";
    HelixReadOnlyStoreRepository storeRepository = Mockito.mock(HelixReadOnlyStoreRepository.class);
    HelixReadOnlyStoreConfigRepository storeConfigRepository = Mockito.mock(HelixReadOnlyStoreConfigRepository.class);

    String storeName = "test-store";
    Store store = Mockito.mock(Store.class);
    Mockito.doReturn(true).when(store).isEnableWrites();

    HybridStoreConfig nonAggStoreConfig = new HybridStoreConfigImpl(
        Time.SECONDS_PER_DAY,
        1,
        TimeUnit.MINUTES.toSeconds(1),
        DataReplicationPolicy.NON_AGGREGATE,
        BufferReplayPolicy.REWIND_FROM_EOP);
    Mockito.doReturn(true).when(store).isHybrid();
    Mockito.doReturn(nonAggStoreConfig).when(store).getHybridStoreConfig();

    PartitionerConfig partitionerConfig = new PartitionerConfigImpl();
    Mockito.doReturn(partitionerConfig).when(store).getPartitionerConfig();

    Version currentVersion = Mockito.mock(Version.class);
    Mockito.doReturn(nonAggStoreConfig).when(currentVersion).getHybridStoreConfig();
    Mockito.doReturn(1).when(currentVersion).getNumber();
    Mockito.doReturn(10).when(currentVersion).getPartitionCount();
    Mockito.doReturn(true).when(currentVersion).isActiveActiveReplicationEnabled();

    Mockito.doReturn(1).when(store).getCurrentVersion();
    Mockito.doReturn(currentVersion).when(store).getVersion(1);
    Mockito.doReturn(storeName).when(store).getName();

    Mockito.doReturn(store).when(storeRepository).getStore(storeName);
    FullHttpResponse response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/" + TYPE_REQUEST_TOPIC + "/" + storeName,
        null,
        null,
        storeConfigRepository,
        Collections.emptyMap(),
        Collections.emptyMap(),
        storeRepository);
    Assert.assertEquals(response.status(), HttpResponseStatus.OK);
    VersionCreationResponse versionCreationResponse =
        OBJECT_MAPPER.readValue(response.content().array(), VersionCreationResponse.class);
    Assert.assertEquals(versionCreationResponse.getName(), storeName);
    Assert.assertEquals(versionCreationResponse.getCluster(), clusterName);
    Assert.assertEquals(versionCreationResponse.getKafkaTopic(), Utils.getRealTimeTopicName(store));
    Assert.assertEquals(versionCreationResponse.getKafkaBootstrapServers(), KAFKA_BOOTSTRAP_SERVERS);
    Assert.assertEquals(versionCreationResponse.getAmplificationFactor(), 1);
    Assert.assertEquals(versionCreationResponse.getPartitions(), 10);
    Assert
        .assertEquals(versionCreationResponse.getPartitionerClass(), DefaultVenicePartitioner.class.getCanonicalName());
  }

  @Test
  public void testRequestTopicForStoreWithValidPartitioner() throws IOException {
    String clusterName = "test-cluster";
    HelixReadOnlyStoreRepository storeRepository = Mockito.mock(HelixReadOnlyStoreRepository.class);
    HelixReadOnlyStoreConfigRepository storeConfigRepository = Mockito.mock(HelixReadOnlyStoreConfigRepository.class);

    String storeName = "test-store";
    Store store = Mockito.mock(Store.class);
    Mockito.doReturn(true).when(store).isEnableWrites();

    HybridStoreConfig nonAggStoreConfig = new HybridStoreConfigImpl(
        Time.SECONDS_PER_DAY,
        1,
        TimeUnit.MINUTES.toSeconds(1),
        DataReplicationPolicy.NON_AGGREGATE,
        BufferReplayPolicy.REWIND_FROM_EOP);
    Mockito.doReturn(true).when(store).isHybrid();
    Mockito.doReturn(nonAggStoreConfig).when(store).getHybridStoreConfig();

    PartitionerConfig partitionerConfig = new PartitionerConfigImpl();

    String partitionerClass = "com.linkedin.venice.TestVenicePartitioner";
    partitionerConfig.setPartitionerClass(partitionerClass);
    Mockito.doReturn(partitionerConfig).when(store).getPartitionerConfig();

    Version currentVersion = Mockito.mock(Version.class);
    Mockito.doReturn(nonAggStoreConfig).when(currentVersion).getHybridStoreConfig();
    Mockito.doReturn(1).when(currentVersion).getNumber();
    Mockito.doReturn(10).when(currentVersion).getPartitionCount();

    Mockito.doReturn(1).when(store).getCurrentVersion();
    Mockito.doReturn(currentVersion).when(store).getVersion(1);
    Mockito.doReturn(storeName).when(store).getName();

    Mockito.doReturn(store).when(storeRepository).getStore(storeName);
    FullHttpResponse response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/" + TYPE_REQUEST_TOPIC + "/" + storeName + "?" + PARTITIONERS + "="
            + partitionerClass,
        null,
        null,
        storeConfigRepository,
        Collections.emptyMap(),
        Collections.emptyMap(),
        storeRepository);
    Assert.assertEquals(response.status(), HttpResponseStatus.OK);
    VersionCreationResponse versionCreationResponse =
        OBJECT_MAPPER.readValue(response.content().array(), VersionCreationResponse.class);
    Assert.assertEquals(versionCreationResponse.getName(), storeName);
    Assert.assertEquals(versionCreationResponse.getCluster(), clusterName);
    Assert.assertEquals(versionCreationResponse.getKafkaTopic(), Utils.getRealTimeTopicName(store));
    Assert.assertEquals(versionCreationResponse.getKafkaBootstrapServers(), KAFKA_BOOTSTRAP_SERVERS);
    Assert.assertEquals(versionCreationResponse.getAmplificationFactor(), 1);
    Assert.assertEquals(versionCreationResponse.getPartitions(), 10);
    Assert.assertEquals(versionCreationResponse.getPartitionerClass(), partitionerClass);
  }

  @Test
  public void testRequestTopicForStoreWithInvalidPartitioner() throws IOException {
    HelixReadOnlyStoreRepository storeRepository = Mockito.mock(HelixReadOnlyStoreRepository.class);
    HelixReadOnlyStoreConfigRepository storeConfigRepository = Mockito.mock(HelixReadOnlyStoreConfigRepository.class);

    String storeName = "test-store";
    Store store = Mockito.mock(Store.class);
    Mockito.doReturn(true).when(store).isEnableWrites();

    HybridStoreConfig nonAggStoreConfig = new HybridStoreConfigImpl(
        Time.SECONDS_PER_DAY,
        1,
        TimeUnit.MINUTES.toSeconds(1),
        DataReplicationPolicy.NON_AGGREGATE,
        BufferReplayPolicy.REWIND_FROM_EOP);
    Mockito.doReturn(true).when(store).isHybrid();
    Mockito.doReturn(nonAggStoreConfig).when(store).getHybridStoreConfig();

    PartitionerConfig partitionerConfig = new PartitionerConfigImpl();
    Mockito.doReturn(partitionerConfig).when(store).getPartitionerConfig();

    Version currentVersion = Mockito.mock(Version.class);
    Mockito.doReturn(nonAggStoreConfig).when(currentVersion).getHybridStoreConfig();
    Mockito.doReturn(1).when(currentVersion).getNumber();
    Mockito.doReturn(10).when(currentVersion).getPartitionCount();

    Mockito.doReturn(1).when(store).getCurrentVersion();
    Mockito.doReturn(currentVersion).when(store).getVersion(1);

    Mockito.doReturn(store).when(storeRepository).getStore(storeName);

    String partitionerClass = "com.linkedin.venice.TestVenicePartitioner";
    FullHttpResponse response = passRequestToMetadataHandler(
        "http://myRouterHost:4567/" + TYPE_REQUEST_TOPIC + "/" + storeName + "?" + PARTITIONERS + "="
            + partitionerClass,
        null,
        null,
        storeConfigRepository,
        Collections.emptyMap(),
        Collections.emptyMap(),
        storeRepository);
    Assert.assertEquals(response.status(), HttpResponseStatus.BAD_REQUEST);
    Assert.assertEquals(
        new String(response.content().array(), StandardCharsets.UTF_8),
        String.format(
            REQUEST_TOPIC_ERROR_FORMAT_UNSUPPORTED_PARTITIONER,
            DefaultVenicePartitioner.class.getCanonicalName()));
  }

  @Test(dataProvider = "blobDiscoveryMissingParamsProvider")
  public void testHandleBlobDiscoveryMissingParams(String storeName, String storeVersion, String storePartition)
      throws IOException {
    HelixReadOnlyStoreConfigRepository storeConfigRepository = Mockito.mock(HelixReadOnlyStoreConfigRepository.class);
    HelixReadOnlyStoreRepository storeRepository = Mockito.mock(HelixReadOnlyStoreRepository.class);
    PushStatusStoreReader pushStatusStoreReader = Mockito.mock(PushStatusStoreReader.class);

    String requestUri = String.format(
        "http://myRouterHost:4567/%s?store_name=%s&store_version=%s&store_partition=%s",
        TYPE_BLOB_DISCOVERY,
        storeName,
        storeVersion,
        storePartition);

    FullHttpResponse response = passRequestToMetadataHandler(
        requestUri,
        null,
        null,
        storeConfigRepository,
        Collections.emptyMap(),
        Collections.emptyMap(),
        storeRepository,
        pushStatusStoreReader);

    String expectedErrorMsg =
        String.format(REQUEST_BLOB_DISCOVERY_MISSING_QUERY_PARAMS, storeName, storeVersion, storePartition);

    Assert.assertEquals(response.content().toString(StandardCharsets.UTF_8), expectedErrorMsg);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), TEXT_PLAIN);
    Assert.assertEquals(response.status(), HttpResponseStatus.BAD_REQUEST);
  }

  @Test
  public void testHandleBlobDiscoveryInvalidStore() throws IOException {
    HelixReadOnlyStoreConfigRepository storeConfigRepository = Mockito.mock(HelixReadOnlyStoreConfigRepository.class);
    HelixReadOnlyStoreRepository storeRepository = Mockito.mock(HelixReadOnlyStoreRepository.class);
    PushStatusStoreReader pushStatusStoreReader = Mockito.mock(PushStatusStoreReader.class);

    String storeName = "invalid-store";
    String storeVersion = "3";
    String storePartition = "30";

    Mockito.doReturn(null).when(storeRepository).getStore(storeName);

    String requestUri = String.format(
        "http://myRouterHost:4567/%s?store_name=%s&store_version=%s&store_partition=%s",
        TYPE_BLOB_DISCOVERY,
        storeName,
        storeVersion,
        storePartition);

    FullHttpResponse response = passRequestToMetadataHandler(
        requestUri,
        null,
        null,
        storeConfigRepository,
        Collections.emptyMap(),
        Collections.emptyMap(),
        storeRepository,
        pushStatusStoreReader);

    String expectedErrorMsg = String.format(REQUEST_ERROR_STORE_NOT_FOUND_IN_CLUSTER, storeName, "test-cluster");

    Assert.assertEquals(response.content().toString(StandardCharsets.UTF_8), expectedErrorMsg);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), TEXT_PLAIN);
    Assert.assertEquals(response.status(), HttpResponseStatus.NOT_FOUND);
  }

  @Test(dataProvider = "blobDiscoverySettingsProvider")
  public void testHandleBlobDiscoverySettings(Boolean isBlobTransferEnabled, Boolean isHybrid) throws IOException {
    HelixReadOnlyStoreConfigRepository storeConfigRepository = Mockito.mock(HelixReadOnlyStoreConfigRepository.class);
    HelixReadOnlyStoreRepository storeRepository = Mockito.mock(HelixReadOnlyStoreRepository.class);
    PushStatusStoreReader pushStatusStoreReader = Mockito.mock(PushStatusStoreReader.class);

    String storeName = "store1";
    String storeVersion = "3";
    String storePartition = "30";

    Store store = Mockito.mock(Store.class);
    Mockito.doReturn(store).when(storeRepository).getStore(storeName);
    Mockito.doReturn(isBlobTransferEnabled).when(store).isBlobTransferEnabled();
    Mockito.doReturn(isHybrid).when(store).isHybrid();

    String requestUri = String.format(
        "http://myRouterHost:4567/%s?store_name=%s&store_version=%s&store_partition=%s",
        TYPE_BLOB_DISCOVERY,
        storeName,
        storeVersion,
        storePartition);

    FullHttpResponse response = passRequestToMetadataHandler(
        requestUri,
        null,
        null,
        storeConfigRepository,
        Collections.emptyMap(),
        Collections.emptyMap(),
        storeRepository,
        pushStatusStoreReader);

    if (isBlobTransferEnabled) {
      Assert.assertEquals(response.status(), HttpResponseStatus.OK);
      Assert.assertEquals(response.headers().get(CONTENT_TYPE), JSON);
    } else {
      String expectedErrorMsg = String.format(REQUEST_BLOB_DISCOVERY_ERROR_INVALID_SETTINGS, storeName);

      Assert.assertEquals(response.content().toString(StandardCharsets.UTF_8), expectedErrorMsg);
      Assert.assertEquals(response.headers().get(CONTENT_TYPE), TEXT_PLAIN);
      Assert.assertEquals(response.status(), HttpResponseStatus.FORBIDDEN);
    }
  }

  @Test
  public void testHandleBlobDiscoveryPushStatusStoreError() throws IOException {
    HelixReadOnlyStoreConfigRepository storeConfigRepository = Mockito.mock(HelixReadOnlyStoreConfigRepository.class);
    HelixReadOnlyStoreRepository storeRepository = Mockito.mock(HelixReadOnlyStoreRepository.class);
    PushStatusStoreReader pushStatusStoreReader = Mockito.mock(PushStatusStoreReader.class);

    String storeName = "store";
    int storeVersion = 3;
    int storePartition = 30;

    Store store = Mockito.mock(Store.class);
    Mockito.doReturn(store).when(storeRepository).getStore(storeName);
    Mockito.doReturn(true).when(store).isBlobTransferEnabled();
    Mockito.doReturn(false).when(store).isHybrid();

    Mockito.doThrow(new VeniceException("exception"))
        .when(pushStatusStoreReader)
        .getPartitionStatus(storeName, storeVersion, storePartition, Optional.empty());

    String requestUri = String.format(
        "http://myRouterHost:4567/%s?store_name=%s&store_version=%s&store_partition=%s",
        TYPE_BLOB_DISCOVERY,
        storeName,
        storeVersion,
        storePartition);

    FullHttpResponse response = passRequestToMetadataHandler(
        requestUri,
        null,
        null,
        storeConfigRepository,
        Collections.emptyMap(),
        Collections.emptyMap(),
        storeRepository,
        pushStatusStoreReader);

    String expectedErrorMsg =
        String.format(REQUEST_BLOB_DISCOVERY_ERROR_PUSH_STORE, storeName, storeVersion, storePartition);
    Assert.assertEquals(response.content().toString(StandardCharsets.UTF_8), expectedErrorMsg);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), TEXT_PLAIN);
    Assert.assertEquals(response.status(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
  }

  @Test
  public void testHandleBlobDiscoveryLiveNodes() throws IOException {
    HelixReadOnlyStoreRepository storeRepository = Mockito.mock(HelixReadOnlyStoreRepository.class);
    HelixReadOnlyStoreConfigRepository storeConfigRepository = Mockito.mock(HelixReadOnlyStoreConfigRepository.class);
    PushStatusStoreReader pushStatusStoreReader = Mockito.mock(PushStatusStoreReader.class);

    String storeName = "store_name";
    int storeVersion = 1;
    int storePartition = 30;

    Map<CharSequence, Integer> instanceResultsFromPushStatusStore = new HashMap<CharSequence, Integer>() {
      {
        put("ltx1-test.prod.linkedin.com_137", ExecutionStatus.COMPLETED.getValue());
        put("ltx1-test1.prod.linkedin.com_137", ExecutionStatus.NOT_CREATED.getValue());
        put("ltx1-test2.prod.linkedin.com_137", ExecutionStatus.COMPLETED.getValue());
      }
    };

    Map<String, Boolean> instanceHealth = new HashMap<String, Boolean>() {
      {
        put("ltx1-test.prod.linkedin.com_137", true);
        put("ltx1-test1.prod.linkedin.com_137", false);
        put("ltx1-test2.prod.linkedin.com_137", true);
      }
    };

    List<String> expectedResult = Arrays.asList("ltx1-test.prod.linkedin.com_137", "ltx1-test2.prod.linkedin.com_137");

    Mockito.doReturn(instanceResultsFromPushStatusStore)
        .when(pushStatusStoreReader)
        .getPartitionStatus(storeName, storeVersion, storePartition, Optional.empty());

    instanceHealth.forEach((instance, isLive) -> {
      Mockito.doReturn(isLive).when(pushStatusStoreReader).isInstanceAlive(storeName, instance);
    });

    Store store = Mockito.mock(Store.class);
    Mockito.doReturn(store).when(storeRepository).getStore(storeName);
    Mockito.doReturn(true).when(store).isBlobTransferEnabled();
    Mockito.doReturn(false).when(store).isHybrid();

    String requestUri = String.format(
        "http://myRouterHost:4567/%s?store_name=%s&store_version=%s&store_partition=%s",
        TYPE_BLOB_DISCOVERY,
        storeName,
        storeVersion,
        storePartition);

    FullHttpResponse response = passRequestToMetadataHandler(
        requestUri,
        null,
        null,
        storeConfigRepository,
        Collections.emptyMap(),
        Collections.emptyMap(),
        storeRepository,
        pushStatusStoreReader);

    BlobPeersDiscoveryResponse blobDiscoveryResponse =
        OBJECT_MAPPER.readValue(response.content().array(), BlobPeersDiscoveryResponse.class);
    List<String> hostNames = blobDiscoveryResponse.getDiscoveryResult();

    Collections.sort(hostNames);
    Collections.sort(expectedResult);

    Assert.assertEquals(hostNames, expectedResult);
    Assert.assertEquals(response.status(), HttpResponseStatus.OK);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), JSON);
  }

  @Test
  public void testHandleBlobDiscoveryDeadNodes() throws IOException {
    HelixReadOnlyStoreRepository storeRepository = Mockito.mock(HelixReadOnlyStoreRepository.class);
    HelixReadOnlyStoreConfigRepository storeConfigRepository = Mockito.mock(HelixReadOnlyStoreConfigRepository.class);
    PushStatusStoreReader pushStatusStoreReader = Mockito.mock(PushStatusStoreReader.class);

    String storeName = "store_name";
    int storeVersion = 1;
    int storePartition = 30;

    Map<CharSequence, Integer> instanceResultsFromPushStatusStore = new HashMap<CharSequence, Integer>() {
      {
        put("ltx1-test.prod.linkedin.com_137", 1);
        put("ltx1-test1.prod.linkedin.com_137", 1);
        put("ltx1-test2.prod.linkedin.com_137", 1);
      }
    };

    Map<String, Boolean> instanceHealth = new HashMap<String, Boolean>() {
      {
        put("ltx1-test.prod.linkedin.com_137", false);
        put("ltx1-test1.prod.linkedin.com_137", false);
        put("ltx1-test2.prod.linkedin.com_137", false);
      }
    };

    List<String> expectedResult = Collections.emptyList();

    Mockito.doReturn(instanceResultsFromPushStatusStore)
        .when(pushStatusStoreReader)
        .getPartitionStatus(
            storeName,
            Integer.valueOf(storeVersion),
            Integer.valueOf(storePartition),
            Optional.empty());

    instanceHealth.forEach((instance, isLive) -> {
      Mockito.doReturn(isLive).when(pushStatusStoreReader).isInstanceAlive(storeName, instance);
    });

    Store store = Mockito.mock(Store.class);
    Mockito.doReturn(store).when(storeRepository).getStore(storeName);
    Mockito.doReturn(true).when(store).isBlobTransferEnabled();
    Mockito.doReturn(false).when(store).isHybrid();

    String requestUri = String.format(
        "http://myRouterHost:4567/%s?store_name=%s&store_version=%s&store_partition=%s",
        TYPE_BLOB_DISCOVERY,
        storeName,
        storeVersion,
        storePartition);

    FullHttpResponse response = passRequestToMetadataHandler(
        requestUri,
        null,
        null,
        storeConfigRepository,
        Collections.emptyMap(),
        Collections.emptyMap(),
        storeRepository,
        pushStatusStoreReader);

    BlobPeersDiscoveryResponse blobDiscoveryResponse =
        OBJECT_MAPPER.readValue(response.content().array(), BlobPeersDiscoveryResponse.class);
    List<String> hostNames = blobDiscoveryResponse.getDiscoveryResult();

    Assert.assertEquals(hostNames, expectedResult);
    Assert.assertEquals(response.status(), HttpResponseStatus.OK);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), JSON);
  }
}
