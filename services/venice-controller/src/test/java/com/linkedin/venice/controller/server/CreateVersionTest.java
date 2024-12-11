package com.linkedin.venice.controller.server;

import static com.linkedin.venice.HttpConstants.HTTP_GET;
import static com.linkedin.venice.VeniceConstants.CONTROLLER_SSL_CERTIFICATE_ATTRIBUTE_NAME;
import static com.linkedin.venice.controller.server.CreateVersion.overrideSourceRegionAddressForIncrementalPushJob;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.HOSTNAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PUSH_JOB_ID;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PUSH_TYPE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.REPUSH_SOURCE_VERSION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.SEPARATE_REAL_TIME_TOPIC_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_SIZE;
import static com.linkedin.venice.controllerapi.ControllerRoute.REQUEST_TOPIC;
import static com.linkedin.venice.meta.BufferReplayPolicy.REWIND_FROM_EOP;
import static com.linkedin.venice.meta.DataReplicationPolicy.ACTIVE_ACTIVE;
import static com.linkedin.venice.meta.DataReplicationPolicy.AGGREGATE;
import static com.linkedin.venice.meta.DataReplicationPolicy.NONE;
import static com.linkedin.venice.meta.DataReplicationPolicy.NON_AGGREGATE;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.Version.PushType;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.Utils;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.security.auth.x500.X500Principal;
import javax.servlet.http.HttpServletRequest;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import spark.QueryParamsMap;
import spark.Request;
import spark.Response;
import spark.Route;


public class CreateVersionTest {
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();

  private static final String CLUSTER_NAME = "test_cluster";
  private static final String STORE_NAME = "test_store";
  private static final String USER = "test_user";
  private static final String JOB_ID = "push_1";

  private Admin admin;
  private X509Certificate certificate;
  private Request request;
  private Response response;
  private DynamicAccessController accessClient;

  @BeforeMethod
  public void setUp() {
    admin = mock(Admin.class);
    request = mock(Request.class);
    response = mock(Response.class);
    accessClient = mock(DynamicAccessController.class);
    certificate = mock(X509Certificate.class);
    HttpServletRequest httpServletRequest = mock(HttpServletRequest.class);

    X509Certificate[] certificateArray = new X509Certificate[1];
    certificateArray[0] = certificate;
    X500Principal principal = new X500Principal("CN=" + USER);

    // Setting query params
    Map<String, String[]> queryMap = new HashMap<>();
    queryMap.put(CLUSTER, new String[] { CLUSTER_NAME });
    queryMap.put(NAME, new String[] { STORE_NAME });
    queryMap.put(STORE_SIZE, new String[] { "0" });
    queryMap.put(REPUSH_SOURCE_VERSION, new String[] { "0" });
    queryMap.put(PUSH_TYPE, new String[] { PushType.INCREMENTAL.name() });
    queryMap.put(PUSH_JOB_ID, new String[] { JOB_ID });
    queryMap.put(HOSTNAME, new String[] { "localhost" });

    QueryParamsMap queryParamsMap = new QueryParamsMap(httpServletRequest);

    doReturn(principal).when(certificate).getSubjectX500Principal();
    doReturn(httpServletRequest).when(request).raw();
    doReturn(queryParamsMap).when(request).queryMap();
    doReturn(REQUEST_TOPIC.getPath()).when(request).pathInfo();
    for (Map.Entry<String, String[]> queryParam: queryMap.entrySet()) {
      doReturn(queryParam.getValue()[0]).when(request).queryParams(queryParam.getKey());
    }
    doReturn(queryMap).when(httpServletRequest).getParameterMap();
    doReturn(certificateArray).when(httpServletRequest).getAttribute(CONTROLLER_SSL_CERTIFICATE_ATTRIBUTE_NAME);
    doReturn(true).when(admin).isLeaderControllerFor(CLUSTER_NAME);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testCreateVersionWithACL(boolean checkReadMethod) throws Exception {
    /**
     * Build a CreateVersion route.
     */
    CreateVersion createVersion = new CreateVersion(true, Optional.of(accessClient), checkReadMethod, false);
    Route createVersionRoute = createVersion.requestTopicForPushing(admin);

    // Not an allowlist user.
    doReturn(false).when(accessClient).isAllowlistUsers(certificate, STORE_NAME, HTTP_GET);

    /**
     * Create version should fail if user doesn't have "Write" method access to the topic
     */
    doReturn(false).when(accessClient).hasAccessToTopic(certificate, STORE_NAME, "Write");
    createVersionRoute.handle(request, response);

    /**
     * Response should be 403 if user doesn't have "Write" method access
     */
    verify(response).status(org.apache.http.HttpStatus.SC_FORBIDDEN);

    if (checkReadMethod) {
      response = mock(Response.class);
      /**
       * Create version should fail if user has "Write" method access but not "Read" method access to topics.
       */
      doReturn(true).when(accessClient).hasAccessToTopic(certificate, STORE_NAME, "Write");
      doReturn(false).when(accessClient).hasAccessToTopic(certificate, STORE_NAME, "Read");
      createVersionRoute.handle(request, response);
      verify(response).status(org.apache.http.HttpStatus.SC_FORBIDDEN);
    }
  }

  @Test(dataProvider = "Two-True-and-False", dataProviderClass = DataProviderUtils.class, description = "requestTopicForPushing should return an RT topic when store is hybrid and inc-push is enabled")
  public void testRequestTopicForHybridIncPushEnabled(
      boolean isSeparateTopicEnabled,
      boolean pushToSeparateTopicEnabled) throws Exception {
    doReturn(true).when(admin).whetherEnableBatchPushFromAdmin(STORE_NAME);
    doCallRealMethod().when(request).queryParamOrDefault(any(), any());
    doReturn(true).when(accessClient).isAllowlistUsers(certificate, STORE_NAME, HTTP_GET);

    Store store = getHybridTestStore();
    store.setIncrementalPushEnabled(true);
    doReturn(store).when(admin).getStore(CLUSTER_NAME, STORE_NAME);

    Version version = new VersionImpl(STORE_NAME, 1, JOB_ID);
    version.setSeparateRealTimeTopicEnabled(isSeparateTopicEnabled);
    doReturn(version).when(admin)
        .incrementVersionIdempotent(
            CLUSTER_NAME,
            STORE_NAME,
            JOB_ID,
            0,
            0,
            PushType.INCREMENTAL,
            false,
            false,
            null,
            Optional.empty(),
            Optional.of(certificate),
            -1,
            Optional.empty(),
            false,
            null,
            0);

    assertTrue(store.isHybrid());
    assertTrue(store.isIncrementalPushEnabled());

    // Build a CreateVersion route.
    CreateVersion createVersion = new CreateVersion(true, Optional.of(accessClient), false, false);
    Route createVersionRoute = createVersion.requestTopicForPushing(admin);
    doReturn(Boolean.toString(pushToSeparateTopicEnabled)).when(request)
        .queryParamOrDefault(SEPARATE_REAL_TIME_TOPIC_ENABLED, "false");
    Object result = createVersionRoute.handle(request, response);
    assertNotNull(result);
    VersionCreationResponse versionCreateResponse =
        OBJECT_MAPPER.readValue(result.toString(), VersionCreationResponse.class);
    if (isSeparateTopicEnabled && pushToSeparateTopicEnabled) {
      assertEquals(versionCreateResponse.getKafkaTopic(), Version.composeSeparateRealTimeTopic(STORE_NAME));
    } else {
      assertEquals(versionCreateResponse.getKafkaTopic(), Utils.getRealTimeTopicName(store));
    }
  }

  // A store should never end up in the state where inc-push is enabled but hybrid configs are not set, nevertheless
  // if it happens an ERROR should be returned on requestTopicForPushing with inc-push job type.
  @Test(description = "requestTopicForPushing should an ERROR when store is not in hybrid but inc-push is enabled")
  public void testRequestTopicForIncPushReturnsErrorWhenStoreIsNotHybridAndIncPushIsEnabled() throws Exception {
    doReturn(true).when(admin).whetherEnableBatchPushFromAdmin(STORE_NAME);
    doCallRealMethod().when(request).queryParamOrDefault(any(), any());
    doReturn(true).when(accessClient).isAllowlistUsers(certificate, STORE_NAME, HTTP_GET);

    Store store = new ZKStore(
        STORE_NAME,
        "abc@linkedin.com",
        10,
        PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_ALL_REPLICAS,
        1);
    store.setIncrementalPushEnabled(true);
    doReturn(store).when(admin).getStore(CLUSTER_NAME, STORE_NAME);

    Version version = new VersionImpl(STORE_NAME, 1, JOB_ID);
    doReturn(version).when(admin)
        .incrementVersionIdempotent(
            CLUSTER_NAME,
            STORE_NAME,
            JOB_ID,
            0,
            0,
            PushType.INCREMENTAL,
            false,
            false,
            null,
            Optional.empty(),
            Optional.of(certificate),
            -1,
            Optional.empty(),
            false,
            null,
            -1);

    Assert.assertFalse(store.isHybrid());
    assertTrue(store.isIncrementalPushEnabled());

    // Build a CreateVersion route.
    CreateVersion createVersion = new CreateVersion(true, Optional.of(accessClient), false, false);
    Route createVersionRoute = createVersion.requestTopicForPushing(admin);

    Object result = createVersionRoute.handle(request, response);
    assertNotNull(result);
    verify(response).status(org.apache.http.HttpStatus.SC_BAD_REQUEST);
    VersionCreationResponse versionCreateResponse =
        OBJECT_MAPPER.readValue(result.toString(), VersionCreationResponse.class);
    assertTrue(versionCreateResponse.isError());
    assertTrue(versionCreateResponse.getError().contains("which does not have hybrid mode enabled"));
    Assert.assertNull(versionCreateResponse.getKafkaTopic());
  }

  @Test
  public void testRequestTopicForIncPushCanUseEmergencyRegionWhenItIsSet() throws Exception {
    Store store = getHybridTestStore();
    store.setIncrementalPushEnabled(true);
    store.setActiveActiveReplicationEnabled(true);
    Version version = new VersionImpl(STORE_NAME, 1, JOB_ID);
    Optional<String> emergencySrcRegion = Optional.of("dc-1");

    doReturn("dc-0").when(admin).getRegionName();
    doReturn(true).when(admin).whetherEnableBatchPushFromAdmin(STORE_NAME);
    doReturn(true).when(admin).isParent();
    doReturn(true).when(admin).isActiveActiveReplicationEnabledInAllRegion(any(), any(), anyBoolean());
    doReturn(store).when(admin).getStore(CLUSTER_NAME, STORE_NAME);
    doReturn("default-src.region.io").when(admin).getKafkaBootstrapServers(anyBoolean());
    doReturn(emergencySrcRegion).when(admin).getEmergencySourceRegion(CLUSTER_NAME);
    doCallRealMethod().when(request).queryParamOrDefault(any(), any());
    doReturn(true).when(accessClient).isAllowlistUsers(certificate, STORE_NAME, HTTP_GET);
    doReturn("dc-1.region.io").when(admin).getNativeReplicationKafkaBootstrapServerAddress(emergencySrcRegion.get());
    doReturn(version).when(admin)
        .incrementVersionIdempotent(
            CLUSTER_NAME,
            STORE_NAME,
            JOB_ID,
            0,
            0,
            PushType.INCREMENTAL,
            false,
            false,
            null,
            Optional.empty(),
            Optional.of(certificate),
            -1,
            emergencySrcRegion,
            false,
            null,
            0);

    assertTrue(store.isHybrid());
    assertTrue(store.isIncrementalPushEnabled());
    assertTrue(admin.isParent());

    // Build a CreateVersion route.
    CreateVersion createVersion = new CreateVersion(true, Optional.of(accessClient), false, false);
    Route createVersionRoute = createVersion.requestTopicForPushing(admin);
    Object result = createVersionRoute.handle(request, response);
    assertNotNull(result);
    VersionCreationResponse versionCreationResponse =
        OBJECT_MAPPER.readValue(result.toString(), VersionCreationResponse.class);
    assertEquals(versionCreationResponse.getKafkaBootstrapServers(), "dc-1.region.io");
    assertEquals(versionCreationResponse.getKafkaSourceRegion(), "dc-0");
  }

  private Store getHybridTestStore() {
    Store store = new ZKStore(
        STORE_NAME,
        "abc@linkedin.com",
        10,
        PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_ALL_REPLICAS,
        1);
    store.setHybridStoreConfig(
        new HybridStoreConfigImpl(
            0,
            1,
            HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD,
            NON_AGGREGATE,
            REWIND_FROM_EOP));
    return store;
  }

  @Test
  public void testOverrideSourceRegionAddressForIncrementalPushJob() {
    VersionCreationResponse creationResponse; // reset after every subtest

    // AA-all-region is disabled and NR is enabled but AGG RT address is NOT set
    creationResponse = new VersionCreationResponse();
    creationResponse.setKafkaBootstrapServers("default.src.region.com");
    doReturn(Optional.empty()).when(admin).getAggregateRealTimeTopicSource(CLUSTER_NAME);
    overrideSourceRegionAddressForIncrementalPushJob(admin, creationResponse, CLUSTER_NAME, null, null, false, true);
    assertEquals(creationResponse.getKafkaBootstrapServers(), "default.src.region.com");

    // AA-all-region is disabled & NR is enabled * AGG RT address is set
    creationResponse = new VersionCreationResponse();
    creationResponse.setKafkaBootstrapServers("default.src.region.com");
    doReturn(Optional.of("agg.rt.region.com")).when(admin).getAggregateRealTimeTopicSource(CLUSTER_NAME);
    overrideSourceRegionAddressForIncrementalPushJob(admin, creationResponse, CLUSTER_NAME, null, null, false, true);
    assertEquals(creationResponse.getKafkaBootstrapServers(), "agg.rt.region.com");

    // AA-all-region and NR are disabled
    creationResponse = new VersionCreationResponse();
    creationResponse.setKafkaBootstrapServers("default.src.region.com");
    overrideSourceRegionAddressForIncrementalPushJob(admin, creationResponse, CLUSTER_NAME, null, null, false, false);
    assertEquals(creationResponse.getKafkaBootstrapServers(), "default.src.region.com");

    // AA-all-region is enabled and NR is disabled
    creationResponse = new VersionCreationResponse();
    creationResponse.setKafkaBootstrapServers("default.src.region.com");
    overrideSourceRegionAddressForIncrementalPushJob(admin, creationResponse, CLUSTER_NAME, null, null, true, false);
    assertEquals(creationResponse.getKafkaBootstrapServers(), "default.src.region.com");

    // AA-all-region and NR are enabled AND emergencySourceRegion and pushJobSourceGridFabric are null
    creationResponse = new VersionCreationResponse();
    creationResponse.setKafkaBootstrapServers("default.src.region.com");
    overrideSourceRegionAddressForIncrementalPushJob(admin, creationResponse, CLUSTER_NAME, null, null, true, true);
    assertEquals(creationResponse.getKafkaBootstrapServers(), "default.src.region.com");

    // AA-all-region and NR are enabled AND emergencySourceRegion is not set but pushJobSourceGridFabric is provided
    creationResponse = new VersionCreationResponse();
    creationResponse.setKafkaBootstrapServers("default.src.region.com");
    doReturn("vpj.src.region.com").when(admin).getNativeReplicationKafkaBootstrapServerAddress("dc-vpj");
    overrideSourceRegionAddressForIncrementalPushJob(admin, creationResponse, CLUSTER_NAME, null, "dc-vpj", true, true);
    assertEquals(creationResponse.getKafkaBootstrapServers(), "vpj.src.region.com");

    // AA-all-region and NR are enabled AND emergencySourceRegion is set and pushJobSourceGridFabric is provided
    creationResponse = new VersionCreationResponse();
    creationResponse.setKafkaBootstrapServers("emergency.src.region.com");
    doReturn("emergency.src.region.com").when(admin).getNativeReplicationKafkaBootstrapServerAddress("dc-e");
    overrideSourceRegionAddressForIncrementalPushJob(
        admin,
        creationResponse,
        CLUSTER_NAME,
        "dc-e",
        "dc-vpj",
        true,
        true);
    assertEquals(creationResponse.getKafkaBootstrapServers(), "emergency.src.region.com");

    // AA-all-region and NR are enabled AND emergencySourceRegion is set and pushJobSourceGridFabric is not provided
    creationResponse = new VersionCreationResponse();
    creationResponse.setKafkaBootstrapServers("emergency.src.region.com");
    doReturn("emergency.src.region.com").when(admin).getNativeReplicationKafkaBootstrapServerAddress("dc-e");
    overrideSourceRegionAddressForIncrementalPushJob(admin, creationResponse, CLUSTER_NAME, "dc-e", null, true, true);
    assertEquals(creationResponse.getKafkaBootstrapServers(), "emergency.src.region.com");
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Failed to get the broker server URL for the source region: dc1")
  public void testOverrideSourceRegionAddressForIncrementalPushJobWhenOverrideRegionAddressIsNotFound() {
    VersionCreationResponse creationResponse = new VersionCreationResponse();
    creationResponse.setKafkaBootstrapServers("default.src.region.com");
    doReturn(null).when(admin).getNativeReplicationKafkaBootstrapServerAddress("dc1");
    overrideSourceRegionAddressForIncrementalPushJob(admin, creationResponse, CLUSTER_NAME, "dc1", null, true, true);
  }

  @Test
  public void testValidatePushTypeForStreamPushType() {
    CreateVersion createVersion = new CreateVersion(true, Optional.of(accessClient), false, false);

    // push type is STREAM and store is not hybrid
    Store store1 = mock(Store.class);
    when(store1.isHybrid()).thenReturn(false);
    Exception e = expectThrows(VeniceException.class, () -> createVersion.validatePushType(PushType.STREAM, store1));
    assertTrue(e.getMessage().contains("which is not configured to be a hybrid store"));

    // push type is STREAM and store is AA enabled hybrid
    Store store2 = mock(Store.class);
    when(store2.isHybrid()).thenReturn(true);
    when(store2.isActiveActiveReplicationEnabled()).thenReturn(true);
    createVersion.validatePushType(PushType.STREAM, store2);

    // push type is STREAM and store is not AA enabled hybrid but has NON_AGGREGATE replication policy
    Store store3 = mock(Store.class);
    when(store3.isHybrid()).thenReturn(true);
    when(store3.isActiveActiveReplicationEnabled()).thenReturn(false);
    when(store3.getHybridStoreConfig()).thenReturn(new HybridStoreConfigImpl(0, 1, 0, NON_AGGREGATE, REWIND_FROM_EOP));
    createVersion.validatePushType(PushType.STREAM, store3);

    // push type is STREAM and store is not AA enabled hybrid but has AGGREGATE replication policy
    Store store4 = mock(Store.class);
    when(store4.isHybrid()).thenReturn(true);
    when(store4.isActiveActiveReplicationEnabled()).thenReturn(false);
    when(store4.getHybridStoreConfig()).thenReturn(new HybridStoreConfigImpl(0, 1, 0, AGGREGATE, REWIND_FROM_EOP));
    createVersion.validatePushType(PushType.STREAM, store4);

    // push type is STREAM and store is not AA enabled hybrid but has NONE replication policy
    Store store5 = mock(Store.class);
    when(store5.isHybrid()).thenReturn(true);
    when(store5.isActiveActiveReplicationEnabled()).thenReturn(false);
    when(store5.getHybridStoreConfig()).thenReturn(new HybridStoreConfigImpl(0, 1, 0, NONE, REWIND_FROM_EOP));
    Exception e5 = expectThrows(VeniceException.class, () -> createVersion.validatePushType(PushType.STREAM, store5));
    assertTrue(e5.getMessage().contains("which is configured to have a hybrid data replication policy"));

    // push type is STREAM and store is not AA enabled hybrid but has ACTIVE_ACTIVE replication policy
    Store store6 = mock(Store.class);
    when(store6.isHybrid()).thenReturn(true);
    when(store6.isActiveActiveReplicationEnabled()).thenReturn(false);
    when(store6.getHybridStoreConfig()).thenReturn(new HybridStoreConfigImpl(0, 1, 0, ACTIVE_ACTIVE, REWIND_FROM_EOP));
    Exception e6 = expectThrows(VeniceException.class, () -> createVersion.validatePushType(PushType.STREAM, store6));
    assertTrue(e6.getMessage().contains("which is configured to have a hybrid data replication policy"));
  }

  @Test
  public void testValidatePushTypeForIncrementalPushPushType() {
    CreateVersion createVersion = new CreateVersion(true, Optional.of(accessClient), false, false);

    // push type is INCREMENTAL and store is not hybrid
    Store store1 = mock(Store.class);
    when(store1.isHybrid()).thenReturn(false);
    Exception e =
        expectThrows(VeniceException.class, () -> createVersion.validatePushType(PushType.INCREMENTAL, store1));
    assertTrue(e.getMessage().contains("which does not have hybrid mode enabled"));

    // push type is INCREMENTAL and store is hybrid but incremental push is not enabled
    Store store2 = mock(Store.class);
    when(store2.isHybrid()).thenReturn(true);
    when(store2.isIncrementalPushEnabled()).thenReturn(false);
    Exception e2 =
        expectThrows(VeniceException.class, () -> createVersion.validatePushType(PushType.INCREMENTAL, store2));
    assertTrue(e2.getMessage().contains("which does not have incremental push enabled"));
  }
}
