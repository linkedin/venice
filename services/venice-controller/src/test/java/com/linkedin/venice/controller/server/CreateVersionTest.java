package com.linkedin.venice.controller.server;

import static com.linkedin.venice.HttpConstants.HTTP_GET;
import static com.linkedin.venice.VeniceConstants.CONTROLLER_SSL_CERTIFICATE_ATTRIBUTE_NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.HOSTNAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PUSH_JOB_ID;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PUSH_TYPE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_SIZE;
import static com.linkedin.venice.controllerapi.ControllerRoute.REQUEST_TOPIC;
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.PartitionerConfigImpl;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.ObjectMapperFactory;
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
  private static final String IP = "0.0.0.0";
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
    queryMap.put(PUSH_TYPE, new String[] { Version.PushType.INCREMENTAL.name() });
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

  @Test
  public void testCreateVersionWithAmplificationFactorAndLeaderFollowerNotEnabled() throws Exception {
    Store store = mock(Store.class);
    PartitionerConfig partitionerConfig = new PartitionerConfigImpl();
    partitionerConfig.setAmplificationFactor(2);

    when(store.isLeaderFollowerModelEnabled()).thenReturn(false);
    when(store.getPartitionerConfig()).thenReturn(partitionerConfig);
    when(admin.getStore(any(), any())).thenReturn(store);

    CreateVersion createVersion = new CreateVersion(false, Optional.empty(), false, false);
    Route createVersionRoute = createVersion.requestTopicForPushing(admin);
    createVersionRoute.handle(request, response);
    verify(response).status(org.apache.http.HttpStatus.SC_BAD_REQUEST);
  }

  @Test(description = "requestTopicForPushing should return an RT topic when store is hybrid and inc-push is enabled")
  public void testRequestTopicForIncPushReturnsRTTopicWhenStoreIsHybridAndIncPushIsEnabled() throws Exception {
    doReturn(true).when(admin).whetherEnableBatchPushFromAdmin(STORE_NAME);
    doCallRealMethod().when(request).queryParamOrDefault(any(), any());
    doReturn(true).when(accessClient).isAllowlistUsers(certificate, STORE_NAME, HTTP_GET);

    Store store = getHybridTestStore();
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
            Version.PushType.INCREMENTAL,
            false,
            false,
            null,
            Optional.empty(),
            Optional.of(certificate),
            -1,
            Optional.empty(),
            false);

    assertTrue(store.isHybrid());
    assertTrue(store.isIncrementalPushEnabled());

    // Build a CreateVersion route.
    CreateVersion createVersion = new CreateVersion(true, Optional.of(accessClient), false, false);
    Route createVersionRoute = createVersion.requestTopicForPushing(admin);

    Object result = createVersionRoute.handle(request, response);
    assertNotNull(result);
    VersionCreationResponse versionCreateResponse =
        OBJECT_MAPPER.readValue(result.toString(), VersionCreationResponse.class);
    assertEquals(versionCreateResponse.getKafkaTopic(), "test_store_rt");
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
            Version.PushType.INCREMENTAL,
            false,
            false,
            null,
            Optional.empty(),
            Optional.of(certificate),
            -1,
            Optional.empty(),
            false);

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
    doReturn("default-src-region").when(admin).getKafkaBootstrapServers(anyBoolean());
    doReturn(true).when(admin).whetherEnableBatchPushFromAdmin(STORE_NAME);
    doReturn(true).when(admin).isParent();
    doCallRealMethod().when(request).queryParamOrDefault(any(), any());
    doReturn(true).when(accessClient).isAllowlistUsers(certificate, STORE_NAME, HTTP_GET);

    Store store = getHybridTestStore();
    store.setIncrementalPushEnabled(true);
    doReturn(store).when(admin).getStore(CLUSTER_NAME, STORE_NAME);

    Optional<String> emergencySrcRegion = Optional.of("dc-1");
    store.setActiveActiveReplicationEnabled(true);
    doReturn(true).when(admin).isActiveActiveReplicationEnabledInAllRegion(any(), any(), anyBoolean());
    doReturn(emergencySrcRegion).when(admin).getEmergencySourceRegion();
    doReturn("dc-1.kafka.venice.org").when(admin)
        .getNativeReplicationKafkaBootstrapServerAddress(emergencySrcRegion.get());

    Version version = new VersionImpl(STORE_NAME, 1, JOB_ID);
    doReturn(version).when(admin)
        .incrementVersionIdempotent(
            CLUSTER_NAME,
            STORE_NAME,
            JOB_ID,
            0,
            0,
            Version.PushType.INCREMENTAL,
            false,
            false,
            null,
            Optional.empty(),
            Optional.of(certificate),
            -1,
            emergencySrcRegion,
            false);

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
    assertEquals(versionCreationResponse.getKafkaBootstrapServers(), "dc-1.kafka.venice.org");
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
            DataReplicationPolicy.NON_AGGREGATE,
            BufferReplayPolicy.REWIND_FROM_EOP));
    return store;
  }
}
