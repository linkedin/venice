package com.linkedin.venice.controller.server;

import static com.linkedin.venice.HttpConstants.HTTP_GET;
import static com.linkedin.venice.VeniceConstants.CONTROLLER_SSL_CERTIFICATE_ATTRIBUTE_NAME;
import static com.linkedin.venice.controller.server.CreateVersion.overrideSourceRegionAddressForIncrementalPushJob;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.REQUEST_TOPIC;
import static com.linkedin.venice.meta.BufferReplayPolicy.REWIND_FROM_EOP;
import static com.linkedin.venice.meta.DataReplicationPolicy.ACTIVE_ACTIVE;
import static com.linkedin.venice.meta.DataReplicationPolicy.AGGREGATE;
import static com.linkedin.venice.meta.DataReplicationPolicy.NONE;
import static com.linkedin.venice.meta.DataReplicationPolicy.NON_AGGREGATE;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.RequestTopicForPushRequest;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PartitionerConfig;
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
import com.linkedin.venice.utils.lazy.Lazy;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.security.auth.x500.X500Principal;
import javax.servlet.http.HttpServletRequest;
import org.apache.http.HttpStatus;
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

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, description = "requestTopicForPushing should return an RT topic when store is hybrid and inc-push is enabled")
  public void testRequestTopicForHybridIncPushEnabled(boolean isSeparateTopicEnabled) throws Exception {
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

    Object result = createVersionRoute.handle(request, response);
    assertNotNull(result);
    VersionCreationResponse versionCreateResponse =
        OBJECT_MAPPER.readValue(result.toString(), VersionCreationResponse.class);
    if (isSeparateTopicEnabled) {
      assertEquals(versionCreateResponse.getKafkaTopic(), Version.composeSeparateRealTimeTopic(STORE_NAME));
    } else {
      assertEquals(versionCreateResponse.getKafkaTopic(), Version.composeRealTimeTopic(STORE_NAME));
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
    assertNull(versionCreateResponse.getKafkaTopic());
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
    overrideSourceRegionAddressForIncrementalPushJob(
        admin,
        creationResponse,
        CLUSTER_NAME,
        STORE_NAME,
        null,
        null,
        false,
        true);
    assertEquals(creationResponse.getKafkaBootstrapServers(), "default.src.region.com");

    // AA-all-region is disabled & NR is enabled * AGG RT address is set
    creationResponse = new VersionCreationResponse();
    creationResponse.setKafkaBootstrapServers("default.src.region.com");
    doReturn(Optional.of("agg.rt.region.com")).when(admin).getAggregateRealTimeTopicSource(CLUSTER_NAME);
    overrideSourceRegionAddressForIncrementalPushJob(
        admin,
        creationResponse,
        CLUSTER_NAME,
        STORE_NAME,
        null,
        null,
        false,
        true);
    assertEquals(creationResponse.getKafkaBootstrapServers(), "agg.rt.region.com");

    // AA-all-region and NR are disabled
    creationResponse = new VersionCreationResponse();
    creationResponse.setKafkaBootstrapServers("default.src.region.com");
    overrideSourceRegionAddressForIncrementalPushJob(
        admin,
        creationResponse,
        CLUSTER_NAME,
        STORE_NAME,
        null,
        null,
        false,
        false);
    assertEquals(creationResponse.getKafkaBootstrapServers(), "default.src.region.com");

    // AA-all-region is enabled and NR is disabled
    creationResponse = new VersionCreationResponse();
    creationResponse.setKafkaBootstrapServers("default.src.region.com");
    overrideSourceRegionAddressForIncrementalPushJob(
        admin,
        creationResponse,
        CLUSTER_NAME,
        STORE_NAME,
        null,
        null,
        true,
        false);
    assertEquals(creationResponse.getKafkaBootstrapServers(), "default.src.region.com");

    // AA-all-region and NR are enabled AND emergencySourceRegion and pushJobSourceGridFabric are null
    creationResponse = new VersionCreationResponse();
    creationResponse.setKafkaBootstrapServers("default.src.region.com");
    overrideSourceRegionAddressForIncrementalPushJob(
        admin,
        creationResponse,
        CLUSTER_NAME,
        STORE_NAME,
        null,
        null,
        true,
        true);
    assertEquals(creationResponse.getKafkaBootstrapServers(), "default.src.region.com");

    // AA-all-region and NR are enabled AND emergencySourceRegion is not set but pushJobSourceGridFabric is provided
    creationResponse = new VersionCreationResponse();
    creationResponse.setKafkaBootstrapServers("default.src.region.com");
    doReturn("vpj.src.region.com").when(admin).getNativeReplicationKafkaBootstrapServerAddress("dc-vpj");
    overrideSourceRegionAddressForIncrementalPushJob(
        admin,
        creationResponse,
        CLUSTER_NAME,
        STORE_NAME,
        null,
        "dc-vpj",
        true,
        true);
    assertEquals(creationResponse.getKafkaBootstrapServers(), "vpj.src.region.com");

    // AA-all-region and NR are enabled AND emergencySourceRegion is set and pushJobSourceGridFabric is provided
    creationResponse = new VersionCreationResponse();
    creationResponse.setKafkaBootstrapServers("emergency.src.region.com");
    doReturn("emergency.src.region.com").when(admin).getNativeReplicationKafkaBootstrapServerAddress("dc-e");
    overrideSourceRegionAddressForIncrementalPushJob(
        admin,
        creationResponse,
        CLUSTER_NAME,
        STORE_NAME,
        "dc-e",
        "dc-vpj",
        true,
        true);
    assertEquals(creationResponse.getKafkaBootstrapServers(), "emergency.src.region.com");

    // AA-all-region and NR are enabled AND emergencySourceRegion is set and pushJobSourceGridFabric is not provided
    creationResponse = new VersionCreationResponse();
    creationResponse.setKafkaBootstrapServers("emergency.src.region.com");
    doReturn("emergency.src.region.com").when(admin).getNativeReplicationKafkaBootstrapServerAddress("dc-e");
    overrideSourceRegionAddressForIncrementalPushJob(
        admin,
        creationResponse,
        CLUSTER_NAME,
        STORE_NAME,
        "dc-e",
        null,
        true,
        true);
    assertEquals(creationResponse.getKafkaBootstrapServers(), "emergency.src.region.com");
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Failed to get the broker server URL for the source region: dc1")
  public void testOverrideSourceRegionAddressForIncrementalPushJobWhenOverrideRegionAddressIsNotFound() {
    VersionCreationResponse creationResponse = new VersionCreationResponse();
    creationResponse.setKafkaBootstrapServers("default.src.region.com");
    doReturn(null).when(admin).getNativeReplicationKafkaBootstrapServerAddress("dc1");
    overrideSourceRegionAddressForIncrementalPushJob(
        admin,
        creationResponse,
        CLUSTER_NAME,
        STORE_NAME,
        "dc1",
        null,
        true,
        true);
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

  @Test
  public void testExtractOptionalParamsFromRequestTopicForPushingRequest() {
    // Test case 1: Default values
    Request mockRequest = mock(Request.class);
    doCallRealMethod().when(mockRequest).queryParamOrDefault(anyString(), anyString());
    doReturn(null).when(mockRequest).queryParams(any());

    RequestTopicForPushRequest requestDetails =
        new RequestTopicForPushRequest(CLUSTER_NAME, STORE_NAME, PushType.BATCH, JOB_ID);

    CreateVersion.extractOptionalParamsFromRequestTopicRequest(mockRequest, requestDetails, false);

    assertNotNull(requestDetails.getPartitioners(), "Default partitioners should not be null");
    assertTrue(requestDetails.getPartitioners().isEmpty(), "Default partitioners should be empty");
    assertFalse(requestDetails.isSendStartOfPush(), "Default sendStartOfPush should be false");
    assertFalse(requestDetails.isSorted(), "Default sorted should be false");
    assertFalse(requestDetails.isWriteComputeEnabled(), "Default writeComputeEnabled should be false");
    assertEquals(
        requestDetails.getRewindTimeInSecondsOverride(),
        -1L,
        "Default rewindTimeInSecondsOverride should be -1");
    assertFalse(requestDetails.isDeferVersionSwap(), "Default deferVersionSwap should be false");
    assertNull(requestDetails.getTargetedRegions(), "Default targetedRegions should be null");
    assertEquals(requestDetails.getRepushSourceVersion(), -1, "Default repushSourceVersion should be -1");
    assertNull(requestDetails.getSourceGridFabric(), "Default sourceGridFabric should be null");
    assertNull(requestDetails.getCompressionDictionary(), "Default compressionDictionary should be null");
    assertNull(requestDetails.getCertificateInRequest(), "Default certificateInRequest should be null");

    // Test case 2: All optional parameters are set
    mockRequest = mock(Request.class);
    doCallRealMethod().when(mockRequest).queryParamOrDefault(any(), any());
    String customPartitioners = "f.q.c.n.P1,f.q.c.n.P2";
    Set<String> expectedPartitioners = new HashSet<>(Arrays.asList("f.q.c.n.P1", "f.q.c.n.P2"));

    when(mockRequest.queryParams(eq(PARTITIONERS))).thenReturn(customPartitioners);
    when(mockRequest.queryParams(SEND_START_OF_PUSH)).thenReturn("true");
    when(mockRequest.queryParams(PUSH_IN_SORTED_ORDER)).thenReturn("true");
    when(mockRequest.queryParams(IS_WRITE_COMPUTE_ENABLED)).thenReturn("true");
    when(mockRequest.queryParams(REWIND_TIME_IN_SECONDS_OVERRIDE)).thenReturn("120");
    when(mockRequest.queryParams(DEFER_VERSION_SWAP)).thenReturn("true");
    when(mockRequest.queryParams(TARGETED_REGIONS)).thenReturn("region-1");
    when(mockRequest.queryParams(REPUSH_SOURCE_VERSION)).thenReturn("5");
    when(mockRequest.queryParams(SOURCE_GRID_FABRIC)).thenReturn("grid-fabric");
    when(mockRequest.queryParams(COMPRESSION_DICTIONARY)).thenReturn("XYZ");

    requestDetails = new RequestTopicForPushRequest(CLUSTER_NAME, STORE_NAME, PushType.BATCH, JOB_ID);

    CreateVersion.extractOptionalParamsFromRequestTopicRequest(mockRequest, requestDetails, false);

    assertEquals(requestDetails.getPartitioners(), expectedPartitioners);
    assertTrue(requestDetails.isSendStartOfPush());
    assertTrue(requestDetails.isSorted());
    assertTrue(requestDetails.isWriteComputeEnabled());
    assertEquals(requestDetails.getRewindTimeInSecondsOverride(), 120L);
    assertTrue(requestDetails.isDeferVersionSwap());
    assertEquals(requestDetails.getTargetedRegions(), "region-1");
    assertEquals(requestDetails.getRepushSourceVersion(), 5);
    assertEquals(requestDetails.getSourceGridFabric(), "grid-fabric");
    assertEquals(requestDetails.getCompressionDictionary(), "XYZ");

    // Test case 3: check that the certificate is set in the request details when access control is enabled
    HttpServletRequest mockHttpServletRequest = mock(HttpServletRequest.class);
    X509Certificate[] mockCertificates = { mock(X509Certificate.class) };
    when(mockHttpServletRequest.getAttribute(CONTROLLER_SSL_CERTIFICATE_ATTRIBUTE_NAME)).thenReturn(mockCertificates);
    when(mockRequest.raw()).thenReturn(mockHttpServletRequest);
    CreateVersion.extractOptionalParamsFromRequestTopicRequest(mockRequest, requestDetails, true);
    assertEquals(requestDetails.getCertificateInRequest(), mockCertificates[0]);

    // Test case 4: Invalid values for optional parameters
    when(mockRequest.queryParams(SEND_START_OF_PUSH)).thenReturn("notBoolean");
    when(mockRequest.queryParams(REWIND_TIME_IN_SECONDS_OVERRIDE)).thenReturn("invalidLong");

    requestDetails = new RequestTopicForPushRequest(CLUSTER_NAME, STORE_NAME, PushType.BATCH, JOB_ID);
    Request finalMockRequest = mockRequest;
    RequestTopicForPushRequest finalRequestDetails = requestDetails;
    VeniceHttpException e = expectThrows(
        VeniceHttpException.class,
        () -> CreateVersion.extractOptionalParamsFromRequestTopicRequest(finalMockRequest, finalRequestDetails, false));
    assertEquals(e.getHttpStatusCode(), HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  public void testVerifyAndConfigurePartitionerSettings() {
    VersionCreationResponse response = new VersionCreationResponse();
    PartitionerConfig storePartitionerConfig = mock(PartitionerConfig.class);
    when(storePartitionerConfig.getPartitionerClass()).thenReturn("f.q.c.n.DefaultPartitioner");

    // Test Case 1: Null partitionersFromRequest (should pass)
    Set<String> partitionersFromRequest = null;

    try {
      CreateVersion.verifyAndConfigurePartitionerSettings(storePartitionerConfig, partitionersFromRequest, response);
    } catch (Exception e) {
      fail("Null partitionersFromRequest should not throw an exception.");
    }
    assertEquals(response.getPartitionerClass(), "f.q.c.n.DefaultPartitioner");

    // Test Case 2: Empty partitionersFromRequest (should pass)
    response = new VersionCreationResponse();
    partitionersFromRequest = Collections.emptySet();
    try {
      CreateVersion.verifyAndConfigurePartitionerSettings(storePartitionerConfig, partitionersFromRequest, response);
    } catch (Exception e) {
      fail("Empty partitionersFromRequest should not throw an exception.");
    }
    assertEquals(response.getPartitionerClass(), "f.q.c.n.DefaultPartitioner");

    // Test Case 3: Matching partitioner in partitionersFromRequest (should pass)
    response = new VersionCreationResponse();
    partitionersFromRequest = new HashSet<>(Arrays.asList("f.q.c.n.DefaultPartitioner", "f.q.c.n.CustomPartitioner"));
    try {
      CreateVersion.verifyAndConfigurePartitionerSettings(storePartitionerConfig, partitionersFromRequest, response);
    } catch (Exception e) {
      fail("Matching partitioner should not throw an exception.");
    }
    assertEquals(response.getPartitionerClass(), "f.q.c.n.DefaultPartitioner");

    // Test Case 4: Non-matching partitioner in partitionersFromRequest (should throw exception)
    final VersionCreationResponse finalResponse = new VersionCreationResponse();
    partitionersFromRequest = new HashSet<>(Collections.singletonList("f.q.c.n.CustomPartitioner"));
    Set<String> finalPartitionersFromRequest = partitionersFromRequest;
    Exception e = expectThrows(
        VeniceException.class,
        () -> CreateVersion.verifyAndConfigurePartitionerSettings(
            storePartitionerConfig,
            finalPartitionersFromRequest,
            finalResponse));
    assertTrue(e.getMessage().contains("cannot be found"));
  }

  @Test
  public void testDetermineResponseTopic() {
    String storeName = "test_store";
    String vtName = Version.composeKafkaTopic(storeName, 1);
    String rtName = Version.composeRealTimeTopic(storeName);
    String srTopicName = Version.composeStreamReprocessingTopic(storeName, 1);
    String separateRtName = Version.composeSeparateRealTimeTopic(storeName);

    // Test Case 1: PushType.INCREMENTAL with separate real-time topic enabled
    Version mockVersion1 = mock(Version.class);
    when(mockVersion1.kafkaTopicName()).thenReturn(vtName);
    when(mockVersion1.isSeparateRealTimeTopicEnabled()).thenReturn(true);
    String result1 = CreateVersion.determineResponseTopic(storeName, mockVersion1, PushType.INCREMENTAL);
    assertEquals(result1, separateRtName);

    // Test Case 2: PushType.INCREMENTAL without separate real-time topic enabled
    Version mockVersion2 = mock(Version.class);
    when(mockVersion2.kafkaTopicName()).thenReturn(vtName);
    when(mockVersion2.isSeparateRealTimeTopicEnabled()).thenReturn(false);
    String result2 = CreateVersion.determineResponseTopic(storeName, mockVersion2, PushType.INCREMENTAL);
    assertEquals(result2, rtName);

    // Test Case 3: PushType.STREAM
    Version mockVersion3 = mock(Version.class);
    when(mockVersion3.kafkaTopicName()).thenReturn(vtName);
    String result3 = CreateVersion.determineResponseTopic(storeName, mockVersion3, PushType.STREAM);
    assertEquals(result3, rtName);

    // Test Case 4: PushType.STREAM_REPROCESSING
    Version mockVersion4 = mock(Version.class);
    when(mockVersion4.kafkaTopicName()).thenReturn(vtName);
    when(mockVersion4.getNumber()).thenReturn(1);
    String result4 = CreateVersion.determineResponseTopic(storeName, mockVersion4, PushType.STREAM_REPROCESSING);
    assertEquals(result4, srTopicName);

    // Test Case 5: Default case with a Kafka topic
    Version mockVersion5 = mock(Version.class);
    when(mockVersion5.kafkaTopicName()).thenReturn(vtName);
    String result5 = CreateVersion.determineResponseTopic(storeName, mockVersion5, PushType.BATCH);
    assertEquals(result5, vtName);
  }

  @Test
  public void testGetCompressionStrategy() {
    // Test Case 1: Real-time topic returns NO_OP
    Version mockVersion1 = mock(Version.class);
    String responseTopic1 = Version.composeRealTimeTopic("test_store");
    CompressionStrategy result1 = CreateVersion.getCompressionStrategy(mockVersion1, responseTopic1);
    assertEquals(result1, CompressionStrategy.NO_OP);

    // Test Case 2: Non-real-time topic returns version's compression strategy
    Version mockVersion2 = mock(Version.class);
    String responseTopic2 = Version.composeKafkaTopic("test_store", 1);
    when(mockVersion2.getCompressionStrategy()).thenReturn(CompressionStrategy.GZIP);
    CompressionStrategy result2 = CreateVersion.getCompressionStrategy(mockVersion2, responseTopic2);
    assertEquals(result2, CompressionStrategy.GZIP);
  }

  @Test
  public void testConfigureSourceFabric() {
    // Test Case 1: Native replication enabled and non-incremental push type
    Admin mockAdmin1 = mock(Admin.class);
    Version mockVersion1 = mock(Version.class);
    Lazy<Boolean> mockLazy1 = mock(Lazy.class);
    RequestTopicForPushRequest mockRequest1 = mock(RequestTopicForPushRequest.class);
    VersionCreationResponse mockResponse1 = new VersionCreationResponse();

    when(mockVersion1.isNativeReplicationEnabled()).thenReturn(true);
    when(mockVersion1.getPushStreamSourceAddress()).thenReturn("bootstrapServer1");
    when(mockVersion1.getNativeReplicationSourceFabric()).thenReturn("sourceFabric1");
    when(mockRequest1.getPushType()).thenReturn(PushType.BATCH);

    CreateVersion.configureSourceFabric(mockAdmin1, mockVersion1, mockLazy1, mockRequest1, mockResponse1);

    assertEquals(mockResponse1.getKafkaBootstrapServers(), "bootstrapServer1");
    assertEquals(mockResponse1.getKafkaSourceRegion(), "sourceFabric1");

    // Test Case 2: Native replication enabled with null PushStreamSourceAddress
    Admin mockAdmin2 = mock(Admin.class);
    Version mockVersion2 = mock(Version.class);
    Lazy<Boolean> mockLazy2 = mock(Lazy.class);
    RequestTopicForPushRequest mockRequest2 = mock(RequestTopicForPushRequest.class);
    VersionCreationResponse mockResponse2 = new VersionCreationResponse();

    when(mockVersion2.isNativeReplicationEnabled()).thenReturn(true);
    when(mockVersion2.getPushStreamSourceAddress()).thenReturn(null);
    when(mockVersion2.getNativeReplicationSourceFabric()).thenReturn("sourceFabric2");
    when(mockRequest2.getPushType()).thenReturn(PushType.BATCH);

    CreateVersion.configureSourceFabric(mockAdmin2, mockVersion2, mockLazy2, mockRequest2, mockResponse2);

    assertNull(mockResponse2.getKafkaBootstrapServers());
    assertEquals(mockResponse2.getKafkaSourceRegion(), "sourceFabric2");

    // Test Case 3: Incremental push with parent admin and override source region
    Admin mockAdmin3 = mock(Admin.class);
    Version mockVersion3 = mock(Version.class);
    Lazy<Boolean> mockLazy3 = mock(Lazy.class);
    RequestTopicForPushRequest mockRequest3 = mock(RequestTopicForPushRequest.class);
    VersionCreationResponse mockResponse3 = new VersionCreationResponse();

    when(mockAdmin3.isParent()).thenReturn(true);
    when(mockVersion3.isNativeReplicationEnabled()).thenReturn(true);
    when(mockRequest3.getPushType()).thenReturn(PushType.INCREMENTAL);
    when(mockRequest3.getClusterName()).thenReturn("testCluster");
    when(mockRequest3.getStoreName()).thenReturn("testStore");
    when(mockRequest3.getEmergencySourceRegion()).thenReturn("emergencyRegion");
    when(mockRequest3.getSourceGridFabric()).thenReturn("gridFabric");
    when(mockLazy3.get()).thenReturn(true);

    when(mockAdmin3.getNativeReplicationKafkaBootstrapServerAddress("emergencyRegion"))
        .thenReturn("emergencyRegionAddress");

    CreateVersion.configureSourceFabric(mockAdmin3, mockVersion3, mockLazy3, mockRequest3, mockResponse3);

    assertEquals(mockResponse3.getKafkaBootstrapServers(), "emergencyRegionAddress");

    // No specific assertions here since `overrideSourceRegionAddressForIncrementalPushJob` is mocked,
    // but you can verify if the mock was called with appropriate parameters.
    verify(mockAdmin3, times(1)).isParent();
  }
}
