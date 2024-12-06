package com.linkedin.venice.controller.server;

import static com.linkedin.venice.meta.Store.NON_EXISTING_VERSION;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;

import com.linkedin.venice.controller.ParentControllerRegionState;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.controllerapi.ControllerRoute;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.httpclient.HttpClientUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.message.BasicNameValuePair;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * For tests that don't require a venice cluster to be running.  We should (where possible) run tests just against one
 * layer.  These tests verify that calls to the spark server interact with the underlying admin as expected, without
 * verifying any state changes that would be triggered by the admin.
 */
public class TestAdminSparkWithMocks {
  @Test
  public void testGetRealTimeTopicUsesAdmin() throws Exception {
    // setup server with mock admin, note returns topic "store_rt"
    VeniceHelixAdmin admin = Mockito.mock(VeniceHelixAdmin.class);
    Store mockStore = new ZKStore(
        "store",
        "owner",
        System.currentTimeMillis(),
        PersistenceType.IN_MEMORY,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION,
        1);
    mockStore.setHybridStoreConfig(
        new HybridStoreConfigImpl(
            25L,
            100L,
            HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD,
            DataReplicationPolicy.NON_AGGREGATE,
            BufferReplayPolicy.REWIND_FROM_EOP));
    doReturn(mockStore).when(admin).getStore(anyString(), anyString());
    doReturn(true).when(admin).isLeaderControllerFor(anyString());
    doReturn(1).when(admin).getReplicationFactor(anyString(), anyString());
    doReturn(1).when(admin).calculateNumberOfPartitions(anyString(), anyString());
    doReturn("kafka-bootstrap").when(admin).getKafkaBootstrapServers(anyBoolean());
    doReturn("store_rt").when(admin).getRealTimeTopic(anyString(), anyString());
    doReturn("store_rt").when(admin).getRealTimeTopic(anyString(), any(Store.class));
    // Add a banned route not relevant to the test just to make sure theres coverage for unbanned routes still be
    // accessible
    AdminSparkServer server =
        ServiceFactory.getMockAdminSparkServer(admin, "clustername", Arrays.asList(ControllerRoute.ADD_DERIVED_SCHEMA));
    int port = server.getPort();

    // build request
    List<NameValuePair> params = new ArrayList<>();
    params.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, "clustername"));
    params.add(new BasicNameValuePair(ControllerApiConstants.NAME, "storename"));
    params.add(new BasicNameValuePair(ControllerApiConstants.STORE_SIZE, Long.toString(1L)));
    params.add(new BasicNameValuePair(ControllerApiConstants.PUSH_JOB_ID, "pushJobId-1234"));
    params.add(new BasicNameValuePair(ControllerApiConstants.PUSH_TYPE, Version.PushType.STREAM.toString()));
    params.add(new BasicNameValuePair(ControllerApiConstants.REPUSH_SOURCE_VERSION, "0"));

    final HttpPost post = new HttpPost("http://localhost:" + port + ControllerRoute.REQUEST_TOPIC.getPath());
    post.setEntity(new UrlEncodedFormEntity(params));

    // make request, parse response
    VersionCreationResponse responseObject;
    try (CloseableHttpAsyncClient httpClient =
        HttpClientUtils.getMinimalHttpClient(1, 1, Optional.of(SslUtils.getVeniceLocalSslFactory()))) {
      httpClient.start();
      HttpResponse response = httpClient.execute(post, null).get();
      String json = IOUtils.toString(response.getEntity().getContent());
      responseObject = ObjectMapperFactory.getInstance().readValue(json, VersionCreationResponse.class);
    }

    // verify response, note we expect same topic, "store_rt"
    Assert.assertFalse(responseObject.isError(), "unexpected error: " + responseObject.getError());
    Assert.assertEquals(responseObject.getKafkaTopic(), "store_rt");

    server.stop();
  }

  @Test
  public void testBannedRoutesAreRejected() throws Exception {
    // setup server with mock admin, note returns topic "store_rt"
    VeniceHelixAdmin admin = Mockito.mock(VeniceHelixAdmin.class);
    Store mockStore = new ZKStore(
        "store",
        "owner",
        System.currentTimeMillis(),
        PersistenceType.IN_MEMORY,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION,
        1);
    mockStore.setHybridStoreConfig(
        new HybridStoreConfigImpl(
            25L,
            100L,
            HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD,
            DataReplicationPolicy.NON_AGGREGATE,
            BufferReplayPolicy.REWIND_FROM_EOP));
    doReturn(mockStore).when(admin).getStore(anyString(), anyString());
    doReturn(true).when(admin).isLeaderControllerFor(anyString());
    doReturn(1).when(admin).getReplicationFactor(anyString(), anyString());
    doReturn(1).when(admin).calculateNumberOfPartitions(anyString(), anyString());
    doReturn("kafka-bootstrap").when(admin).getKafkaBootstrapServers(anyBoolean());
    doReturn("store_rt").when(admin).getRealTimeTopic(anyString(), anyString());
    doReturn("store_rt").when(admin).getRealTimeTopic(anyString(), any(Store.class));
    AdminSparkServer server =
        ServiceFactory.getMockAdminSparkServer(admin, "clustername", Arrays.asList(ControllerRoute.REQUEST_TOPIC));
    int port = server.getPort();

    // build request
    List<NameValuePair> params = new ArrayList<>();
    params.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, "clustername"));
    params.add(new BasicNameValuePair(ControllerApiConstants.NAME, "storename"));
    params.add(new BasicNameValuePair(ControllerApiConstants.STORE_SIZE, Long.toString(1L)));
    params.add(new BasicNameValuePair(ControllerApiConstants.PUSH_JOB_ID, "pushJobId-1234"));
    params.add(new BasicNameValuePair(ControllerApiConstants.PUSH_TYPE, Version.PushType.STREAM.toString()));
    params.add(new BasicNameValuePair(ControllerApiConstants.REPUSH_SOURCE_VERSION, "0"));

    final HttpPost post =
        new HttpPost("http://localhost:" + port + ControllerRoute.REQUEST_TOPIC.getPath() + "?query=foo");

    post.setEntity(new UrlEncodedFormEntity(params));

    // make request, parse response
    VersionCreationResponse responseObject;
    try (CloseableHttpAsyncClient httpClient =
        HttpClientUtils.getMinimalHttpClient(1, 1, Optional.of(SslUtils.getVeniceLocalSslFactory()))) {
      httpClient.start();
      HttpResponse response = httpClient.execute(post, null).get();

      // Make sure we got banned
      Assert.assertEquals(response.getStatusLine().getStatusCode(), HttpStatus.SC_FORBIDDEN);
    } finally {
      server.stop();
    }
  }

  @Test(dataProvider = "Two-True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testAAIncrementalPushRTSourceRegion(boolean sourceGridFabricPresent, boolean emergencySourceRegionPresent)
      throws Exception {
    // setup server with mock admin, note returns topic "store_rt"
    String storeName = "store-inc-push";
    String clusterName = "test_cluster";
    String pushJobId1 = "push_1";
    String corpRegionKafka = "kafka-bootstrap.corp";
    String emergencySourceRegionKafka = "kafka-bootstrap.emergency";
    String sourceGridFabricKafka = "kafka-bootstrap.grid";
    String corpRegion = "region.corp";
    String emergencySourceRegion = "region.emergency";
    String sourceGridFabric = "region.grid";
    Optional<String> optionalemergencySourceRegion = Optional.empty();
    Optional<String> optionalSourceGridSourceFabric = Optional.empty();

    VeniceHelixAdmin admin = Mockito.mock(VeniceHelixAdmin.class);
    Store mockStore = new ZKStore(
        storeName,
        "owner",
        System.currentTimeMillis(),
        PersistenceType.IN_MEMORY,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION,
        1);
    mockStore.setHybridStoreConfig(
        new HybridStoreConfigImpl(
            25L,
            100L,
            HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD,
            DataReplicationPolicy.NON_AGGREGATE,
            BufferReplayPolicy.REWIND_FROM_EOP));
    mockStore.setActiveActiveReplicationEnabled(true);
    mockStore.setIncrementalPushEnabled(true);
    doReturn(mockStore).when(admin).getStore(anyString(), anyString());
    doReturn(true).when(admin).isParent();
    doReturn(ParentControllerRegionState.ACTIVE).when(admin).getParentControllerRegionState();
    doReturn(true).when(admin).isLeaderControllerFor(anyString());
    doReturn(1).when(admin).getReplicationFactor(anyString(), anyString());
    doReturn(1).when(admin).calculateNumberOfPartitions(anyString(), anyString());
    doReturn(corpRegionKafka).when(admin).getKafkaBootstrapServers(anyBoolean());
    doReturn(true).when(admin).whetherEnableBatchPushFromAdmin(anyString());
    doReturn(true).when(admin).isActiveActiveReplicationEnabledInAllRegion(clusterName, storeName, false);
    doReturn(Utils.getRealTimeTopicName(mockStore)).when(admin).getRealTimeTopic(anyString(), anyString());
    doReturn(Utils.getRealTimeTopicName(mockStore)).when(admin).getRealTimeTopic(anyString(), any(Store.class));
    doReturn(corpRegionKafka).when(admin).getNativeReplicationKafkaBootstrapServerAddress(corpRegion);
    doReturn(emergencySourceRegionKafka).when(admin)
        .getNativeReplicationKafkaBootstrapServerAddress(emergencySourceRegion);
    doReturn(sourceGridFabricKafka).when(admin).getNativeReplicationKafkaBootstrapServerAddress(sourceGridFabric);

    if (emergencySourceRegionPresent) {
      doReturn(Optional.of(emergencySourceRegion)).when(admin).getEmergencySourceRegion(clusterName);
      optionalemergencySourceRegion = Optional.of(emergencySourceRegion);
    }

    Version version = new VersionImpl(storeName, 1, pushJobId1);

    // build request
    List<NameValuePair> params = new ArrayList<>();
    params.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, clusterName));
    params.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeName));
    params.add(new BasicNameValuePair(ControllerApiConstants.STORE_SIZE, Long.toString(1L)));
    params.add(new BasicNameValuePair(ControllerApiConstants.PUSH_JOB_ID, pushJobId1));
    params.add(new BasicNameValuePair(ControllerApiConstants.PUSH_TYPE, Version.PushType.INCREMENTAL.toString()));
    params.add(new BasicNameValuePair(ControllerApiConstants.REPUSH_SOURCE_VERSION, "0"));
    if (sourceGridFabricPresent) {
      params.add(new BasicNameValuePair(ControllerApiConstants.SOURCE_GRID_FABRIC, sourceGridFabric));
      optionalSourceGridSourceFabric = Optional.of(sourceGridFabric);
    }

    doReturn(version).when(admin)
        .incrementVersionIdempotent(
            clusterName,
            storeName,
            pushJobId1,
            1,
            1,
            Version.PushType.INCREMENTAL,
            false,
            false,
            null,
            optionalSourceGridSourceFabric,
            Optional.empty(),
            -1,
            optionalemergencySourceRegion,
            false,
            null,
            NON_EXISTING_VERSION);

    // Add a banned route not relevant to the test just to make sure theres coverage for unbanned routes still be
    // accessible
    AdminSparkServer server =
        ServiceFactory.getMockAdminSparkServer(admin, "clustername", Arrays.asList(ControllerRoute.ADD_DERIVED_SCHEMA));
    int port = server.getPort();
    final HttpPost post = new HttpPost("http://localhost:" + port + ControllerRoute.REQUEST_TOPIC.getPath());
    post.setEntity(new UrlEncodedFormEntity(params));

    // make request, parse response
    VersionCreationResponse responseObject;
    try (CloseableHttpAsyncClient httpClient =
        HttpClientUtils.getMinimalHttpClient(1, 1, Optional.of(SslUtils.getVeniceLocalSslFactory()))) {
      httpClient.start();
      HttpResponse response = httpClient.execute(post, null).get();
      String json = IOUtils.toString(response.getEntity().getContent());
      responseObject = ObjectMapperFactory.getInstance().readValue(json, VersionCreationResponse.class);
    }

    // verify response, the kafka bootstrap server should be set correctly depending on inputs.
    Assert.assertFalse(responseObject.isError(), "unexpected error: " + responseObject.getError());
    if (emergencySourceRegionPresent) {
      Assert.assertEquals(responseObject.getKafkaBootstrapServers(), emergencySourceRegionKafka);
    } else if (sourceGridFabricPresent) {
      Assert.assertEquals(responseObject.getKafkaBootstrapServers(), sourceGridFabricKafka);
    } else {
      Assert.assertEquals(responseObject.getKafkaBootstrapServers(), corpRegionKafka);
    }

    server.stop();
  }

  /**
   * @param samzaPolicy true means it's running in AGGREGATE mode, otherwise running in NON_AGGREGATE mode.
   * @param storePolicy true means store is configured in AGGREGATE mode, otherwise configured in NON_AGGREGATE mode.
   * @throws Exception
   */
  @Test(dataProvider = "Three-True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testSamzaReplicationPolicyMode(boolean samzaPolicy, boolean storePolicy, boolean aaEnabled)
      throws Exception {
    // setup server with mock admin, note returns topic "store_rt"
    VeniceHelixAdmin admin = Mockito.mock(VeniceHelixAdmin.class);
    Store mockStore = new ZKStore(
        "store",
        "owner",
        System.currentTimeMillis(),
        PersistenceType.IN_MEMORY,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION,
        1);
    if (storePolicy) {
      mockStore.setHybridStoreConfig(
          new HybridStoreConfigImpl(
              25L,
              100L,
              HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD,
              DataReplicationPolicy.AGGREGATE,
              BufferReplayPolicy.REWIND_FROM_EOP));
    } else {
      mockStore.setHybridStoreConfig(
          new HybridStoreConfigImpl(
              25L,
              100L,
              HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD,
              DataReplicationPolicy.NON_AGGREGATE,
              BufferReplayPolicy.REWIND_FROM_EOP));
    }
    doReturn(mockStore).when(admin).getStore(anyString(), anyString());
    doReturn(true).when(admin).isLeaderControllerFor(anyString());
    doReturn(1).when(admin).getReplicationFactor(anyString(), anyString());
    doReturn(1).when(admin).calculateNumberOfPartitions(anyString(), anyString());
    doReturn("kafka-bootstrap").when(admin).getKafkaBootstrapServers(anyBoolean());
    doReturn("store_rt").when(admin).getRealTimeTopic(anyString(), anyString());
    doReturn("store_rt").when(admin).getRealTimeTopic(anyString(), any(Store.class));
    doReturn(samzaPolicy).when(admin).isParent();
    doReturn(ParentControllerRegionState.ACTIVE).when(admin).getParentControllerRegionState();
    doReturn(aaEnabled).when(admin).isActiveActiveReplicationEnabledInAllRegion(anyString(), anyString(), eq(true));
    mockStore.setActiveActiveReplicationEnabled(aaEnabled);

    // Add a banned route not relevant to the test just to make sure theres coverage for unbanned routes still be
    // accessible
    AdminSparkServer server =
        ServiceFactory.getMockAdminSparkServer(admin, "clustername", Arrays.asList(ControllerRoute.ADD_DERIVED_SCHEMA));
    int port = server.getPort();

    // build request
    List<NameValuePair> params = new ArrayList<>();
    params.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, "clustername"));
    params.add(new BasicNameValuePair(ControllerApiConstants.NAME, "storename"));
    params.add(new BasicNameValuePair(ControllerApiConstants.STORE_SIZE, Long.toString(1L)));
    params.add(new BasicNameValuePair(ControllerApiConstants.PUSH_JOB_ID, "pushJobId-1234"));
    params.add(new BasicNameValuePair(ControllerApiConstants.PUSH_TYPE, Version.PushType.STREAM.toString()));
    params.add(new BasicNameValuePair(ControllerApiConstants.REPUSH_SOURCE_VERSION, "0"));
    final HttpPost post = new HttpPost("http://localhost:" + port + ControllerRoute.REQUEST_TOPIC.getPath());
    post.setEntity(new UrlEncodedFormEntity(params));

    // make request, parse response
    VersionCreationResponse responseObject;
    try (CloseableHttpAsyncClient httpClient =
        HttpClientUtils.getMinimalHttpClient(1, 1, Optional.of(SslUtils.getVeniceLocalSslFactory()))) {
      httpClient.start();
      HttpResponse response = httpClient.execute(post, null).get();
      String json = IOUtils.toString(response.getEntity().getContent());
      responseObject = ObjectMapperFactory.getInstance().readValue(json, VersionCreationResponse.class);
    }

    // verify response, note we expect same topic, "store_rt"

    if ((storePolicy && samzaPolicy) || (!storePolicy && !samzaPolicy) || aaEnabled) {
      Assert.assertFalse(responseObject.isError(), "unexpected error: " + responseObject.getError());
      Assert.assertEquals(responseObject.getKafkaTopic(), "store_rt");
    } else {
      Assert.assertTrue(responseObject.isError(), "expected error: ");
      Assert.assertEquals(responseObject.getKafkaTopic(), null);
    }
    server.stop();
  }

}
