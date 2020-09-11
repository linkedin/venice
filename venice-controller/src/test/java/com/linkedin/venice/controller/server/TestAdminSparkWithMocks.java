package com.linkedin.venice.controller.server;

import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.controllerapi.ControllerRoute;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.router.httpclient.HttpClientUtils;
import com.linkedin.venice.utils.SslUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.message.BasicNameValuePair;
import org.codehaus.jackson.map.ObjectMapper;
import org.mockito.Mockito;
import static org.mockito.Mockito.*;

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
    //setup server with mock admin, note returns topic "store_rt"
    VeniceHelixAdmin admin = Mockito.mock(VeniceHelixAdmin.class);
    Store mockStore = new Store("store", "owner", System.currentTimeMillis(), PersistenceType.IN_MEMORY, RoutingStrategy.CONSISTENT_HASH, ReadStrategy.ANY_OF_ONLINE, OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    mockStore.setHybridStoreConfig(new HybridStoreConfig(25L, 100L, HybridStoreConfig.DEFAULT_HYBRID_TIME_LAG_THRESHOLD));
    doReturn(mockStore).when(admin).getStore(anyString(), anyString());
    doReturn(true).when(admin).isMasterController(anyString());
    doReturn(1).when(admin).getReplicationFactor(anyString(), anyString());
    doReturn(1).when(admin).calculateNumberOfPartitions(anyString(), anyString(), anyLong());
    doReturn("kafka-bootstrap").when(admin).getKafkaBootstrapServers(anyBoolean());
    doReturn("store_rt").when(admin).getRealTimeTopic(anyString(), anyString());
    AdminSparkServer server = ServiceFactory.getMockAdminSparkServer(admin, "clustername");
    int port = server.getPort();

    //build request
    List<NameValuePair> params = new ArrayList<>();
    params.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, "clustername"));
    params.add(new BasicNameValuePair(ControllerApiConstants.HOSTNAME, "localhost"));
    params.add(new BasicNameValuePair(ControllerApiConstants.NAME, "storename"));
    params.add(new BasicNameValuePair(ControllerApiConstants.STORE_SIZE, Long.toString(1L)));
    params.add(new BasicNameValuePair(ControllerApiConstants.PUSH_JOB_ID, "pushJobId-1234"));
    params.add(new BasicNameValuePair(ControllerApiConstants.PUSH_TYPE, Version.PushType.STREAM.toString()));
    final HttpPost post = new HttpPost("http://localhost:" + port + ControllerRoute.REQUEST_TOPIC.getPath());
    post.setEntity(new UrlEncodedFormEntity(params));

    //make request, parse response
    VersionCreationResponse responseObject;
    try (CloseableHttpAsyncClient httpClient = HttpClientUtils.getMinimalHttpClient(1,1, Optional.of(SslUtils.getLocalSslFactory()))) {
      httpClient.start();
      HttpResponse response = httpClient.execute(post, null).get();
      String json = IOUtils.toString(response.getEntity().getContent());
      responseObject = new ObjectMapper().readValue(json, VersionCreationResponse.class);
    }

    //verify response, note we expect same topic, "store_rt"
    Assert.assertFalse(responseObject.isError(), "unexpected error: " + responseObject.getError());
    Assert.assertEquals(responseObject.getKafkaTopic(), "store_rt");

    server.stop();
  }
}
