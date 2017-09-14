package com.linkedin.venice.controller.server;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.VeniceControllerConfig;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.message.BasicNameValuePair;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.CREATE_VERSION;
import static com.linkedin.venice.controllerapi.ControllerRoute.JOB;

public class TestBackupControllerResponse {
  @Test
  public void backupControllerThrows421()
      throws Exception {

    String clusterName = "backupControllerThrows421";
    KafkaBrokerWrapper kafka = ServiceFactory.getKafkaBroker();
    String kafkaBootstrap = kafka.getHost() + ":" + kafka.getPort();


    VeniceHelixAdmin veniceAdmin; /* First controller, comes up as active controller */
    int bogusPort = 21; /* nothing runs here, doesn't need to bind.since not launching AdminSparkServer */
    String controllerName = Utils.getHelixNodeIdentifier(bogusPort);
    VeniceProperties controllerOneProps = new PropertyBuilder()
        .put(ENABLE_TOPIC_REPLICATOR, false)
        .put(KAFKA_ZK_ADDRESS, kafka.getZkAddress())
        .put(KAFKA_REPLICA_FACTOR, 1)
        .put(DEFAULT_REPLICA_FACTOR, 1)
        .put(DEFAULT_NUMBER_OF_PARTITION, 3)
        .put(DEFAULT_MAX_NUMBER_OF_PARTITIONS, 10)
        .put(DEFAULT_PARTITION_SIZE, 100)
        .put(KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrap)
        .put(ZOOKEEPER_ADDRESS, kafka.getZkAddress())
        .put(CLUSTER_NAME, clusterName)
        .put(CONTROLLER_NAME, controllerName)
        .put(ConfigKeys.ADMIN_PORT, bogusPort)
        .put(CLUSTER_TO_D2, TestUtils.getClusterToDefaultD2String(clusterName))
        .build();
    VeniceControllerConfig config = new VeniceControllerConfig(controllerOneProps);
    veniceAdmin = new VeniceHelixAdmin(TestUtils.getMultiClusterConfigFromOneCluster(config), new MetricsRepository());
    veniceAdmin.start(clusterName);

    /* ControllerWrapper is second controller, comes up as inactive standby controller */
    VeniceControllerWrapper controller = ServiceFactory.getVeniceController(clusterName, kafka);
    Thread.sleep(2000);
    CloseableHttpAsyncClient httpClient = HttpAsyncClients.createDefault();
    httpClient.start();
    String controllerUrl = controller.getControllerUrl();

    final HttpPost post = new HttpPost(controllerUrl + CREATE_VERSION.getPath());
    List<NameValuePair> params = new ArrayList<NameValuePair>();
    params.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, clusterName));
    post.setEntity(new UrlEncodedFormEntity(params));

    HttpResponse response = httpClient.execute(post, null).get();
    int responseStatus = response.getStatusLine().getStatusCode();
    Assert.assertEquals(responseStatus, HttpConstants.SC_MISDIRECTED_REQUEST);

    List<NameValuePair> queryParams = new ArrayList<>();
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, clusterName));
    String queryString = URLEncodedUtils.format(queryParams, StandardCharsets.UTF_8);
    final HttpGet get = new HttpGet(controllerUrl + JOB.getPath() + "?" + queryString);
    HttpResponse response2 = httpClient.execute(get, null).get();

    int responseStatus2 = response2.getStatusLine().getStatusCode();
    Assert.assertEquals(responseStatus2, HttpConstants.SC_MISDIRECTED_REQUEST);

    httpClient.close();
    controller.close();
    veniceAdmin.close();
    kafka.close();
  }
}
