package com.linkedin.venice.controller.server;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.VeniceControllerClusterConfig;
import com.linkedin.venice.controller.VeniceControllerConfig;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.VersionResponse;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
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


/**
 * Created by mwise on 4/20/16.
 */
public class TestAdminSparkServer {

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
        .put(VeniceControllerClusterConfig.KAFKA_ZK_ADDRESS, kafka.getZkAddress())
        .put(VeniceControllerClusterConfig.KAFKA_REPLICA_FACTOR, 1)
        .put(VeniceControllerClusterConfig.REPLICA_FACTOR, 1)
        .put(VeniceControllerClusterConfig.NUMBER_OF_PARTITION, 3)
        .put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrap)
        .put(ConfigKeys.ZOOKEEPER_ADDRESS, kafka.getZkAddress())
        .put(ConfigKeys.CLUSTER_NAME, clusterName)
        .put(VeniceControllerClusterConfig.CONTROLLER_NAME, controllerName)
        .put(ConfigKeys.ADMIN_PORT, bogusPort)
        .build();
    VeniceControllerConfig config = new VeniceControllerConfig(controllerOneProps);
    veniceAdmin = new VeniceHelixAdmin(config);
    veniceAdmin.start(clusterName);

    /* ControllerWrapper is second controller, comes up as inactive standby controller */
    VeniceControllerWrapper controller = ServiceFactory.getVeniceController(clusterName, kafka);
    Thread.sleep(2000);
    CloseableHttpAsyncClient httpClient = HttpAsyncClients.createDefault();
    httpClient.start();
    String controllerUrl = controller.getControllerUrl();

    final HttpPost post = new HttpPost(controllerUrl + ControllerApiConstants.CREATE_PATH);
    List<NameValuePair> params = new ArrayList<NameValuePair>();
    params.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, clusterName));
    post.setEntity(new UrlEncodedFormEntity(params));

    HttpResponse response = httpClient.execute(post, null).get();
    int responseStatus = response.getStatusLine().getStatusCode();
    Assert.assertEquals(responseStatus, HttpConstants.SC_MISDIRECTED_REQUEST);

    List<NameValuePair> queryParams = new ArrayList<>();
    queryParams.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, clusterName));
    String queryString = URLEncodedUtils.format(queryParams, StandardCharsets.UTF_8);
    final HttpGet get = new HttpGet(controllerUrl + ControllerApiConstants.JOB_PATH + "?" + queryString);
    HttpResponse response2 = httpClient.execute(get, null).get();

    int responseStatus2 = response2.getStatusLine().getStatusCode();
    Assert.assertEquals(responseStatus2, HttpConstants.SC_MISDIRECTED_REQUEST);

    httpClient.close();
    controller.close();
    veniceAdmin.close();
    kafka.close();
  }

  @Test
  public void controllerClientCanQueryNextVersion()
      throws InterruptedException {
    VeniceClusterWrapper venice = ServiceFactory.getVeniceCluster();

    String routerUrl = "http://" + venice.getVeniceRouter().getAddress();
    String kafkaTopic = venice.getNewStoreVersion();
    String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopic);
    String storeToCreate = "does-not-yet-exist-"+storeName;
    int currentVersion = Version.parseVersionFromKafkaTopicName(kafkaTopic);

    // queryNextVersion
    VersionResponse nextVersionResponse = ControllerClient.queryNextVersion(routerUrl, venice.getClusterName(),
        storeName);
    Assert.assertEquals(nextVersionResponse.getVersion(), currentVersion + 1);
    VersionResponse badVersionResponse = ControllerClient.queryNextVersion(routerUrl, venice.getClusterName(),
        storeToCreate);
    Assert.assertTrue(badVersionResponse.isError());

    // reserveVersion
    VersionResponse badReservation = ControllerClient.reserveVersion(routerUrl, venice.getClusterName(), storeName,
        currentVersion);
    Assert.assertTrue(badReservation.isError(), "controller client should not allow reservation of current version");
    int reserveVersion = currentVersion + 1;
    VersionResponse goodReservation = ControllerClient.reserveVersion(routerUrl, venice.getClusterName(), storeName, reserveVersion);
    Assert.assertFalse(goodReservation.isError(), "should be able to reserve next version");

    VersionResponse afterReservationPeek = ControllerClient.queryNextVersion(routerUrl, venice.getClusterName(),
        storeName);
    Assert.assertEquals(afterReservationPeek.getVersion(), goodReservation.getVersion() + 1);
    VersionResponse doubleReservation = ControllerClient.reserveVersion(routerUrl, venice.getClusterName(), storeName,
        reserveVersion);
    Assert.assertTrue(doubleReservation.isError(), "controller client should not allow duplicate version reservation");

    // create Store
    NewStoreResponse newStoreResponse = ControllerClient.createNewStore(routerUrl, venice.getClusterName(),
        storeToCreate, "owner");
    Assert.assertFalse(newStoreResponse.isError(), "create new store shoulld succeed for a store that doesn't exist");
    VersionResponse newVersionResponse = ControllerClient.queryNextVersion(routerUrl, venice.getClusterName(),
        storeToCreate);
    Assert.assertFalse(newVersionResponse.isError());
    NewStoreResponse duplicateNewStoreResponse = ControllerClient.createNewStore(routerUrl, venice.getClusterName(),
        storeToCreate, "owner");
    Assert.assertTrue(duplicateNewStoreResponse.isError(),
        "create new store should fail for duplicate store creation");

    venice.close();
  }

}
