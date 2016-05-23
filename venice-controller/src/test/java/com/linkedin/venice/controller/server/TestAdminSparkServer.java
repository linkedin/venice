package com.linkedin.venice.controller.server;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.VeniceControllerClusterConfig;
import com.linkedin.venice.controller.VeniceControllerConfig;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiStoreResponse;
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
import java.util.Arrays;
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
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Created by mwise on 4/20/16.
 */
public class TestAdminSparkServer {

  private VeniceClusterWrapper venice;
  private String routerUrl;



  @BeforeClass
  public void setUp(){
    venice = ServiceFactory.getVeniceCluster();
    routerUrl = "http://" + venice.getVeniceRouter().getAddress();
  }

  @AfterClass
  public void tearDown(){
    venice.close();
  }

  @Test
  public void controllerClientCanQueryNextVersion() throws InterruptedException {
    String kafkaTopic = venice.getNewStoreVersion();
    String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopic);
    int currentVersion = Version.parseVersionFromKafkaTopicName(kafkaTopic);

    VersionResponse nextVersionResponse =
        ControllerClient.queryNextVersion(routerUrl, venice.getClusterName(), storeName);
    Assert.assertEquals(nextVersionResponse.getVersion(), currentVersion + 1);

    VersionResponse badVersionResponse =
        ControllerClient.queryNextVersion(routerUrl, venice.getClusterName(), "does-not-exist-"+storeName);
    Assert.assertTrue(badVersionResponse.isError());
  }

  @Test
  public void controllerClientCanReserverVersions() {
    String kafkaTopic = venice.getNewStoreVersion();
    String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopic);
    int currentVersion = Version.parseVersionFromKafkaTopicName(kafkaTopic);

    VersionResponse badReservation =
        ControllerClient.reserveVersion(routerUrl, venice.getClusterName(), storeName, currentVersion);
    Assert.assertTrue(badReservation.isError(), "controller client should not allow reservation of current version");
    int reserveVersion = currentVersion + 1;
    VersionResponse goodReservation =
        ControllerClient.reserveVersion(routerUrl, venice.getClusterName(), storeName, reserveVersion);
    Assert.assertFalse(goodReservation.isError(), "should be able to reserve next version");

    VersionResponse afterReservationPeek =
        ControllerClient.queryNextVersion(routerUrl, venice.getClusterName(), storeName);
    Assert.assertEquals(afterReservationPeek.getVersion(), goodReservation.getVersion() + 1);
    VersionResponse doubleReservation =
        ControllerClient.reserveVersion(routerUrl, venice.getClusterName(), storeName, reserveVersion);
    Assert.assertTrue(doubleReservation.isError(), "controller client should not allow duplicate version reservation");
  }

  @Test
  public void controllerClientCanCreateNewStore(){
    String storeToCreate = "newTestStore123";
    // create Store
    NewStoreResponse newStoreResponse = ControllerClient.createNewStore(routerUrl, venice.getClusterName(),
        storeToCreate, "owner");
    Assert.assertFalse(newStoreResponse.isError(), "create new store should succeed for a store that doesn't exist");
    VersionResponse newVersionResponse = ControllerClient.queryNextVersion(routerUrl, venice.getClusterName(),
        storeToCreate);
    Assert.assertFalse(newVersionResponse.isError());
    NewStoreResponse duplicateNewStoreResponse = ControllerClient.createNewStore(routerUrl, venice.getClusterName(),
        storeToCreate, "owner");
    Assert.assertTrue(duplicateNewStoreResponse.isError(), "create new store should fail for duplicate store creation");
  }

  @Test
  public void controllerClientShouldListStores(){
    List<String> storeNames = new ArrayList<>();
    for (int i=0; i<10; i++){ //add 10 stores;
      storeNames.add(Version.parseStoreFromKafkaTopicName(venice.getNewStoreVersion()));
    }

    MultiStoreResponse storeResponse = ControllerClient.queryStoreList(routerUrl, venice.getClusterName());
    Assert.assertFalse(storeResponse.isError());
    List<String> returnedStoreNames = Arrays.asList(storeResponse.getStores());
    for (String expectedStore : storeNames){
      Assert.assertTrue(returnedStoreNames.contains(expectedStore), "Query store list should include " + expectedStore);
    }
  }

}
