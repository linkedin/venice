package com.linkedin.venice.controller.server;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiStoreResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.Version;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Separate the tests from {@link TestAdminSparkServer}, because we need start more storage nodes to execute the
 * resource with bunch of partitions.
 */
public class TestAdminSparkServerWithMultiServers {
  private VeniceClusterWrapper venice;
  private String routerUrl;
  private int numberOfServer = 3;

  @BeforeClass
  public void setUp() {
    venice = ServiceFactory.getVeniceCluster(numberOfServer);
    routerUrl = "http://" + venice.getVeniceRouter().getAddress();
  }

  @AfterClass
  public void tearDown() {
    venice.close();
  }

  @Test
  public void controllerClientShouldListStores() {

    List<String> storeNames = new ArrayList<>();
    for (int i = 0; i < 10; i++) { //add 10 stores;
      storeNames.add(Version.parseStoreFromKafkaTopicName(venice.getNewStoreVersion().getKafkaTopic()));
    }

    MultiStoreResponse storeResponse = ControllerClient.queryStoreList(routerUrl, venice.getClusterName());
    Assert.assertFalse(storeResponse.isError());
    List<String> returnedStoreNames = Arrays.asList(storeResponse.getStores());
    for (String expectedStore : storeNames) {
      Assert.assertTrue(returnedStoreNames.contains(expectedStore), "Query store list should include " + expectedStore);
    }
  }
}
