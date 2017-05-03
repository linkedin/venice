package com.linkedin.venice.controller.server;

import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiStoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.FlakyTestRetryAnalyzer;
import com.linkedin.venice.utils.TestUtils;
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
  private final int numberOfServer = 4;

  @BeforeClass
  public void setUp() {
    venice = ServiceFactory.getVeniceCluster(1, numberOfServer, 1);
    routerUrl = venice.getRandomRouterURL();
  }

  @AfterClass
  public void tearDown() {
    venice.close();
  }

  /**
   * TODO: This test should be fixed. It is flaky, especially on a slow or heavily loaded machine.
   */
  @Test(retryAnalyzer = FlakyTestRetryAnalyzer.class)
  public void controllerClientShouldListStores() {
    List<String> storeNames = new ArrayList<>();
    for (int i = 0; i < 10; i++) { //add 10 stores;
      storeNames.add(Version.parseStoreFromKafkaTopicName(venice.getNewStoreVersion().getKafkaTopic()));
    }

    MultiStoreResponse storeResponse = ControllerClient.listStores(routerUrl, venice.getClusterName());
    Assert.assertFalse(storeResponse.isError());
    List<String> returnedStoreNames = Arrays.asList(storeResponse.getStores());
    for (String expectedStore : storeNames) {
      Assert.assertTrue(returnedStoreNames.contains(expectedStore), "Query store list should include " + expectedStore);
    }
  }


  @Test
  public void requestTopicIsIdempotent() {
    String storeName = TestUtils.getUniqueString("store");
    String pushOne = TestUtils.getUniqueString("pushId");
    String pushTwo = TestUtils.getUniqueString("pushId");

    ControllerClient client = new ControllerClient(venice.getClusterName(), routerUrl);

    venice.getNewStore(storeName);
    VersionCreationResponse
        responseOneA = client.requestTopicForWrites(storeName, 1, ControllerApiConstants.PushType.BATCH, pushOne);
    if (responseOneA.isError()){
      Assert.fail(responseOneA.getError());
    }
    VersionCreationResponse responseOneB = client.requestTopicForWrites(storeName, 1, ControllerApiConstants.PushType.BATCH, pushOne);
    if (responseOneB.isError()){
      Assert.fail(responseOneA.getError());
    }
    VersionCreationResponse responseTwo = client.requestTopicForWrites(storeName, 1, ControllerApiConstants.PushType.BATCH, pushTwo);
    if (responseTwo.isError()){
      Assert.fail(responseOneA.getError());
    }

    Assert.assertEquals(responseOneA.getKafkaTopic(), responseOneB.getKafkaTopic(), "Multiple requests for topics with the same pushId must return the same kafka topic");
    Assert.assertNotEquals(responseOneA.getKafkaTopic(), responseTwo.getKafkaTopic(), "Multiple requests for topics with different pushIds must return different topics");
  }

  @Test
  public void requestTopicIsIdempotentWithConcurrency() throws InterruptedException {
    String storeName = TestUtils.getUniqueString("store");
    venice.getNewStore(storeName);
    for (int i=0;i<10;i++){
      String pushId = TestUtils.getUniqueString("pushId");
      List<VersionCreationResponse> responses = new ArrayList<>();
      Thread t1 = requestTopicThread(pushId, storeName, responses);
      Thread t2 = requestTopicThread(pushId, storeName, responses);
      t1.start();
      t2.start();
      t1.join();
      t2.join();
      for (int j=0;j<responses.size();j++) {
        if (responses.get(j).isError()) {
          Assert.fail(responses.get(j).getError());
        }
      }
      Assert.assertEquals(responses.get(0).getKafkaTopic(), responses.get(1).getKafkaTopic(),
          "Idempotent topic requests failed under concurrency.  If this test ever fails, investigate! Don't just run it again and hope it passes");
    }
  }

  private Thread requestTopicThread(String pushId, String storeName, List<VersionCreationResponse> output) {
    return new Thread(() -> {
      ControllerClient client = new ControllerClient(venice.getClusterName(), routerUrl);
      VersionCreationResponse vcr = client.requestTopicForWrites(storeName, 1, ControllerApiConstants.PushType.BATCH, pushId);
      output.add(vcr);
      client.close();
    });
  }
}
