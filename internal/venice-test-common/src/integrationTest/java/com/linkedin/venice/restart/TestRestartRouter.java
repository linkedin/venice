package com.linkedin.venice.restart;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test(singleThreaded = true)
public class TestRestartRouter {
  private VeniceClusterWrapper cluster;

  @BeforeClass
  public void setUp() {
    int numberOfController = 1;
    int numberOfServer = 1;
    int numberOfRouter = 2;

    cluster = ServiceFactory.getVeniceCluster(numberOfController, numberOfServer, numberOfRouter);
  }

  @AfterClass
  public void cleanUp() {
    cluster.close();
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testRestartRouter() {
    String storeName = Utils.getUniqueString("testRestartRouter");
    String storeOwner = Utils.getUniqueString("store-owner");
    String keySchema = "\"string\"";
    String valueSchema = "\"string\"";
    VeniceRouterWrapper routerWrapper = cluster.getRandomVeniceRouter();
    ControllerClient controllerClient =
        new ControllerClient(cluster.getClusterName(), "http://" + routerWrapper.getAddress());
    NewStoreResponse storeResponse = controllerClient.createNewStore(storeName, storeOwner, keySchema, valueSchema);
    Assert.assertFalse(storeResponse.isError());

    // stop the selected router
    cluster.stopVeniceRouter(routerWrapper.getPort());

    VersionCreationResponse versionCreationResponse = controllerClient.requestTopicForWrites(
        storeName,
        100,
        Version.PushType.BATCH,
        Version.guidBasedDummyPushId(),
        true,
        true,
        false,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        false,
        -1);
    Assert.assertTrue(
        versionCreationResponse.isError(),
        "Router has already been shutdown, should not handle the request.");

    // Choose another router to handle request. Cluster will help us to find another running router and send the request
    // to.
    VersionCreationResponse response = cluster.getNewVersion(storeName);
    int versionNum = response.getVersion();
    Assert.assertEquals(versionNum, 1);
    // restart
    cluster.restartVeniceRouter(routerWrapper.getPort());
    // wait unit find the leader controller.(After restart, it need some time to read data from zk.)
    TestUtils.waitForNonDeterministicCompletion(3, TimeUnit.SECONDS, () -> {
      RoutingDataRepository repository = routerWrapper.getRoutingDataRepository();
      try {
        repository.getLeaderController();
        return true;
      } catch (VeniceException e) {
        return false;
      }
    });

    // The restarted router could continue to handle request.
    response = controllerClient.requestTopicForWrites(
        storeName,
        100,
        Version.PushType.BATCH,
        Version.guidBasedDummyPushId(),
        true,
        true,
        false,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        false,
        -1);
    Assert.assertFalse(response.isError());
    Assert.assertEquals(response.getVersion(), versionNum + 1);

  }
}
