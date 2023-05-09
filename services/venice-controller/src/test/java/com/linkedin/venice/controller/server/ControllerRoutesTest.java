package com.linkedin.venice.controller.server;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.controllerapi.LeaderControllerResponse;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.util.Optional;
import org.testng.Assert;
import org.testng.annotations.Test;
import spark.Request;
import spark.Response;
import spark.Route;


public class ControllerRoutesTest {
  private static final String TEST_CLUSTER = "test_cluster";
  private static final String TEST_NODE_ID = "l2181";
  private static final String TEST_HOST = "localhost";
  private static final int TEST_PORT = 2181;
  private static final int TEST_SSL_PORT = 2182;

  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @Test
  public void testGetLeaderController() throws Exception {
    Admin mockAdmin = mock(VeniceParentHelixAdmin.class);
    doReturn(true).when(mockAdmin).isLeaderControllerFor(anyString());
    Instance leaderController = new Instance(TEST_NODE_ID, TEST_HOST, TEST_PORT, TEST_SSL_PORT);
    doReturn(leaderController).when(mockAdmin).getLeaderController(anyString());

    Request request = mock(Request.class);
    doReturn(TEST_CLUSTER).when(request).queryParams(eq(ControllerApiConstants.CLUSTER));

    Route leaderControllerRoute =
        new ControllerRoutes(false, Optional.empty(), pubSubTopicRepository, Optional.empty(), Optional.empty())
            .getLeaderController(mockAdmin);
    LeaderControllerResponse leaderControllerResponse = ObjectMapperFactory.getInstance()
        .readValue(
            leaderControllerRoute.handle(request, mock(Response.class)).toString(),
            LeaderControllerResponse.class);
    Assert.assertEquals(leaderControllerResponse.getCluster(), TEST_CLUSTER);
    Assert.assertEquals(leaderControllerResponse.getUrl(), "http://" + TEST_HOST + ":" + TEST_PORT);
    Assert.assertEquals(leaderControllerResponse.getSecureUrl(), "https://" + TEST_HOST + ":" + TEST_SSL_PORT);

    Route leaderControllerSslRoute =
        new ControllerRoutes(true, Optional.empty(), pubSubTopicRepository, Optional.empty(), Optional.empty())
            .getLeaderController(mockAdmin);
    LeaderControllerResponse leaderControllerResponseSsl = ObjectMapperFactory.getInstance()
        .readValue(
            leaderControllerSslRoute.handle(request, mock(Response.class)).toString(),
            LeaderControllerResponse.class);
    Assert.assertEquals(leaderControllerResponseSsl.getCluster(), TEST_CLUSTER);
    Assert.assertEquals(leaderControllerResponseSsl.getUrl(), "https://" + TEST_HOST + ":" + TEST_SSL_PORT);
    Assert.assertEquals(leaderControllerResponseSsl.getSecureUrl(), "https://" + TEST_HOST + ":" + TEST_SSL_PORT);

    // Controller doesn't support SSL
    Instance leaderNonSslController = new Instance(TEST_NODE_ID, TEST_HOST, TEST_PORT, TEST_PORT);
    doReturn(leaderNonSslController).when(mockAdmin).getLeaderController(anyString());

    LeaderControllerResponse leaderControllerNonSslResponse = ObjectMapperFactory.getInstance()
        .readValue(
            leaderControllerRoute.handle(request, mock(Response.class)).toString(),
            LeaderControllerResponse.class);
    Assert.assertEquals(leaderControllerNonSslResponse.getCluster(), TEST_CLUSTER);
    Assert.assertEquals(leaderControllerNonSslResponse.getUrl(), "http://" + TEST_HOST + ":" + TEST_PORT);
    Assert.assertEquals(leaderControllerNonSslResponse.getSecureUrl(), null);
  }
}
