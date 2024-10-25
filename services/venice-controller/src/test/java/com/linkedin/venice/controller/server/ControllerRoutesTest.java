package com.linkedin.venice.controller.server;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.ControllerRequestHandlerDependencies;
import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.controllerapi.LeaderControllerResponse;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.util.Optional;
import org.testng.annotations.BeforeMethod;
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
  private static final int TEST_GRPC_PORT = 2183;
  private static final int TEST_GRPC_SSL_PORT = 2184;

  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  private VeniceControllerRequestHandler requestHandler;
  private ControllerRequestHandlerDependencies mockDependencies;
  private Admin mockAdmin;

  @BeforeMethod
  public void setUp() {
    mockAdmin = mock(VeniceParentHelixAdmin.class);
    mockDependencies = mock(ControllerRequestHandlerDependencies.class);
    doReturn(mockAdmin).when(mockDependencies).getAdmin();
  }

  @Test
  public void testGetLeaderController() throws Exception {
    doReturn(true).when(mockAdmin).isLeaderControllerFor(anyString());
    Instance leaderController =
        new Instance(TEST_NODE_ID, TEST_HOST, TEST_PORT, TEST_SSL_PORT, TEST_GRPC_PORT, TEST_GRPC_SSL_PORT);
    doReturn(leaderController).when(mockAdmin).getLeaderController(anyString());

    Request request = mock(Request.class);
    doReturn(TEST_CLUSTER).when(request).queryParams(eq(ControllerApiConstants.CLUSTER));
    when(mockDependencies.isSslEnabled()).thenReturn(false);
    requestHandler = new VeniceControllerRequestHandler(mockDependencies);
    Route leaderControllerRoute = new ControllerRoutes(false, Optional.empty(), pubSubTopicRepository, requestHandler)
        .getLeaderController(mockAdmin);
    LeaderControllerResponse leaderControllerResponse = ObjectMapperFactory.getInstance()
        .readValue(
            leaderControllerRoute.handle(request, mock(Response.class)).toString(),
            LeaderControllerResponse.class);
    assertEquals(leaderControllerResponse.getCluster(), TEST_CLUSTER);
    assertEquals(leaderControllerResponse.getUrl(), "http://" + TEST_HOST + ":" + TEST_PORT);
    assertEquals(leaderControllerResponse.getSecureUrl(), "https://" + TEST_HOST + ":" + TEST_SSL_PORT);
    assertEquals(leaderControllerResponse.getGrpcUrl(), TEST_HOST + ":" + TEST_GRPC_PORT);
    assertEquals(leaderControllerResponse.getSecureGrpcUrl(), TEST_HOST + ":" + TEST_GRPC_SSL_PORT);

    when(mockDependencies.isSslEnabled()).thenReturn(true);
    requestHandler = new VeniceControllerRequestHandler(mockDependencies);
    Route leaderControllerSslRoute = new ControllerRoutes(true, Optional.empty(), pubSubTopicRepository, requestHandler)
        .getLeaderController(mockAdmin);
    LeaderControllerResponse leaderControllerResponseSsl = ObjectMapperFactory.getInstance()
        .readValue(
            leaderControllerSslRoute.handle(request, mock(Response.class)).toString(),
            LeaderControllerResponse.class);
    assertEquals(leaderControllerResponseSsl.getCluster(), TEST_CLUSTER);
    assertEquals(leaderControllerResponseSsl.getUrl(), "https://" + TEST_HOST + ":" + TEST_SSL_PORT);
    assertEquals(leaderControllerResponseSsl.getSecureUrl(), "https://" + TEST_HOST + ":" + TEST_SSL_PORT);
    assertEquals(leaderControllerResponseSsl.getGrpcUrl(), TEST_HOST + ":" + TEST_GRPC_PORT);
    assertEquals(leaderControllerResponseSsl.getSecureGrpcUrl(), TEST_HOST + ":" + TEST_GRPC_SSL_PORT);

    // Controller doesn't support SSL
    Instance leaderNonSslController =
        new Instance(TEST_NODE_ID, TEST_HOST, TEST_PORT, TEST_PORT, TEST_GRPC_PORT, TEST_GRPC_SSL_PORT);
    doReturn(leaderNonSslController).when(mockAdmin).getLeaderController(anyString());

    LeaderControllerResponse leaderControllerNonSslResponse = ObjectMapperFactory.getInstance()
        .readValue(
            leaderControllerRoute.handle(request, mock(Response.class)).toString(),
            LeaderControllerResponse.class);
    assertEquals(leaderControllerNonSslResponse.getCluster(), TEST_CLUSTER);
    assertEquals(leaderControllerNonSslResponse.getUrl(), "http://" + TEST_HOST + ":" + TEST_PORT);
    assertEquals(leaderControllerNonSslResponse.getSecureUrl(), null);
    assertEquals(leaderControllerNonSslResponse.getGrpcUrl(), TEST_HOST + ":" + TEST_GRPC_PORT);
    assertEquals(leaderControllerNonSslResponse.getSecureGrpcUrl(), TEST_HOST + ":" + TEST_GRPC_SSL_PORT);
  }
}
