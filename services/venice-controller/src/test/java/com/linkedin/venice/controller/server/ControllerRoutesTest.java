package com.linkedin.venice.controller.server;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.ControllerRequestHandlerDependencies;
import com.linkedin.venice.controller.InstanceRemovableStatuses;
import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import com.linkedin.venice.controllerapi.AdminOperationProtocolVersionControllerResponse;
import com.linkedin.venice.controllerapi.AggregatedHealthStatusRequest;
import com.linkedin.venice.controllerapi.ChildAwareResponse;
import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.controllerapi.LeaderControllerResponse;
import com.linkedin.venice.controllerapi.StoppableNodeStatusResponse;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import spark.Request;
import spark.Response;
import spark.Route;


public class ControllerRoutesTest {
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();
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

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    mockAdmin = mock(VeniceParentHelixAdmin.class);
    mockDependencies = mock(ControllerRequestHandlerDependencies.class);
    doReturn(mockAdmin).when(mockDependencies).getAdmin();
    requestHandler = new VeniceControllerRequestHandler(mockDependencies);
  }

  @Test
  public void testGetLeaderController() throws Exception {
    doReturn(true).when(mockAdmin).isLeaderControllerFor(anyString());
    Instance leaderController =
        new Instance(TEST_NODE_ID, TEST_HOST, TEST_PORT, TEST_SSL_PORT, TEST_GRPC_PORT, TEST_GRPC_SSL_PORT);

    doReturn(leaderController).when(mockAdmin).getLeaderController(anyString());

    Request request = mock(Request.class);
    doReturn(TEST_CLUSTER).when(request).queryParams(eq(ControllerApiConstants.CLUSTER));

    Route leaderControllerRoute = new ControllerRoutes(false, Optional.empty(), pubSubTopicRepository, requestHandler)
        .getLeaderController(mockAdmin);
    LeaderControllerResponse leaderControllerResponse = OBJECT_MAPPER.readValue(
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
    LeaderControllerResponse leaderControllerResponseSsl = OBJECT_MAPPER.readValue(
        leaderControllerSslRoute.handle(request, mock(Response.class)).toString(),
        LeaderControllerResponse.class);
    assertEquals(leaderControllerResponseSsl.getCluster(), TEST_CLUSTER);
    assertEquals(leaderControllerResponseSsl.getUrl(), "https://" + TEST_HOST + ":" + TEST_SSL_PORT);
    assertEquals(leaderControllerResponseSsl.getSecureUrl(), "https://" + TEST_HOST + ":" + TEST_SSL_PORT);
    assertEquals(leaderControllerResponse.getGrpcUrl(), TEST_HOST + ":" + TEST_GRPC_PORT);
    assertEquals(leaderControllerResponse.getSecureGrpcUrl(), TEST_HOST + ":" + TEST_GRPC_SSL_PORT);

    // Controller doesn't support SSL
    Instance leaderNonSslController =
        new Instance(TEST_NODE_ID, TEST_HOST, TEST_PORT, TEST_PORT, TEST_GRPC_PORT, TEST_GRPC_SSL_PORT);
    doReturn(leaderNonSslController).when(mockAdmin).getLeaderController(anyString());

    LeaderControllerResponse leaderControllerNonSslResponse = OBJECT_MAPPER.readValue(
        leaderControllerRoute.handle(request, mock(Response.class)).toString(),
        LeaderControllerResponse.class);
    assertEquals(leaderControllerNonSslResponse.getCluster(), TEST_CLUSTER);
    assertEquals(leaderControllerNonSslResponse.getUrl(), "http://" + TEST_HOST + ":" + TEST_PORT);
    assertEquals(leaderControllerNonSslResponse.getSecureUrl(), null);
    assertEquals(leaderControllerNonSslResponse.getGrpcUrl(), TEST_HOST + ":" + TEST_GRPC_PORT);
    assertEquals(leaderControllerNonSslResponse.getSecureGrpcUrl(), TEST_HOST + ":" + TEST_GRPC_SSL_PORT);
  }

  @Test
  public void testGetAggregatedHealthStatus() throws Exception {
    ControllerRoutes controllerRoutes =
        new ControllerRoutes(false, Optional.empty(), pubSubTopicRepository, requestHandler);
    Admin mockAdmin = mock(VeniceParentHelixAdmin.class);

    List<String> instanceList = Arrays.asList("instance1_5000", "instance2_5000");
    List<String> toBeStoppedInstanceList = Arrays.asList("instance3_5000", "instance4_5000");

    AggregatedHealthStatusRequest requestBody =
        new AggregatedHealthStatusRequest(TEST_CLUSTER, instanceList, toBeStoppedInstanceList);
    String body = OBJECT_MAPPER.writeValueAsString(requestBody);

    Request request = mock(Request.class);
    doReturn(body).when(request).body();

    // Test redirect
    InstanceRemovableStatuses redirectStatuses = new InstanceRemovableStatuses();
    redirectStatuses.setRedirectUrl("http://redirect.com");
    doReturn(redirectStatuses).when(mockAdmin)
        .getAggregatedHealthStatus(TEST_CLUSTER, instanceList, toBeStoppedInstanceList, false);
    Response redirectResponse = mock(Response.class);
    controllerRoutes.getAggregatedHealthStatus(mockAdmin).handle(request, redirectResponse);
    verify(redirectResponse).redirect("http://redirect.com/aggregatedHealthStatus", 302);

    // Test non-removable instances
    Map<String, String> nonStoppableInstances = new HashMap() {
      {
        put("instance1_5000", "reason1");
        put("instance2_5000", "reason2");
      }
    };
    InstanceRemovableStatuses nonRemovableStatus = new InstanceRemovableStatuses();
    nonRemovableStatus.setNonStoppableInstancesWithReasons(nonStoppableInstances);

    doReturn(nonRemovableStatus).when(mockAdmin)
        .getAggregatedHealthStatus(TEST_CLUSTER, instanceList, toBeStoppedInstanceList, false);
    Response nonRemovableResponse = mock(Response.class);
    StoppableNodeStatusResponse nonRemovableStoppableResponse = OBJECT_MAPPER.readValue(
        controllerRoutes.getAggregatedHealthStatus(mockAdmin).handle(request, nonRemovableResponse).toString(),
        StoppableNodeStatusResponse.class);
    assertTrue(nonRemovableStoppableResponse.getStoppableInstances().isEmpty());
    assertEquals(nonRemovableStoppableResponse.getNonStoppableInstancesWithReasons().size(), 2);
    assertEquals(nonRemovableStoppableResponse.getNonStoppableInstancesWithReasons().get("instance1_5000"), "reason1");
    assertEquals(nonRemovableStoppableResponse.getNonStoppableInstancesWithReasons().get("instance2_5000"), "reason2");

    // Test removable instances
    List<String> stoppableInstances = Arrays.asList("instance1_5000", "instance2_5000");
    InstanceRemovableStatuses removableStatus = new InstanceRemovableStatuses();
    removableStatus.setStoppableInstances(stoppableInstances);

    doReturn(removableStatus).when(mockAdmin)
        .getAggregatedHealthStatus(TEST_CLUSTER, instanceList, toBeStoppedInstanceList, false);
    Response removableResponse = mock(Response.class);
    StoppableNodeStatusResponse removableStoppableResponse = OBJECT_MAPPER.readValue(
        controllerRoutes.getAggregatedHealthStatus(mockAdmin).handle(request, removableResponse).toString(),
        StoppableNodeStatusResponse.class);
    assertTrue(removableStoppableResponse.getNonStoppableInstancesWithReasons().isEmpty());
    assertEquals(removableStoppableResponse.getStoppableInstances().size(), 2);
    assertEquals(removableStoppableResponse.getStoppableInstances().get(0), "instance1_5000");
    assertEquals(removableStoppableResponse.getStoppableInstances().get(1), "instance2_5000");

    // Test removable and non removable instances
    List<String> stoppableInstances1 = Arrays.asList("instance1_5000");
    Map<String, String> nonStoppableInstances1 = new HashMap() {
      {
        put("instance2_5000", "reason2");
      }
    };

    InstanceRemovableStatuses removableAndNonRemovableStatus = new InstanceRemovableStatuses();
    removableAndNonRemovableStatus.setStoppableInstances(stoppableInstances1);
    removableAndNonRemovableStatus.setNonStoppableInstancesWithReasons(nonStoppableInstances1);

    doReturn(removableAndNonRemovableStatus).when(mockAdmin)
        .getAggregatedHealthStatus(TEST_CLUSTER, instanceList, toBeStoppedInstanceList, false);
    Response removableAndNonRemovableResponse = mock(Response.class);
    StoppableNodeStatusResponse removableAndNonRemovableStoppableResponse = OBJECT_MAPPER.readValue(
        controllerRoutes.getAggregatedHealthStatus(mockAdmin)
            .handle(request, removableAndNonRemovableResponse)
            .toString(),
        StoppableNodeStatusResponse.class);
    assertEquals(removableAndNonRemovableStoppableResponse.getNonStoppableInstancesWithReasons().size(), 1);
    assertEquals(
        removableAndNonRemovableStoppableResponse.getNonStoppableInstancesWithReasons().get("instance2_5000"),
        "reason2");
    assertEquals(removableAndNonRemovableStoppableResponse.getStoppableInstances().size(), 1);
    assertEquals(removableAndNonRemovableStoppableResponse.getStoppableInstances().get(0), "instance1_5000");
  }

  @Test
  public void testGetLocalAdminOperationProtocolVersion() throws Exception {
    doReturn(true).when(mockAdmin).isLeaderControllerFor(anyString());
    Instance leaderController =
        new Instance(TEST_NODE_ID, TEST_HOST, TEST_PORT, TEST_SSL_PORT, TEST_GRPC_PORT, TEST_GRPC_SSL_PORT);

    doReturn(leaderController).when(mockAdmin).getLeaderController(anyString());
    String leaderControllerHost = String.format("http://%s:%s", TEST_HOST, TEST_PORT);
    doReturn(leaderControllerHost).when(mockAdmin).getControllerName();

    Request request = mock(Request.class);
    doReturn(TEST_CLUSTER).when(request).queryParams(eq(ControllerApiConstants.CLUSTER));
    doReturn(1L).when(mockAdmin).getLocalAdminOperationProtocolVersion();
    doReturn(leaderControllerHost + "/get_local_admin_operation_protocol_version").when(request).url();

    Route localAdminOperationVersionRoute =
        new ControllerRoutes(false, Optional.empty(), pubSubTopicRepository, requestHandler)
            .getLocalAdminOperationProtocolVersion(mockAdmin);
    AdminOperationProtocolVersionControllerResponse response = OBJECT_MAPPER.readValue(
        localAdminOperationVersionRoute.handle(request, mock(Response.class)).toString(),
        AdminOperationProtocolVersionControllerResponse.class);
    assertEquals(response.getLocalAdminOperationProtocolVersion(), 1L);
    assertEquals(response.getControllerNameToVersionMap().size(), 0);
  }

  @Test
  public void testGetAdminOperationVersionFromControllers() throws Exception {
    doReturn(true).when(mockAdmin).isLeaderControllerFor(anyString());
    Instance leaderController =
        new Instance(TEST_NODE_ID, TEST_HOST, TEST_PORT, TEST_SSL_PORT, TEST_GRPC_PORT, TEST_GRPC_SSL_PORT);

    doReturn(leaderController).when(mockAdmin).getLeaderController(anyString());
    String leaderControllerHostHttps = String.format("https://%s:%s", TEST_HOST, TEST_PORT);
    String leaderControllerHost = String.format("%s_%s", TEST_HOST, TEST_PORT);
    doReturn(leaderControllerHost).when(mockAdmin).getControllerName();

    Request request = mock(Request.class);
    doReturn(TEST_CLUSTER).when(request).queryParams(eq(ControllerApiConstants.CLUSTER));
    doReturn(1L).when(mockAdmin).getLocalAdminOperationProtocolVersion();
    doReturn(leaderControllerHostHttps + "/get_admin_operation_version_from_controllers").when(request).url();

    Map<String, Long> controllerNameToVersionMap = new HashMap<>();
    controllerNameToVersionMap.put("localhost_8080", 1L);
    controllerNameToVersionMap.put("localhost_8081", 2L);
    controllerNameToVersionMap.put(leaderControllerHost, 1L);
    doReturn(controllerNameToVersionMap).when(mockAdmin).getAdminOperationVersionFromControllers(anyString());

    Route adminOperationVersionRoute =
        new ControllerRoutes(false, Optional.empty(), pubSubTopicRepository, requestHandler)
            .getAdminOperationVersionFromControllers(mockAdmin);
    AdminOperationProtocolVersionControllerResponse response = OBJECT_MAPPER.readValue(
        adminOperationVersionRoute.handle(request, mock(Response.class)).toString(),
        AdminOperationProtocolVersionControllerResponse.class);
    assertEquals(response.getLocalAdminOperationProtocolVersion(), 1L);
    assertEquals(response.getControllerNameToVersionMap().size(), 3);
    assertEquals(response.getControllerNameToVersionMap(), controllerNameToVersionMap);
    assertEquals(response.getLocalControllerName(), leaderControllerHost);
    assertEquals(response.getCluster(), TEST_CLUSTER);
  }

  @Test
  public void testGetChildControllersForParentController() throws Exception {
    doReturn(true).when(mockAdmin).isLeaderControllerFor(anyString());
    doReturn(true).when(mockAdmin).isParent();

    Map<String, String> childUrlMap = new HashMap<>();
    childUrlMap.put("dc1", "http://dc1-controller:8080");
    childUrlMap.put("dc2", "http://dc2-controller:8080");

    Map<String, String> childD2Map = new HashMap<>();
    childD2Map.put("dc1", "d2://dc1-controller");
    childD2Map.put("dc2", "d2://dc2-controller");

    doReturn(childUrlMap).when(mockAdmin).getChildDataCenterControllerUrlMap(TEST_CLUSTER);
    doReturn(childD2Map).when(mockAdmin).getChildDataCenterControllerD2Map(TEST_CLUSTER);
    doReturn("VeniceController").when(mockAdmin).getChildControllerD2ServiceName(TEST_CLUSTER);

    Request request = mock(Request.class);
    doReturn(TEST_CLUSTER).when(request).queryParams(eq(ControllerApiConstants.CLUSTER));

    Route childControllersRoute = new ControllerRoutes(false, Optional.empty(), pubSubTopicRepository, requestHandler)
        .getChildControllers(mockAdmin);
    ChildAwareResponse response = OBJECT_MAPPER
        .readValue(childControllersRoute.handle(request, mock(Response.class)).toString(), ChildAwareResponse.class);

    assertEquals(response.getCluster(), TEST_CLUSTER);
    assertEquals(response.getChildDataCenterControllerUrlMap().size(), 2);
    assertEquals(response.getChildDataCenterControllerUrlMap().get("dc1"), "http://dc1-controller:8080");
    assertEquals(response.getChildDataCenterControllerUrlMap().get("dc2"), "http://dc2-controller:8080");
    assertEquals(response.getChildDataCenterControllerD2Map().size(), 2);
    assertEquals(response.getChildDataCenterControllerD2Map().get("dc1"), "d2://dc1-controller");
    assertEquals(response.getChildDataCenterControllerD2Map().get("dc2"), "d2://dc2-controller");
    assertEquals(response.getD2ServiceName(), "VeniceController");
  }

  @Test
  public void testGetChildControllersForChildController() throws Exception {
    doReturn(true).when(mockAdmin).isLeaderControllerFor(anyString());
    doReturn(false).when(mockAdmin).isParent();

    Request request = mock(Request.class);
    doReturn(TEST_CLUSTER).when(request).queryParams(eq(ControllerApiConstants.CLUSTER));

    Route childControllersRoute = new ControllerRoutes(false, Optional.empty(), pubSubTopicRepository, requestHandler)
        .getChildControllers(mockAdmin);
    ChildAwareResponse response = OBJECT_MAPPER
        .readValue(childControllersRoute.handle(request, mock(Response.class)).toString(), ChildAwareResponse.class);

    assertEquals(response.getCluster(), TEST_CLUSTER);
    assertTrue(
        response.getChildDataCenterControllerUrlMap() == null
            || response.getChildDataCenterControllerUrlMap().isEmpty());
    assertTrue(
        response.getChildDataCenterControllerD2Map() == null || response.getChildDataCenterControllerD2Map().isEmpty());
    assertTrue(response.getD2ServiceName() == null);
  }
}
