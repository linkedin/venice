package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.VERSION;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.protocols.controller.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.controller.GetJobStatusGrpcRequest;
import com.linkedin.venice.protocols.controller.GetJobStatusGrpcResponse;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.testng.Assert;
import org.testng.annotations.Test;
import spark.QueryParamsMap;
import spark.Request;
import spark.Response;
import spark.Route;


public class JobRoutesTest {
  private static final String TEST_CLUSTER = "test-cluster";
  private static final String TEST_STORE = "test-store";
  private static final String TEST_VERSION = "1";

  @Test
  public void testJobStatusReturnsSuccessfulResponse() throws Exception {
    Admin mockAdmin = mock(VeniceParentHelixAdmin.class);
    JobRequestHandler mockRequestHandler = mock(JobRequestHandler.class);
    doReturn(true).when(mockAdmin).isLeaderControllerFor(TEST_CLUSTER);

    Request request = mock(Request.class);
    doReturn(TEST_CLUSTER).when(request).queryParams(eq(CLUSTER));
    doReturn(TEST_STORE).when(request).queryParams(eq(NAME));
    doReturn(TEST_VERSION).when(request).queryParams(eq(VERSION));

    QueryParamsMap queryParamsMap = mock(QueryParamsMap.class);
    Map<String, String[]> queryMap = new HashMap<>();
    queryMap.put(CLUSTER, new String[] { TEST_CLUSTER });
    queryMap.put(NAME, new String[] { TEST_STORE });
    queryMap.put(VERSION, new String[] { TEST_VERSION });
    doReturn(queryMap).when(queryParamsMap).toMap();
    doReturn(queryParamsMap).when(request).queryMap();

    Route route = new JobRoutes(false, Optional.empty(), mockRequestHandler).jobStatus(mockAdmin);

    GetJobStatusGrpcResponse grpcResponse = GetJobStatusGrpcResponse.newBuilder()
        .setStoreInfo(ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build())
        .setVersion(1)
        .setStatus(ExecutionStatus.COMPLETED.toString())
        .build();
    when(mockRequestHandler.getJobStatus(any(GetJobStatusGrpcRequest.class))).thenReturn(grpcResponse);

    JobStatusQueryResponse response = ObjectMapperFactory.getInstance()
        .readValue(route.handle(request, mock(Response.class)).toString(), JobStatusQueryResponse.class);

    Assert.assertFalse(response.isError());
    Assert.assertEquals(response.getCluster(), TEST_CLUSTER);
    Assert.assertEquals(response.getName(), TEST_STORE);
    Assert.assertEquals(response.getVersion(), 1);
    Assert.assertEquals(response.getStatus(), ExecutionStatus.COMPLETED.toString());
  }

  @Test
  public void testJobStatusReturnsStartedStatus() throws Exception {
    Admin mockAdmin = mock(VeniceParentHelixAdmin.class);
    JobRequestHandler mockRequestHandler = mock(JobRequestHandler.class);
    doReturn(true).when(mockAdmin).isLeaderControllerFor(TEST_CLUSTER);

    Request request = mock(Request.class);
    doReturn(TEST_CLUSTER).when(request).queryParams(eq(CLUSTER));
    doReturn(TEST_STORE).when(request).queryParams(eq(NAME));
    doReturn(TEST_VERSION).when(request).queryParams(eq(VERSION));

    QueryParamsMap queryParamsMap = mock(QueryParamsMap.class);
    Map<String, String[]> queryMap = new HashMap<>();
    queryMap.put(CLUSTER, new String[] { TEST_CLUSTER });
    queryMap.put(NAME, new String[] { TEST_STORE });
    queryMap.put(VERSION, new String[] { TEST_VERSION });
    doReturn(queryMap).when(queryParamsMap).toMap();
    doReturn(queryParamsMap).when(request).queryMap();

    Route route = new JobRoutes(false, Optional.empty(), mockRequestHandler).jobStatus(mockAdmin);

    Map<String, String> extraInfo = new HashMap<>();
    extraInfo.put("dc-0", "STARTED");
    extraInfo.put("dc-1", "COMPLETED");

    GetJobStatusGrpcResponse grpcResponse = GetJobStatusGrpcResponse.newBuilder()
        .setStoreInfo(ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build())
        .setVersion(1)
        .setStatus(ExecutionStatus.STARTED.toString())
        .setStatusDetails("Push in progress")
        .setStatusUpdateTimestamp(1234567890L)
        .putAllExtraInfo(extraInfo)
        .build();
    when(mockRequestHandler.getJobStatus(any(GetJobStatusGrpcRequest.class))).thenReturn(grpcResponse);

    JobStatusQueryResponse response = ObjectMapperFactory.getInstance()
        .readValue(route.handle(request, mock(Response.class)).toString(), JobStatusQueryResponse.class);

    Assert.assertFalse(response.isError());
    Assert.assertEquals(response.getStatus(), ExecutionStatus.STARTED.toString());
    Assert.assertEquals(response.getOptionalStatusDetails().get(), "Push in progress");
    Assert.assertEquals(response.getStatusUpdateTimestamp(), Long.valueOf(1234567890L));
    Assert.assertEquals(response.getExtraInfo().get("dc-0"), "STARTED");
    Assert.assertEquals(response.getExtraInfo().get("dc-1"), "COMPLETED");
  }

  @Test
  public void testJobStatusReturnsErrorResponse() throws Exception {
    Admin mockAdmin = mock(VeniceParentHelixAdmin.class);
    JobRequestHandler mockRequestHandler = mock(JobRequestHandler.class);
    doReturn(true).when(mockAdmin).isLeaderControllerFor(TEST_CLUSTER);

    Request request = mock(Request.class);
    doReturn(TEST_CLUSTER).when(request).queryParams(eq(CLUSTER));
    doReturn(TEST_STORE).when(request).queryParams(eq(NAME));
    doReturn(TEST_VERSION).when(request).queryParams(eq(VERSION));

    QueryParamsMap queryParamsMap = mock(QueryParamsMap.class);
    Map<String, String[]> queryMap = new HashMap<>();
    queryMap.put(CLUSTER, new String[] { TEST_CLUSTER });
    queryMap.put(NAME, new String[] { TEST_STORE });
    queryMap.put(VERSION, new String[] { TEST_VERSION });
    doReturn(queryMap).when(queryParamsMap).toMap();
    doReturn(queryParamsMap).when(request).queryMap();

    Route route = new JobRoutes(false, Optional.empty(), mockRequestHandler).jobStatus(mockAdmin);

    doThrow(new VeniceException("Failed to get job status")).when(mockRequestHandler)
        .getJobStatus(any(GetJobStatusGrpcRequest.class));

    JobStatusQueryResponse response = ObjectMapperFactory.getInstance()
        .readValue(route.handle(request, mock(Response.class)).toString(), JobStatusQueryResponse.class);

    Assert.assertTrue(response.isError());
    Assert.assertTrue(response.getError().contains("Failed to get job status"));
  }

  @Test
  public void testMapGrpcResponseToHttp() {
    JobRequestHandler mockRequestHandler = mock(JobRequestHandler.class);
    JobRoutes jobRoutes = new JobRoutes(false, Optional.empty(), mockRequestHandler);

    Map<String, String> extraInfo = new HashMap<>();
    extraInfo.put("dc-0", "COMPLETED");
    Map<String, String> extraDetails = new HashMap<>();
    extraDetails.put("dc-0", "All partitions completed");
    Map<String, Long> extraInfoUpdateTimestamp = new HashMap<>();
    extraInfoUpdateTimestamp.put("dc-0", 1234567890L);

    GetJobStatusGrpcResponse grpcResponse = GetJobStatusGrpcResponse.newBuilder()
        .setStoreInfo(ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build())
        .setVersion(1)
        .setStatus(ExecutionStatus.COMPLETED.toString())
        .setStatusDetails("All partitions completed")
        .setStatusUpdateTimestamp(1234567890L)
        .putAllExtraInfo(extraInfo)
        .putAllExtraDetails(extraDetails)
        .putAllExtraInfoUpdateTimestamp(extraInfoUpdateTimestamp)
        .build();

    JobStatusQueryResponse httpResponse = jobRoutes.mapGrpcResponseToHttp(grpcResponse);

    Assert.assertEquals(httpResponse.getCluster(), TEST_CLUSTER);
    Assert.assertEquals(httpResponse.getName(), TEST_STORE);
    Assert.assertEquals(httpResponse.getVersion(), 1);
    Assert.assertEquals(httpResponse.getStatus(), ExecutionStatus.COMPLETED.toString());
    Assert.assertEquals(httpResponse.getOptionalStatusDetails().get(), "All partitions completed");
    Assert.assertEquals(httpResponse.getStatusUpdateTimestamp(), Long.valueOf(1234567890L));
    Assert.assertEquals(httpResponse.getExtraInfo().get("dc-0"), "COMPLETED");
    Assert.assertEquals(httpResponse.getExtraDetails().get("dc-0"), "All partitions completed");
    Assert.assertEquals(httpResponse.getExtraInfoUpdateTimestamp().get("dc-0"), Long.valueOf(1234567890L));
  }
}
