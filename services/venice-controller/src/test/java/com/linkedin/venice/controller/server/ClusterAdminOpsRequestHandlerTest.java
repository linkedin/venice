package com.linkedin.venice.controller.server;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.AdminCommandExecutionTracker;
import com.linkedin.venice.controller.ControllerRequestHandlerDependencies;
import com.linkedin.venice.controllerapi.AdminCommandExecution;
import com.linkedin.venice.controllerapi.AdminCommandExecutionStatus;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.protocols.controller.AdminCommandExecutionStatusGrpcRequest;
import com.linkedin.venice.protocols.controller.AdminCommandExecutionStatusGrpcResponse;
import com.linkedin.venice.protocols.controller.AdminTopicGrpcMetadata;
import com.linkedin.venice.protocols.controller.AdminTopicMetadataGrpcRequest;
import com.linkedin.venice.protocols.controller.AdminTopicMetadataGrpcResponse;
import com.linkedin.venice.protocols.controller.LastSuccessfulAdminCommandExecutionGrpcRequest;
import com.linkedin.venice.protocols.controller.LastSuccessfulAdminCommandExecutionGrpcResponse;
import com.linkedin.venice.protocols.controller.UpdateAdminOperationProtocolVersionGrpcRequest;
import com.linkedin.venice.protocols.controller.UpdateAdminTopicMetadataGrpcRequest;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ClusterAdminOpsRequestHandlerTest {
  private ClusterAdminOpsRequestHandler handler;
  private Admin mockAdmin;
  private AdminCommandExecutionTracker mockTracker;

  @BeforeMethod
  public void setUp() {
    mockAdmin = mock(Admin.class);
    ControllerRequestHandlerDependencies dependencies = mock(ControllerRequestHandlerDependencies.class);
    when(dependencies.getAdmin()).thenReturn(mockAdmin);
    handler = new ClusterAdminOpsRequestHandler(dependencies);
    mockTracker = mock(AdminCommandExecutionTracker.class);
  }

  @Test
  public void testGetAdminCommandExecutionStatusSuccess() {
    String clusterName = "test-cluster";
    long executionId = 12345L;

    AdminCommandExecution mockExecution = new AdminCommandExecution();
    mockExecution.setExecutionId(executionId);
    mockExecution.setOperation("test-operation");
    mockExecution.setStartTime(String.valueOf(123456789L));
    mockExecution.setFabricToExecutionStatusMap(
        new ConcurrentHashMap<>(Collections.singletonMap("fabric1", AdminCommandExecutionStatus.COMPLETED)));

    when(mockAdmin.getAdminCommandExecutionTracker(clusterName)).thenReturn(Optional.of(mockTracker));
    when(mockTracker.checkExecutionStatus(executionId)).thenReturn(mockExecution);

    AdminCommandExecutionStatusGrpcRequest request = AdminCommandExecutionStatusGrpcRequest.newBuilder()
        .setClusterName(clusterName)
        .setAdminCommandExecutionId(executionId)
        .build();

    AdminCommandExecutionStatusGrpcResponse response = handler.getAdminCommandExecutionStatus(request);

    assertNotNull(response);
    assertEquals(response.getClusterName(), clusterName);
    assertEquals(response.getAdminCommandExecutionId(), executionId);
    assertEquals(response.getOperation(), "test-operation");
    assertEquals(response.getStartTime(), String.valueOf(123456789L));
    assertEquals(
        response.getFabricToExecutionStatusMapMap().get("fabric1"),
        AdminCommandExecutionStatus.COMPLETED.name());
  }

  @Test
  public void testGetAdminCommandExecutionStatusTrackerNotPresent() {
    String clusterName = "test-cluster";
    long executionId = 12345L;

    when(mockAdmin.getAdminCommandExecutionTracker(clusterName)).thenReturn(Optional.empty());

    AdminCommandExecutionStatusGrpcRequest request = AdminCommandExecutionStatusGrpcRequest.newBuilder()
        .setClusterName(clusterName)
        .setAdminCommandExecutionId(executionId)
        .build();

    Exception e = expectThrows(VeniceException.class, () -> handler.getAdminCommandExecutionStatus(request));
    assertEquals(
        e.getMessage(),
        "Could not track execution in this controller for the cluster: test-cluster. Make sure you send the command to a correct parent controller.",
        "Actual message: " + e.getMessage());
  }

  @Test
  public void testGetAdminCommandExecutionStatusExecutionNotFound() {
    String clusterName = "test-cluster";
    long executionId = 12345L;

    when(mockAdmin.getAdminCommandExecutionTracker(clusterName)).thenReturn(Optional.of(mockTracker));
    when(mockTracker.checkExecutionStatus(executionId)).thenReturn(null);

    AdminCommandExecutionStatusGrpcRequest request = AdminCommandExecutionStatusGrpcRequest.newBuilder()
        .setClusterName(clusterName)
        .setAdminCommandExecutionId(executionId)
        .build();

    Exception e = expectThrows(VeniceException.class, () -> handler.getAdminCommandExecutionStatus(request));
    assertEquals(e.getMessage(), "Could not find the execution by given id: 12345 in cluster: test-cluster");
  }

  @Test
  public void testGetLastSucceedExecutionIdSuccess() {
    String clusterName = "test-cluster";
    long lastExecutionId = 54321L;

    when(mockAdmin.getLastSucceedExecutionId(clusterName)).thenReturn(lastExecutionId);

    LastSuccessfulAdminCommandExecutionGrpcRequest request =
        LastSuccessfulAdminCommandExecutionGrpcRequest.newBuilder().setClusterName(clusterName).build();

    LastSuccessfulAdminCommandExecutionGrpcResponse response = handler.getLastSucceedExecutionId(request);

    assertNotNull(response);
    assertEquals(response.getClusterName(), clusterName);
    assertEquals(response.getLastSuccessfulAdminCommandExecutionId(), lastExecutionId);
  }

  @Test
  public void testGetLastSucceedExecutionIdInvalidCluster() {
    LastSuccessfulAdminCommandExecutionGrpcRequest request =
        LastSuccessfulAdminCommandExecutionGrpcRequest.newBuilder().setClusterName("").build();

    Exception e = expectThrows(IllegalArgumentException.class, () -> handler.getLastSucceedExecutionId(request));
    assertEquals(e.getMessage(), "Cluster name is required for getting last succeeded execution id");
  }

  @Test
  public void testGetAdminTopicMetadataSuccess() {
    String clusterName = "test-cluster";
    long executionId = 123L;
    long offset = 456L;
    long upstreamOffset = 789L;

    Map<String, Long> metadata = new HashMap<>();
    metadata.put("offset", offset);
    metadata.put("upstreamOffset", upstreamOffset);
    metadata.put("executionId", executionId);

    when(mockAdmin.getAdminTopicMetadata(eq(clusterName), any())).thenReturn(metadata);

    AdminTopicMetadataGrpcRequest request =
        AdminTopicMetadataGrpcRequest.newBuilder().setClusterName(clusterName).build();

    AdminTopicMetadataGrpcResponse response = handler.getAdminTopicMetadata(request);

    assertNotNull(response);
    AdminTopicGrpcMetadata adminTopicGrpcMetadata = response.getMetadata();
    assertEquals(adminTopicGrpcMetadata.getExecutionId(), executionId);
    assertEquals(adminTopicGrpcMetadata.getOffset(), offset);
    assertEquals(adminTopicGrpcMetadata.getUpstreamOffset(), upstreamOffset);

    // non null store name
    request = AdminTopicMetadataGrpcRequest.newBuilder().setClusterName(clusterName).setStoreName("test-store").build();
    response = handler.getAdminTopicMetadata(request);

    assertNotNull(response);
    adminTopicGrpcMetadata = response.getMetadata();
    assertEquals(adminTopicGrpcMetadata.getExecutionId(), executionId);
    assertEquals(adminTopicGrpcMetadata.getOffset(), 0);
    assertEquals(adminTopicGrpcMetadata.getUpstreamOffset(), 0);
  }

  @Test
  public void testGetAdminTopicMetadataInvalidCluster() {
    AdminTopicMetadataGrpcRequest request = AdminTopicMetadataGrpcRequest.newBuilder().setClusterName("").build();

    Exception exception = expectThrows(IllegalArgumentException.class, () -> handler.getAdminTopicMetadata(request));
    assertTrue(exception.getMessage().contains("Cluster name is required for getting admin topic metadata"));
  }

  @Test
  public void testUpdateAdminTopicMetadataSuccess() {
    String clusterName = "test-cluster";
    long executionId = 12345L;
    AdminTopicGrpcMetadata metadata = AdminTopicGrpcMetadata.newBuilder()
        .setClusterName(clusterName)
        .setExecutionId(executionId)
        .setOffset(123L)
        .setUpstreamOffset(456L)
        .build();
    UpdateAdminTopicMetadataGrpcRequest request =
        UpdateAdminTopicMetadataGrpcRequest.newBuilder().setMetadata(metadata).build();
    AdminTopicMetadataGrpcResponse response = handler.updateAdminTopicMetadata(request);
    assertNotNull(response);
    assertEquals(response.getMetadata().getClusterName(), clusterName);
    assertFalse(response.getMetadata().hasStoreName());

    // Store name is provided
    metadata = AdminTopicGrpcMetadata.newBuilder()
        .setClusterName(clusterName)
        .setExecutionId(executionId)
        .setStoreName("test-store")
        .build();
    request = UpdateAdminTopicMetadataGrpcRequest.newBuilder().setMetadata(metadata).build();
    response = handler.updateAdminTopicMetadata(request);
    assertNotNull(response);
    assertEquals(response.getMetadata().getClusterName(), clusterName);
  }

  @Test
  public void testUpdateAdminTopicMetadataInvalidInputs() {
    String clusterName = "test-cluster";
    long executionId = 12345L;

    // No execution id
    AdminTopicGrpcMetadata metadata = AdminTopicGrpcMetadata.newBuilder().setClusterName(clusterName).build();
    UpdateAdminTopicMetadataGrpcRequest request =
        UpdateAdminTopicMetadataGrpcRequest.newBuilder().setMetadata(metadata).build();
    Exception exception = expectThrows(IllegalArgumentException.class, () -> handler.updateAdminTopicMetadata(request));
    assertTrue(exception.getMessage().contains("Admin command execution id with positive value is required"));

    // Either offset or upstream offset is provided
    metadata = AdminTopicGrpcMetadata.newBuilder()
        .setClusterName(clusterName)
        .setExecutionId(executionId)
        .setOffset(123L)
        .build();
    UpdateAdminTopicMetadataGrpcRequest request1 =
        UpdateAdminTopicMetadataGrpcRequest.newBuilder().setMetadata(metadata).build();
    exception = expectThrows(VeniceException.class, () -> handler.updateAdminTopicMetadata(request1));
    assertTrue(
        exception.getMessage().contains("Offsets must be provided to update cluster-level admin topic metadata"),
        "Actual message: " + exception.getMessage());

    metadata = AdminTopicGrpcMetadata.newBuilder()
        .setClusterName(clusterName)
        .setExecutionId(executionId)
        .setUpstreamOffset(123L)
        .build();
    UpdateAdminTopicMetadataGrpcRequest request2 =
        UpdateAdminTopicMetadataGrpcRequest.newBuilder().setMetadata(metadata).build();
    exception = expectThrows(VeniceException.class, () -> handler.updateAdminTopicMetadata(request2));
    assertTrue(
        exception.getMessage().contains("Offsets must be provided to update cluster-level admin topic metadata"),
        "Actual message: " + exception.getMessage());

    // both offsets and store name are provided
    metadata = AdminTopicGrpcMetadata.newBuilder()
        .setClusterName(clusterName)
        .setExecutionId(executionId)
        .setOffset(123L)
        .setStoreName("test-store")
        .build();
    UpdateAdminTopicMetadataGrpcRequest request3 =
        UpdateAdminTopicMetadataGrpcRequest.newBuilder().setMetadata(metadata).build();
    exception = expectThrows(VeniceException.class, () -> handler.updateAdminTopicMetadata(request3));
    assertTrue(
        exception.getMessage().contains("Updating offsets is not allowed for store-level admin topic metadata"),
        "Actual message: " + exception.getMessage());
  }

  @Test
  public void testUpdateAdminOperationProtocolVersionSuccess() {
    String clusterName = "test-cluster";
    long version = 12345L;
    UpdateAdminOperationProtocolVersionGrpcRequest request = UpdateAdminOperationProtocolVersionGrpcRequest.newBuilder()
        .setClusterName(clusterName)
        .setAdminOperationProtocolVersion(version)
        .build();
    AdminTopicMetadataGrpcResponse response = handler.updateAdminOperationProtocolVersion(request);

    assertNotNull(response);
    assertEquals(response.getMetadata().getClusterName(), clusterName);
    assertEquals(response.getMetadata().getAdminOperationProtocolVersion(), version);
  }

  @Test
  public void testUpdateAdminOperationProtocolVersionInvalidInputs() {
    String clusterName = "test-cluster";
    long version = 12345L;

    // No cluster name
    UpdateAdminOperationProtocolVersionGrpcRequest request1 =
        UpdateAdminOperationProtocolVersionGrpcRequest.newBuilder()
            .setClusterName("")
            .setAdminOperationProtocolVersion(version)
            .build();
    Exception exception =
        expectThrows(IllegalArgumentException.class, () -> handler.updateAdminOperationProtocolVersion(request1));
    assertTrue(
        exception.getMessage().contains("Cluster name is required for updating admin operation protocol version"));

    // Invalid version
    UpdateAdminOperationProtocolVersionGrpcRequest request2 =
        UpdateAdminOperationProtocolVersionGrpcRequest.newBuilder()
            .setClusterName(clusterName)
            .setAdminOperationProtocolVersion(0)
            .build();
    exception =
        expectThrows(IllegalArgumentException.class, () -> handler.updateAdminOperationProtocolVersion(request2));
    assertTrue(
        exception.getMessage()
            .contains("Admin operation protocol version is required and must be -1 or greater than 0"));
  }
}
