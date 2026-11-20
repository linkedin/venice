package com.linkedin.venice.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.exceptions.VeniceException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ParentVersionOrchestratorTest {
  private static final String CLUSTER = "test-cluster";
  private static final String STORE = "test-store";
  private static final String SYSTEM_CLUSTER = "system-schema-cluster";

  private VeniceParentHelixAdmin parent;
  private VeniceHelixAdmin helixAdmin;
  private VeniceControllerMultiClusterConfig multiClusterConfigs;
  private VeniceControllerClusterConfig systemClusterConfig;
  private ParentVersionOrchestrator orchestrator;

  @BeforeMethod
  public void setUp() {
    parent = mock(VeniceParentHelixAdmin.class);
    helixAdmin = mock(VeniceHelixAdmin.class);
    multiClusterConfigs = mock(VeniceControllerMultiClusterConfig.class);
    systemClusterConfig = mock(VeniceControllerClusterConfig.class);

    when(parent.getVeniceHelixAdmin()).thenReturn(helixAdmin);
    when(parent.getMultiClusterConfigs()).thenReturn(multiClusterConfigs);
    when(multiClusterConfigs.getSystemSchemaClusterName()).thenReturn(SYSTEM_CLUSTER);
    when(multiClusterConfigs.getControllerConfig(SYSTEM_CLUSTER)).thenReturn(systemClusterConfig);

    orchestrator = new ParentVersionOrchestrator(parent);
  }

  @Test
  public void testRepushInfoMissingColoClientThrowsVeniceException() {
    when(helixAdmin.getControllerClientMap(CLUSTER)).thenReturn(Collections.emptyMap());

    assertThrows(
        VeniceException.class,
        () -> orchestrator.getRepushInfo(CLUSTER, STORE, Optional.of("missing-colo")));
  }

  @Test
  public void testRepushInfoNullStoreInColoThrowsVeniceException() {
    ControllerClient client = mock(ControllerClient.class);
    StoreResponse emptyResponse = new StoreResponse(); // store is null
    when(client.getStore(STORE)).thenReturn(emptyResponse);
    when(helixAdmin.getControllerClientMap(CLUSTER)).thenReturn(Collections.singletonMap("colo1", client));

    assertThrows(
        VeniceException.class,
        () -> orchestrator.getRepushInfo(CLUSTER, STORE, Optional.of("colo1")));
  }

  @Test
  public void testRepushInfoNoFabricWithNoVersionsThrowsVeniceException() {
    FabricControllerClientProvider provider = mock(FabricControllerClientProvider.class);
    when(helixAdmin.getFabricControllerClientProvider()).thenReturn(provider);
    when(helixAdmin.getControllerClientMap(CLUSTER)).thenReturn(Collections.emptyMap());
    when(provider.queryAllRegions(any(), eq(CLUSTER), anyInt(), any(), any(), any()))
        .thenReturn(Collections.emptyMap());

    assertThrows(
        VeniceException.class,
        () -> orchestrator.getRepushInfo(CLUSTER, STORE, Optional.empty()));
  }

  @Test
  public void testRepushInfoNoFabricWithInvalidVersionsThrowsVeniceException() {
    FabricControllerClientProvider provider = mock(FabricControllerClientProvider.class);
    when(helixAdmin.getFabricControllerClientProvider()).thenReturn(provider);
    Map<String, ControllerClient> clients = new HashMap<>();
    ControllerClient client = mock(ControllerClient.class);
    clients.put("colo1", client);
    when(helixAdmin.getControllerClientMap(CLUSTER)).thenReturn(clients);

    // All colos have non-positive version (e.g. -1 sentinel)
    when(provider.queryAllRegions(any(), eq(CLUSTER), anyInt(), any(), any(), any()))
        .thenReturn(Collections.singletonMap("colo1", -1));

    assertThrows(
        VeniceException.class,
        () -> orchestrator.getRepushInfo(CLUSTER, STORE, Optional.empty()));
  }

  @Test
  public void testGetCurrentVersionInRegionNullStoreReturnsMinusOne() {
    ControllerClient client = mock(ControllerClient.class);
    StoreResponse emptyResponse = new StoreResponse(); // store is null
    when(client.getStore(STORE)).thenReturn(emptyResponse);
    when(helixAdmin.getControllerClientMap(CLUSTER)).thenReturn(Collections.singletonMap("region1", client));

    int version = orchestrator.getCurrentVersionInRegion(CLUSTER, STORE, "region1");
    assertEquals(version, -1);
  }

  @Test
  public void testGetCurrentVersionInRegionMissingClientReturnsMinusOne() {
    when(helixAdmin.getControllerClientMap(CLUSTER)).thenReturn(Collections.emptyMap());

    int version = orchestrator.getCurrentVersionInRegion(CLUSTER, STORE, "unknown-region");
    assertEquals(version, -1);
  }
}
