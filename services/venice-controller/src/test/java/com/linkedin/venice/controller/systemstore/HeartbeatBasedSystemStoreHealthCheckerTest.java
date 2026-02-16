package com.linkedin.venice.controller.systemstore;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import com.linkedin.venice.controller.systemstore.SystemStoreHealthChecker.HealthCheckResult;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.SystemStoreHeartbeatResponse;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.mockito.ArgumentMatchers;
import org.testng.Assert;
import org.testng.annotations.Test;


public class HeartbeatBasedSystemStoreHealthCheckerTest {
  @Test
  public void testGetHeartbeatFromSystemStore() {
    String clusterName = "venice";
    String userStoreName1 = "testStore";
    String metaStoreName1 = VeniceSystemStoreType.META_STORE.getSystemStoreName(userStoreName1);
    String pushStatusStoreName1 = VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(userStoreName1);

    String userStoreName2 = "testStore2";
    String metaStoreName2 = VeniceSystemStoreType.META_STORE.getSystemStoreName(userStoreName2);
    String pushStatusStoreName2 = VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(userStoreName2);

    SystemStoreHeartbeatResponse heartbeatResponse1 = new SystemStoreHeartbeatResponse();
    heartbeatResponse1.setHeartbeatTimestamp(10L);
    SystemStoreHeartbeatResponse heartbeatResponse2 = new SystemStoreHeartbeatResponse();
    heartbeatResponse2.setHeartbeatTimestamp(11L);
    SystemStoreHeartbeatResponse heartbeatResponse3 = new SystemStoreHeartbeatResponse();
    heartbeatResponse3.setHeartbeatTimestamp(-1L);

    ControllerClient client1 = mock(ControllerClient.class);
    when(client1.getHeartbeatFromSystemStore(metaStoreName1)).thenReturn(heartbeatResponse1);
    when(client1.getHeartbeatFromSystemStore(pushStatusStoreName1)).thenReturn(heartbeatResponse1);
    when(client1.getHeartbeatFromSystemStore(metaStoreName2)).thenReturn(heartbeatResponse1);
    when(client1.getHeartbeatFromSystemStore(pushStatusStoreName2)).thenReturn(heartbeatResponse1);

    ControllerClient client2 = mock(ControllerClient.class);
    when(client2.getHeartbeatFromSystemStore(metaStoreName1)).thenReturn(heartbeatResponse2);
    when(client2.getHeartbeatFromSystemStore(pushStatusStoreName1)).thenReturn(heartbeatResponse3);
    when(client2.getHeartbeatFromSystemStore(metaStoreName2)).thenReturn(heartbeatResponse1);
    when(client2.getHeartbeatFromSystemStore(pushStatusStoreName2)).thenReturn(heartbeatResponse1);

    HeartbeatBasedSystemStoreHealthChecker checker = mock(HeartbeatBasedSystemStoreHealthChecker.class);
    when(checker.getHeartbeatFromSystemStore(anyString(), anyString())).thenCallRealMethod();
    Map<String, ControllerClient> controllerClientMap = new HashMap<>();
    controllerClientMap.put("region1", client1);
    controllerClientMap.put("region2", client2);
    when(checker.getControllerClientMap(clusterName)).thenReturn(controllerClientMap);

    Assert.assertEquals(checker.getHeartbeatFromSystemStore(clusterName, metaStoreName1), 10L);
    Assert.assertEquals(checker.getHeartbeatFromSystemStore(clusterName, pushStatusStoreName1), -1L);
    Assert.assertEquals(checker.getHeartbeatFromSystemStore(clusterName, metaStoreName2), 10L);
    Assert.assertEquals(checker.getHeartbeatFromSystemStore(clusterName, pushStatusStoreName2), 10L);
  }

  @Test
  public void testCheckHealthReturnsCorrectResults() {
    String clusterName = "venice";
    String storeName1 = VeniceSystemStoreType.META_STORE.getSystemStoreName("testStore1");
    String storeName2 = VeniceSystemStoreType.META_STORE.getSystemStoreName("testStore2");

    HeartbeatBasedSystemStoreHealthChecker checker = mock(HeartbeatBasedSystemStoreHealthChecker.class);
    doCallRealMethod().when(checker).checkHealth(anyString(), ArgumentMatchers.<Set<String>>any());
    doCallRealMethod().when(checker)
        .checkHeartbeatFromSystemStores(
            anyString(),
            ArgumentMatchers.<Map<String, Long>>any(),
            ArgumentMatchers.<Map<String, HealthCheckResult>>any());
    when(checker.shouldContinue(anyString())).thenReturn(true);
    when(checker.getHeartbeatCheckIntervalInSeconds()).thenReturn(1);
    doCallRealMethod().when(checker)
        .periodicCheckTask(anyString(), ArgumentMatchers.anyInt(), ArgumentMatchers.anyInt(), ArgumentMatchers.any());

    // storeName1 returns fresh heartbeat, storeName2 returns stale
    when(checker.getHeartbeatFromSystemStore(anyString(), anyString())).thenAnswer(invocation -> {
      String store = invocation.getArgument(1);
      if (store.equals(storeName1)) {
        return Long.MAX_VALUE; // fresh heartbeat
      }
      return 0L; // stale heartbeat
    });

    Set<String> storeNames = new HashSet<>();
    storeNames.add(storeName1);
    storeNames.add(storeName2);

    Map<String, HealthCheckResult> results = checker.checkHealth(clusterName, storeNames);

    Assert.assertEquals(results.get(storeName1), HealthCheckResult.HEALTHY);
    Assert.assertEquals(results.get(storeName2), HealthCheckResult.UNHEALTHY);
  }

  @Test
  public void testCheckHealthEmptySet() {
    VeniceParentHelixAdmin parentAdmin = mock(VeniceParentHelixAdmin.class);
    AtomicBoolean isRunning = new AtomicBoolean(true);
    HeartbeatBasedSystemStoreHealthChecker checker =
        new HeartbeatBasedSystemStoreHealthChecker(parentAdmin, 5, isRunning);

    Map<String, HealthCheckResult> results = checker.checkHealth("venice", new HashSet<>());
    Assert.assertTrue(results.isEmpty());
  }

  @Test
  public void testCheckHealthWhenNotRunning() {
    VeniceParentHelixAdmin parentAdmin = mock(VeniceParentHelixAdmin.class);
    VeniceHelixAdmin helixAdmin = mock(VeniceHelixAdmin.class);
    when(parentAdmin.getVeniceHelixAdmin()).thenReturn(helixAdmin);
    when(parentAdmin.isLeaderControllerFor(anyString())).thenReturn(true);
    Map<String, ControllerClient> clientMap = new HashMap<>();
    clientMap.put("region1", mock(ControllerClient.class));
    when(helixAdmin.getControllerClientMap("venice")).thenReturn(clientMap);

    AtomicBoolean isRunning = new AtomicBoolean(false);
    HeartbeatBasedSystemStoreHealthChecker checker =
        new HeartbeatBasedSystemStoreHealthChecker(parentAdmin, 5, isRunning);

    Set<String> storeNames = new HashSet<>();
    storeNames.add("store1");

    // When not running, shouldContinue returns false immediately, no heartbeats sent
    Map<String, HealthCheckResult> results = checker.checkHealth("venice", storeNames);
    // Results should be empty since the send loop broke before any heartbeats were sent
    Assert.assertTrue(results.isEmpty());
  }
}
