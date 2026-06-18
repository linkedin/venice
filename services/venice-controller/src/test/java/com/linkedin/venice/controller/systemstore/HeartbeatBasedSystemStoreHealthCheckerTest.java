package com.linkedin.venice.controller.systemstore;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
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
  public void testCheckHealthMarksAlwaysStaleStoreUnhealthyAfterWaitWindow() {
    // A single store that reads stale on every poll and is never aborted must end UNHEALTHY via the post-loop
    // completion block in checkHeartbeatFromSystemStores. This pins that terminal path directly, decoupled from any
    // healthy sibling store.
    String clusterName = "venice";
    String storeName = VeniceSystemStoreType.META_STORE.getSystemStoreName("testStore1");

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

    // Stale on every poll, so the store never resolves to HEALTHY and is still pending when the wait window ends.
    when(checker.getHeartbeatFromSystemStore(anyString(), anyString())).thenReturn(0L);

    Set<String> storeNames = new HashSet<>();
    storeNames.add(storeName);

    Map<String, HealthCheckResult> results = checker.checkHealth(clusterName, storeNames);

    verify(checker, atLeastOnce()).getHeartbeatFromSystemStore(clusterName, storeName);
    Assert.assertEquals(
        results.get(storeName),
        HealthCheckResult.UNHEALTHY,
        "a store stale through the full wait window (never aborted) must be marked UNHEALTHY by the post-loop block");
  }

  @Test
  public void testCheckHealthFlipsStaleStoreToHealthyOnLaterPoll() {
    // A store that is stale on the first poll but reads back a fresh heartbeat on a later polling iteration must
    // end up HEALTHY. This exercises the multi-iteration polling loop (the core retry-and-recover behavior).
    String clusterName = "venice";
    String storeName = VeniceSystemStoreType.META_STORE.getSystemStoreName("testStore1");

    // Wait time is large enough for >= 2 polling iterations; the check interval is 0 so the test runs fast.
    HeartbeatBasedSystemStoreHealthChecker checker = spy(
        new HeartbeatBasedSystemStoreHealthChecker(mock(VeniceParentHelixAdmin.class), 60, new AtomicBoolean(true)));
    doReturn(true).when(checker).shouldContinue(anyString());
    doReturn(0).when(checker).getHeartbeatCheckIntervalInSeconds();
    // No regions wired; the send phase is a no-op and reads come from the stub below.
    doReturn(new HashMap<String, ControllerClient>()).when(checker).getControllerClientMap(anyString());
    // First poll returns a stale heartbeat (older than the sent timestamp) -> UNHEALTHY; the second poll returns a
    // fresh heartbeat -> the store flips to HEALTHY.
    doReturn(0L).doReturn(Long.MAX_VALUE).when(checker).getHeartbeatFromSystemStore(anyString(), anyString());

    Set<String> storeNames = new HashSet<>();
    storeNames.add(storeName);

    Map<String, HealthCheckResult> results = checker.checkHealth(clusterName, storeNames);

    Assert.assertEquals(
        results.get(storeName),
        HealthCheckResult.HEALTHY,
        "a store stale on the first poll but fresh on a later poll should end HEALTHY");
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
  public void testCheckHealthOmitsUnpolledStoresWhenAborted() {
    // Verify the contract that stores never polled (because the periodic check aborted before reaching them)
    // are omitted from results, not pre-initialized as UNHEALTHY.
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
    when(checker.getHeartbeatCheckIntervalInSeconds()).thenReturn(1);
    doCallRealMethod().when(checker)
        .periodicCheckTask(anyString(), ArgumentMatchers.anyInt(), ArgumentMatchers.anyInt(), ArgumentMatchers.any());

    // shouldContinue returns true for the two phase-1 sends and the initial periodicCheckTask gate,
    // then false on the first per-store check inside the polling lambda — so no store is actually polled.
    when(checker.shouldContinue(anyString())).thenReturn(true, true, true, false);

    Set<String> storeNames = new HashSet<>();
    storeNames.add(storeName1);
    storeNames.add(storeName2);

    Map<String, HealthCheckResult> results = checker.checkHealth(clusterName, storeNames);

    Assert.assertTrue(
        results.isEmpty(),
        "stores whose heartbeats were sent but never polled should be omitted from results (deferred), not UNHEALTHY");
  }

  @Test
  public void testCheckHealthDefersStaleStorePolledThenAborted() {
    // A store found stale on a poll must NOT be recorded UNHEALTHY if the round aborts before the wait window
    // elapses — no final decision was reached, so it must be deferred (omitted) per the contract. This differs
    // from the never-polled case: here the store IS polled (and read stale) before the abort.
    String clusterName = "venice";
    String storeName = VeniceSystemStoreType.META_STORE.getSystemStoreName("testStore1");

    HeartbeatBasedSystemStoreHealthChecker checker = spy(
        new HeartbeatBasedSystemStoreHealthChecker(mock(VeniceParentHelixAdmin.class), 60, new AtomicBoolean(true)));
    doReturn(0).when(checker).getHeartbeatCheckIntervalInSeconds();
    // No regions wired; the send phase is a no-op and reads come from the stub below.
    doReturn(new HashMap<String, ControllerClient>()).when(checker).getControllerClientMap(anyString());
    // Always stale, so the store never resolves to HEALTHY and stays pending.
    doReturn(0L).when(checker).getHeartbeatFromSystemStore(anyString(), anyString());
    // true for the phase-1 send, the periodic-check gate, and the in-loop poll (so the store is polled and read
    // stale), then false so the round aborts before the wait window and the post-loop completion check defers
    // the still-pending store instead of marking it UNHEALTHY.
    doReturn(true, true, true, false).when(checker).shouldContinue(anyString());

    Set<String> storeNames = new HashSet<>();
    storeNames.add(storeName);

    Map<String, HealthCheckResult> results = checker.checkHealth(clusterName, storeNames);

    verify(checker).getHeartbeatFromSystemStore(clusterName, storeName);
    Assert.assertTrue(
        results.isEmpty(),
        "a store polled stale but aborted before the wait window elapsed should be deferred, not UNHEALTHY");
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
