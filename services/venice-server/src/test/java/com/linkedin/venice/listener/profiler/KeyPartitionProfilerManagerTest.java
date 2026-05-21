package com.linkedin.venice.listener.profiler;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.utils.TestUtils;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


public class KeyPartitionProfilerManagerTest {
  private KeyPartitionProfilerManager manager;

  @AfterMethod
  public void tearDown() {
    if (manager != null) {
      manager.shutdown();
    }
  }

  @Test
  public void fastPathFlagFollowsSessionLifecycle() {
    manager = new KeyPartitionProfilerManager(v -> 4);
    assertFalse(manager.isAnyProfilingActive(), "no active sessions on startup");
    KeyPartitionProfilerManager.StartResult result = manager.startProfiling("storeA", "storeA_v1", 5_000, 50);
    assertEquals(result.status, KeyPartitionProfilerManager.StartResult.Status.STARTED);
    assertTrue(manager.isAnyProfilingActive());
    assertNotNull(manager.getProfiler("storeA"));
    manager.stopProfiling("storeA");
    assertFalse(manager.isAnyProfilingActive());
    assertNull(manager.getProfiler("storeA"));
  }

  @Test
  public void concurrentSessionsAreCapped() {
    manager = new KeyPartitionProfilerManager(v -> 4, 2, null);
    KeyPartitionProfilerManager.StartResult first = manager.startProfiling("a", "a_v1", 30_000, 10);
    KeyPartitionProfilerManager.StartResult second = manager.startProfiling("b", "b_v1", 30_000, 10);
    KeyPartitionProfilerManager.StartResult third = manager.startProfiling("c", "c_v1", 30_000, 10);

    assertEquals(first.status, KeyPartitionProfilerManager.StartResult.Status.STARTED);
    assertEquals(second.status, KeyPartitionProfilerManager.StartResult.Status.STARTED);
    assertEquals(third.status, KeyPartitionProfilerManager.StartResult.Status.CAPACITY_EXCEEDED);
  }

  @Test
  public void doubleStartReturnsAlreadyRunning() {
    manager = new KeyPartitionProfilerManager(v -> 4);
    manager.startProfiling("a", "a_v1", 30_000, 10);
    KeyPartitionProfilerManager.StartResult dup = manager.startProfiling("a", "a_v1", 30_000, 10);
    assertEquals(dup.status, KeyPartitionProfilerManager.StartResult.Status.ALREADY_RUNNING);
  }

  @Test
  public void invalidDurationIsRejected() {
    manager = new KeyPartitionProfilerManager(v -> 4);
    KeyPartitionProfilerManager.StartResult result = manager.startProfiling("a", "a_v1", 0, 10);
    assertEquals(result.status, KeyPartitionProfilerManager.StartResult.Status.INVALID);
    assertFalse(manager.isAnyProfilingActive());
  }

  @Test
  public void durationAboveCapIsRejected() {
    manager = new KeyPartitionProfilerManager(v -> 4);
    KeyPartitionProfilerManager.StartResult result =
        manager.startProfiling("a", "a_v1", KeyPartitionProfilerManager.MAX_DURATION_MS + 1, 10);
    assertEquals(result.status, KeyPartitionProfilerManager.StartResult.Status.INVALID);
    assertTrue(result.message.contains("durationMs exceeds maximum"), result.message);
  }

  @Test
  public void invalidTopKIsRejected() {
    manager = new KeyPartitionProfilerManager(v -> 4);
    KeyPartitionProfilerManager.StartResult zero = manager.startProfiling("a", "a_v1", 5_000, 0);
    assertEquals(zero.status, KeyPartitionProfilerManager.StartResult.Status.INVALID);
    KeyPartitionProfilerManager.StartResult negative = manager.startProfiling("a", "a_v1", 5_000, -1);
    assertEquals(negative.status, KeyPartitionProfilerManager.StartResult.Status.INVALID);
  }

  @Test
  public void topKAboveCapIsRejected() {
    manager = new KeyPartitionProfilerManager(v -> 4);
    KeyPartitionProfilerManager.StartResult result =
        manager.startProfiling("a", "a_v1", 5_000, KeyPartitionProfilerManager.MAX_TOP_K + 1);
    assertEquals(result.status, KeyPartitionProfilerManager.StartResult.Status.INVALID);
    assertTrue(result.message.contains("topK exceeds maximum"), result.message);
  }

  @Test
  public void unresolvablePartitionCountIsRejected() {
    manager = new KeyPartitionProfilerManager(v -> {
      throw new IllegalStateException("no such storeVersion");
    });
    KeyPartitionProfilerManager.StartResult result = manager.startProfiling("a", "a_v1", 5_000, 10);
    assertEquals(result.status, KeyPartitionProfilerManager.StartResult.Status.INVALID);
    assertTrue(result.message.contains("no such storeVersion"), result.message);
  }

  @Test
  public void autoExpirationClearsFastPathAndRemovesSession() {
    manager = new KeyPartitionProfilerManager(v -> 4);
    manager.startProfiling("a", "a_v1", 100, 10);
    assertTrue(manager.isAnyProfilingActive());
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      assertFalse(manager.isAnyProfilingActive(), "fast-path flag should clear after expiration");
      assertNull(manager.getProfiler("a"));
    });
  }

  @Test
  public void stopOnUnknownStoreReturnsEmpty() {
    manager = new KeyPartitionProfilerManager(v -> 4);
    assertFalse(manager.stopProfiling("nonexistent").isPresent());
  }

  @Test
  public void stopReturnsStoppedProfilerCarryingItsStoreVersion() {
    manager = new KeyPartitionProfilerManager(v -> 4);
    manager.startProfiling("storeA", "storeA_v7", 30_000, 10);
    Optional<KeyPartitionProfiler> stopped = manager.stopProfiling("storeA");
    assertTrue(stopped.isPresent());
    assertEquals(stopped.get().getStoreName(), "storeA");
    assertEquals(stopped.get().getStoreVersion(), "storeA_v7");
  }

  @Test
  public void fastPathFlagStaysHotUntilLastSessionEnds() {
    manager = new KeyPartitionProfilerManager(v -> 4);
    manager.startProfiling("a", "a_v1", 30_000, 10);
    manager.startProfiling("b", "b_v1", 30_000, 10);
    assertEquals(manager.activeSessionCount(), 2);
    manager.stopProfiling("a");
    assertEquals(manager.activeSessionCount(), 1);
    assertTrue(manager.isAnyProfilingActive(), "still one active session");
    manager.stopProfiling("b");
    assertEquals(manager.activeSessionCount(), 0);
    assertFalse(manager.isAnyProfilingActive());
  }
}
