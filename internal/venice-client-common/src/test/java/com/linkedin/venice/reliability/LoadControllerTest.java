package com.linkedin.venice.reliability;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.utils.TestMockTime;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class LoadControllerTest {
  @DataProvider(name = "counterPaths")
  public Object[][] counterPaths() {
    return new Object[][] { { false }, { true } };
  }

  @Test(dataProvider = "counterPaths")
  public void testRequestRejectionWhenOverload(boolean useIndependentCounter) {
    LoadController loadController = LoadController.newBuilder()
        .setWindowSizeInSec(5)
        .setRejectionRatioUpdateIntervalInSec(1)
        .setMaxRejectionRatio(0.5)
        .setAcceptMultiplier(2.0)
        .setUseIndependentCounter(useIndependentCounter)
        .build();
    for (int i = 0; i < 100; i++) {
      loadController.recordRequest();
    }
    for (int i = 0; i < 30; i++) {
      loadController.recordAccept();
    }

    assertTrue(loadController.getRejectionRatio() > 0.3);
    assertTrue(loadController.isOverloaded());

    int rejectCount = 0;
    for (int i = 0; i < 1000; ++i) {
      if (loadController.shouldRejectRequest()) {
        rejectCount++;
      }
    }
    assertTrue(rejectCount > 300);
  }

  @Test(dataProvider = "counterPaths")
  public void testRejectionRatioReset(boolean useIndependentCounter) throws InterruptedException {
    LoadController loadController = LoadController.newBuilder()
        .setWindowSizeInSec(3)
        .setRejectionRatioUpdateIntervalInSec(1)
        .setMaxRejectionRatio(0.5)
        .setAcceptMultiplier(1.0)
        .setUseIndependentCounter(useIndependentCounter)
        .build();
    for (int i = 0; i < 100; i++) {
      loadController.recordRequest();
    }
    for (int i = 0; i < 30; i++) {
      loadController.recordAccept();
    }

    assertTrue(loadController.getRejectionRatio() > 0.3);
    assertTrue(loadController.isOverloaded());

    Thread.sleep(10 * 1000); // over 2 time windows
    for (int i = 0; i < 100; i++) {
      loadController.recordRequest();
      loadController.recordAccept();
    }

    assertEquals(loadController.getRejectionRatio(), 0.0d);
    assertFalse(loadController.isOverloaded());
  }

  @Test(dataProvider = "counterPaths")
  public void testNotOverloadedWhenAcceptKeepsUp(boolean useIndependentCounter) {
    LoadController loadController = LoadController.newBuilder()
        .setWindowSizeInSec(5)
        .setRejectionRatioUpdateIntervalInSec(1)
        .setMaxRejectionRatio(0.5)
        .setAcceptMultiplier(1.0)
        .setUseIndependentCounter(useIndependentCounter)
        .build();
    for (int i = 0; i < 100; i++) {
      loadController.recordRequest();
      loadController.recordAccept();
    }
    assertEquals(loadController.getRejectionRatio(), 0.0d);
    assertFalse(loadController.isOverloaded());
    assertFalse(loadController.shouldRejectRequest());
  }

  @Test(dataProvider = "counterPaths")
  public void testMaxRejectionRatioIsRespected(boolean useIndependentCounter) {
    LoadController loadController = LoadController.newBuilder()
        .setWindowSizeInSec(5)
        .setRejectionRatioUpdateIntervalInSec(1)
        .setMaxRejectionRatio(0.25)
        .setAcceptMultiplier(2.0)
        .setUseIndependentCounter(useIndependentCounter)
        .build();
    // 1000 requests, 0 accepts — raw ratio would approach 1.0 but must be capped at 0.25.
    for (int i = 0; i < 1000; i++) {
      loadController.recordRequest();
    }
    assertEquals(loadController.getRejectionRatio(), 0.25d, 1e-9);
  }

  @Test(dataProvider = "counterPaths")
  public void testZeroRequestsMeansZeroRatio(boolean useIndependentCounter) {
    LoadController loadController = LoadController.newBuilder()
        .setWindowSizeInSec(5)
        .setRejectionRatioUpdateIntervalInSec(1)
        .setMaxRejectionRatio(0.5)
        .setAcceptMultiplier(2.0)
        .setUseIndependentCounter(useIndependentCounter)
        .build();
    assertEquals(loadController.getRejectionRatio(), 0.0d);
    assertFalse(loadController.isOverloaded());
    assertFalse(loadController.shouldRejectRequest());
  }

  @Test(dataProvider = "counterPaths")
  public void testRejectionRatioIsCachedBetweenUpdates(boolean useIndependentCounter) {
    TestMockTime mockTime = new TestMockTime(0);
    LoadController loadController = LoadController.newBuilder()
        .setWindowSizeInSec(10)
        .setRejectionRatioUpdateIntervalInSec(5)
        .setMaxRejectionRatio(1.0)
        .setAcceptMultiplier(2.0)
        .setTime(mockTime)
        .setUseIndependentCounter(useIndependentCounter)
        .build();
    for (int i = 0; i < 100; i++) {
      loadController.recordRequest();
    }
    double first = loadController.getRejectionRatio();
    assertTrue(first > 0);

    // Record a flood of accepts, but without advancing time past the update interval the cached
    // ratio must be returned unchanged.
    for (int i = 0; i < 200; i++) {
      loadController.recordAccept();
    }
    assertEquals(loadController.getRejectionRatio(), first, 1e-9);

    // Advance past the update interval; the ratio must refresh and drop toward 0 because accepts
    // now outpace the accept-multiplier adjusted requests.
    mockTime.addMilliseconds(TimeUnit.SECONDS.toMillis(6));
    double refreshed = loadController.getRejectionRatio();
    assertTrue(refreshed < first);
  }

  /**
   * Parity: feed identical sequences to both the Tehuti-backed and the independent-counter
   * instances and assert their rejection ratios are equivalent for the scenarios the production
   * code actually hits. Ensures flipping the flag does not change externally observable behavior.
   */
  @Test
  public void testBothCounterPathsProduceEquivalentRatios() {
    LoadController tehuti = LoadController.newBuilder()
        .setWindowSizeInSec(5)
        .setRejectionRatioUpdateIntervalInSec(1)
        .setMaxRejectionRatio(1.0)
        .setAcceptMultiplier(2.0)
        .setUseIndependentCounter(false)
        .build();
    LoadController independent = LoadController.newBuilder()
        .setWindowSizeInSec(5)
        .setRejectionRatioUpdateIntervalInSec(1)
        .setMaxRejectionRatio(1.0)
        .setAcceptMultiplier(2.0)
        .setUseIndependentCounter(true)
        .build();

    // Overload — 100 requests, 30 accepts.
    for (int i = 0; i < 100; i++) {
      tehuti.recordRequest();
      independent.recordRequest();
    }
    for (int i = 0; i < 30; i++) {
      tehuti.recordAccept();
      independent.recordAccept();
    }
    assertEquals(tehuti.getRejectionRatio(), independent.getRejectionRatio(), 1e-6);

    // Steady state on top — heavy balanced traffic. Allow a small tolerance: the two counters use
    // independent bucket structures, so cached ratios may differ by at most one update cycle.
    for (int i = 0; i < 500; i++) {
      tehuti.recordRequest();
      tehuti.recordAccept();
      independent.recordRequest();
      independent.recordAccept();
    }
    assertEquals(tehuti.getRejectionRatio(), independent.getRejectionRatio(), 0.02);
  }

  @Test
  public void testCompletableAllOf() {
    CompletableFuture<Void> future1 = new CompletableFuture<>();
    CompletableFuture<Void> future2 = new CompletableFuture<>();
    future1.complete(null);
    future2.completeExceptionally(new RuntimeException("Test exception"));
    CompletableFuture.allOf(future1, future2).exceptionally(e -> {
      e.printStackTrace();
      return null;
    });
  }
}
