package com.linkedin.venice.reliability;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.CompletableFuture;
import org.testng.annotations.Test;


public class LoadControllerTest {
  @Test
  public void testRequestRejectionWhenOverload() {
    LoadController loadController = LoadController.newBuilder()
        .setWindowSizeInSec(5)
        .setRejectionRatioUpdateIntervalInSec(1)
        .setMaxRejectionRatio(0.5)
        .setAcceptMultiplier(2.0)
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

  @Test
  public void testRejectionRatioReset() throws InterruptedException {
    LoadController loadController = LoadController.newBuilder()
        .setWindowSizeInSec(3)
        .setRejectionRatioUpdateIntervalInSec(1)
        .setMaxRejectionRatio(0.5)
        .setAcceptMultiplier(1.0)
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
