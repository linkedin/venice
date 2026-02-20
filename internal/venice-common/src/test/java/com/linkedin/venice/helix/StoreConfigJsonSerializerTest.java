package com.linkedin.venice.helix;

import com.linkedin.venice.meta.StoreConfig;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for StoreConfigJsonSerializer to verify ObjectMapper isolation and thread safety.
 * This addresses the non-deterministic behavior caused by shared ObjectMapper instances.
 */
public class StoreConfigJsonSerializerTest {
  /**
   * Test that each serializer instance has its own ObjectMapper.
   * This is the key fix that prevents race conditions.
   */
  @Test
  public void testEachSerializerHasOwnObjectMapper() {
    StoreConfigJsonSerializer serializer1 = new StoreConfigJsonSerializer();
    StoreConfigJsonSerializer serializer2 = new StoreConfigJsonSerializer();

    // Each serializer should have its own ObjectMapper instance
    Assert.assertNotSame(
        serializer1.getObjectMapper(),
        serializer2.getObjectMapper(),
        "Each serializer should have its own ObjectMapper instance");
  }

  /**
   * Test that mixin is properly registered on the ObjectMapper.
   */
  @Test
  public void testMixinIsRegistered() {
    StoreConfigJsonSerializer serializer = new StoreConfigJsonSerializer();

    // Verify that the mixin was registered
    Assert.assertNotNull(
        serializer.getObjectMapper().findMixInClassFor(StoreConfig.class),
        "Mixin should be registered for StoreConfig");
  }

  /**
   * Test concurrent serializer instantiation to ensure thread safety.
   * This verifies that creating multiple serializers concurrently is safe.
   */
  @Test
  public void testConcurrentSerializerInstantiation() throws InterruptedException {
    int threadCount = 20;
    int iterationsPerThread = 50;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(threadCount);
    AtomicInteger successCount = new AtomicInteger(0);

    for (int i = 0; i < threadCount; i++) {
      executor.submit(() -> {
        try {
          startLatch.await();

          // Create multiple serializers concurrently
          for (int j = 0; j < iterationsPerThread; j++) {
            StoreConfigJsonSerializer serializer = new StoreConfigJsonSerializer();

            // Verify each has its own ObjectMapper
            if (serializer.getObjectMapper() != null) {
              successCount.incrementAndGet();
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
        } finally {
          doneLatch.countDown();
        }
      });
    }

    // Start all threads at once
    startLatch.countDown();

    // Wait for completion
    boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
    executor.shutdown();

    Assert.assertTrue(completed, "All threads should complete within timeout");
    Assert.assertEquals(
        successCount.get(),
        threadCount * iterationsPerThread,
        "All serializer instantiations should succeed");
  }

  /**
   * Test that multiple serializer instances have isolated ObjectMappers.
   */
  @Test
  public void testMultipleSerializerInstancesHaveIsolatedObjectMappers() {
    // Create multiple serializers
    StoreConfigJsonSerializer serializer1 = new StoreConfigJsonSerializer();
    StoreConfigJsonSerializer serializer2 = new StoreConfigJsonSerializer();
    StoreConfigJsonSerializer serializer3 = new StoreConfigJsonSerializer();

    // All should have different ObjectMapper instances
    Assert.assertNotSame(serializer1.getObjectMapper(), serializer2.getObjectMapper());
    Assert.assertNotSame(serializer2.getObjectMapper(), serializer3.getObjectMapper());
    Assert.assertNotSame(serializer1.getObjectMapper(), serializer3.getObjectMapper());

    // All should have the mixin registered
    Assert.assertNotNull(serializer1.getObjectMapper().findMixInClassFor(StoreConfig.class));
    Assert.assertNotNull(serializer2.getObjectMapper().findMixInClassFor(StoreConfig.class));
    Assert.assertNotNull(serializer3.getObjectMapper().findMixInClassFor(StoreConfig.class));
  }
}
