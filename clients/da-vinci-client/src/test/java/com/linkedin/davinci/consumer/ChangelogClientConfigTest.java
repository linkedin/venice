package com.linkedin.davinci.consumer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link ChangelogClientConfig#cloneConfig} to verify that the clone
 * does not share the {@code innerClientConfig} reference with the original.
 *
 * <p>Bug: {@code cloneConfig()} ends its builder chain with
 * {@code .setInnerClientConfig(config.getInnerClientConfig())}, which replaces the
 * freshly-allocated {@code innerClientConfig} on the new object with the <em>same</em>
 * reference held by the source config.  As a result, every subsequent
 * {@code clone.setStoreName(x)} call mutates the shared object, clobbering the store
 * name that every other clone (including the original) exposes via
 * {@code getStoreName()}.
 *
 * <p>This shows up as a data-race in production when two threads each call
 * {@code cloneConfig(global).setStoreName(differentStore)} concurrently:
 * the second writer's store name "wins" in the shared {@code innerClientConfig}, so
 * the first consumer ends up associated with the wrong store.
 */
public class ChangelogClientConfigTest {
  private static final String GLOBAL_STORE = "global_store";

  /**
   * Deterministic (single-threaded) regression test.
   *
   * <p>Two sequential clone+setStoreName calls share the same innerClientConfig, so
   * the second setStoreName overwrites the value set by the first.  After both calls:
   * <ul>
   *   <li>clone1 should report {@code "store_A"} – but it reports {@code "store_B"}</li>
   *   <li>clone2 reports {@code "store_B"} correctly</li>
   * </ul>
   * This test will <strong>fail</strong> until the bug is fixed.
   */
  @Test
  @SuppressWarnings("rawtypes")
  public void testCloneConfigDoesNotShareInnerClientConfig() {
    ChangelogClientConfig global = new ChangelogClientConfig(GLOBAL_STORE);

    ChangelogClientConfig clone1 = ChangelogClientConfig.cloneConfig(global).setStoreName("store_A");
    ChangelogClientConfig clone2 = ChangelogClientConfig.cloneConfig(global).setStoreName("store_B");

    // Each clone must remember its own store name independently of the other.
    Assert.assertEquals(
        clone1.getStoreName(),
        "store_A",
        "clone1 store name was overwritten by clone2 – cloneConfig() shares innerClientConfig by reference");
    Assert.assertEquals(clone2.getStoreName(), "store_B");

    // The original must also be unaffected.
    Assert.assertEquals(
        global.getStoreName(),
        GLOBAL_STORE,
        "cloneConfig() mutated the original's innerClientConfig via setStoreName on a clone");
  }

  /**
   * Multi-threaded race-condition test that mirrors the production failure.
   *
   * <p>Strategy:
   * <ol>
   *   <li>N threads each call {@code cloneConfig(global)} and park at a
   *       {@link CyclicBarrier}.</li>
   *   <li>Once every thread has a clone (and therefore all clones share the same
   *       {@code innerClientConfig}), all threads are released simultaneously to call
   *       {@code setStoreName(uniqueName)}.</li>
   *   <li>After all threads finish, we verify that every clone retained its own
   *       unique store name.</li>
   * </ol>
   *
   * <p>With the bug, all N clones share one {@code innerClientConfig}; the last
   * {@code setStoreName} to execute wins, so the set of distinct store names observed
   * across all clones will be 1 (or at most a handful) instead of N.
   *
   * <p>This test will <strong>fail</strong> until the bug is fixed.
   */
  @Test
  @SuppressWarnings("rawtypes")
  public void testCloneConfigIsThreadSafe() throws InterruptedException {
    final int numThreads = 20;

    ChangelogClientConfig global = new ChangelogClientConfig(GLOBAL_STORE);

    ChangelogClientConfig[] clones = new ChangelogClientConfig[numThreads];
    String[] expectedNames = new String[numThreads];
    List<Throwable> errors = new ArrayList<>();

    // Barrier with numThreads parties: every thread must reach it before any proceeds.
    CyclicBarrier allClonesReady = new CyclicBarrier(numThreads);

    ExecutorService pool = Executors.newFixedThreadPool(numThreads);
    for (int i = 0; i < numThreads; i++) {
      final int idx = i;
      expectedNames[idx] = "store_" + idx;
      pool.submit(() -> {
        try {
          // Phase 1: clone first, before any thread sets a store name.
          ChangelogClientConfig clone = ChangelogClientConfig.cloneConfig(global);
          clones[idx] = clone;

          // Wait until every thread has its clone – maximises the overlap window.
          allClonesReady.await(10, TimeUnit.SECONDS);

          // Phase 2: all threads race to setStoreName concurrently.
          clone.setStoreName(expectedNames[idx]);
        } catch (InterruptedException | BrokenBarrierException | java.util.concurrent.TimeoutException e) {
          synchronized (errors) {
            errors.add(e);
          }
          Thread.currentThread().interrupt();
        }
      });
    }

    pool.shutdown();
    boolean finished = pool.awaitTermination(30, TimeUnit.SECONDS);
    Assert.assertTrue(finished, "Thread pool did not terminate in time");
    Assert.assertTrue(errors.isEmpty(), "Unexpected exceptions in worker threads: " + errors);

    // Collect the store name each clone reports after all writers are done.
    Set<String> observed = new HashSet<>();
    List<String> violations = new ArrayList<>();
    for (int i = 0; i < numThreads; i++) {
      String actual = clones[i].getStoreName();
      observed.add(actual);
      if (!expectedNames[i].equals(actual)) {
        violations.add(String.format("clone[%d]: expected '%s', got '%s'", i, expectedNames[i], actual));
      }
    }

    Assert.assertTrue(
        violations.isEmpty(),
        "Store name corruption detected – cloneConfig() shares innerClientConfig by reference.\n"
            + String.join("\n", violations) + "\nDistinct names observed: " + observed);

    Assert.assertEquals(
        observed.size(),
        numThreads,
        "Expected " + numThreads + " distinct store names across clones but observed only " + observed.size() + ": "
            + observed);
  }
}
