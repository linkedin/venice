import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controller.stats.DeadStoreStats;
import com.linkedin.venice.meta.StoreInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;


/**
 * Tests for the DeadStoreStats changes and the StoreInfo modifications.
 */
public class TestDeadStoreStats {
  /**
   A simple fake implementation of DeadStoreStats for testing.
   Marks a store as dead if its name equals "dead" by setting the isStoreDead flag
   and populating the storeDeadStatusReasons list.
   */
  public static class FakeDeadStoreStats implements DeadStoreStats {
    @Override
    public List<StoreInfo> getDeadStores(List<StoreInfo> storeInfos, Map<String, String> params) {
      List<StoreInfo> deadStores = new ArrayList<>();
      String lookBackMSStr = params.get("lookBackMS");

      for (StoreInfo store: storeInfos) {
        // For testing, a store is considered dead if its name equals "dead".
        if ("dead".equals(store.getName())) {
          store.setIsStoreDead(true);
          if (lookBackMSStr != null && !lookBackMSStr.isEmpty()) {
            store.setStoreDeadStatusReasons(
                Arrays.asList("No traffic detected within lookback window of " + lookBackMSStr + "ms"));
          } else {
            store.setStoreDeadStatusReasons(Arrays.asList("No traffic detected"));
          }
          deadStores.add(store);
        } else {
          store.setIsStoreDead(false);
          store.setStoreDeadStatusReasons(Collections.emptyList());
        }
      }
      return deadStores;
    }

    @Override
    public void preFetchStats(List<StoreInfo> storeInfos) {
      // This is a no-op for the fake implementation.
      // In a real implementation, this would pre-fetch and cache the dead store stats.
      System.out.println("Pre-fetching stats for testing purposes. No actual operation performed.");
    }
  }

  /**
   * Tests that the StoreInfo getters and setters for the dead status and reasons work correctly.
   */
  @Test
  public void testStoreInfoDeadStatusSettersAndGetters() {
    // Create a new store (assuming StoreInfo has a no-arg constructor and name property)
    StoreInfo store = new StoreInfo();
    store.setName("testStore");
    assertFalse(store.getIsStoreDead());
    assertTrue(store.getStoreDeadStatusReasons().isEmpty());

    // Now mark the store as dead and provide reasons.
    store.setIsStoreDead(true);
    List<String> reasons = Arrays.asList("No traffic", "Deprecated");
    store.setStoreDeadStatusReasons(reasons);

    // Verify the state is updated accordingly.
    assertTrue(store.getIsStoreDead());
    assertEquals(reasons, store.getStoreDeadStatusReasons());
  }

  /**
   * Tests the FakeDeadStoreStats implementation.
   * Only the store with name "dead" should be marked as dead.
   */
  @Test
  public void testFakeDeadStoreStats() {
    // Create two stores: one intended to be dead and one alive.
    StoreInfo deadStore = new StoreInfo();
    deadStore.setName("dead");
    StoreInfo aliveStore = new StoreInfo();
    aliveStore.setName("alive");

    List<StoreInfo> stores = Arrays.asList(deadStore, aliveStore);

    DeadStoreStats stats = new FakeDeadStoreStats();
    List<StoreInfo> deadStores = stats.getDeadStores(stores, Collections.emptyMap());

    // Verify that only the deadStore is returned and its state has been updated.
    assertEquals(1, deadStores.size());
    assertEquals("dead", deadStores.get(0).getName());
    assertTrue(deadStores.get(0).getIsStoreDead());
    List<String> expectedReasons = Arrays.asList("No traffic detected");
    assertEquals(expectedReasons, deadStores.get(0).getStoreDeadStatusReasons());

    // Confirm that the alive store remains not marked as dead.
    assertFalse(aliveStore.getIsStoreDead());
    assertTrue(aliveStore.getStoreDeadStatusReasons().isEmpty());
  }

  /**
   * Tests the FakeDeadStoreStats implementation with lookBackMS parameter.
   */
  @Test
  public void testFakeDeadStoreStatsWithLookBack() {
    // Create two stores: one intended to be dead and one alive.
    StoreInfo deadStore = new StoreInfo();
    deadStore.setName("dead");
    StoreInfo aliveStore = new StoreInfo();
    aliveStore.setName("alive");

    List<StoreInfo> stores = Arrays.asList(deadStore, aliveStore);

    DeadStoreStats stats = new FakeDeadStoreStats();
    Map<String, String> params = new HashMap<>();
    params.put("lookBackMS", "30000");
    List<StoreInfo> deadStores = stats.getDeadStores(stores, params);

    // Verify that only the deadStore is returned and its state has been updated with lookback info
    assertEquals(1, deadStores.size());
    assertEquals("dead", deadStores.get(0).getName());
    assertTrue(deadStores.get(0).getIsStoreDead());
    List<String> expectedReasons = Arrays.asList("No traffic detected within lookback window of 30000ms");
    assertEquals(expectedReasons, deadStores.get(0).getStoreDeadStatusReasons());

    // Confirm that the alive store remains not marked as dead.
    assertFalse(aliveStore.getIsStoreDead());
    assertTrue(aliveStore.getStoreDeadStatusReasons().isEmpty());
  }
}
