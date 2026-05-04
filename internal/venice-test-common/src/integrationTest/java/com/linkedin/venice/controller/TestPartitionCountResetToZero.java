package com.linkedin.venice.controller;

import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Regression tests for the partition-count auto-calc behavior on update-store.
 *
 * <p>{@link com.linkedin.venice.utils.PartitionUtils#calculatePartitionCount} auto-calcs the
 * partition count for a new push from storage quota when the persisted
 * {@code store.getPartitionCount() == 0}. Reaching that branch requires the store record to
 * actually carry a zero, so {@link VeniceHelixAdmin#setStorePartitionCount} must preserve a
 * caller-supplied 0 verbatim instead of substituting a cluster-level minimum.
 *
 * <p>Earlier versions of {@code setStorePartitionCount} clamped {@code 0} to
 * {@code clusterConfig.getMinNumberOfPartitions()}, which silently defeated both the auto-calc
 * branch in {@code PartitionUtils.calculatePartitionCount} and the parent-side hybrid-conversion
 * floor enforcement in {@link VeniceParentHelixAdmin#updateStore}.
 */
public class TestPartitionCountResetToZero extends AbstractTestVeniceHelixAdmin {
  private final MetricsRepository metricsRepository = new MetricsRepository();

  @BeforeClass(alwaysRun = true)
  public void setUp() throws Exception {
    setupCluster(metricsRepository);
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    super.cleanUp();
  }

  /**
   * Baseline: a freshly created store carries {@code partitionCount == 0} (from the {@code ZKStore}
   * constructor) and {@code calculateNumberOfPartitions} returns the quota-derived auto-calc value.
   */
  @Test(timeOut = TOTAL_TIMEOUT_FOR_SHORT_TEST_MS)
  public void testFreshStoreAutoCalcsPartitionCountFromStorageQuota() {
    String storeName = Utils.getUniqueString("auto-calc-fresh");
    veniceAdmin.createStore(clusterName, storeName, storeOwner, KEY_SCHEMA, VALUE_SCHEMA);

    long partitionSize = controllerConfig.getPartitionSize();
    int minPartitions = controllerConfig.getMinNumberOfPartitions();
    int maxPartitions = controllerConfig.getMaxNumberOfPartitions();
    int expectedAutoCalc = Math.min(maxPartitions, Math.max(minPartitions, 5));
    long storageQuota = partitionSize * expectedAutoCalc;

    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setStorageQuotaInByte(storageQuota));

    Store store = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertEquals(
        store.getPartitionCount(),
        0,
        "Fresh store record must carry partitionCount=0 so calculatePartitionCount can run on push.");

    int calculated = veniceAdmin.calculateNumberOfPartitions(clusterName, storeName);
    Assert.assertEquals(
        calculated,
        expectedAutoCalc,
        "calculateNumberOfPartitions on a fresh store should auto-calc from storage quota, "
            + "since storePartitionCount==0 falls through to PartitionUtils.calculatePartitionCount.");
  }

  /**
   * Setting {@code partitionCount=0} via {@code update-store} is the documented "use auto-calc"
   * sentinel. The persisted store record must keep that zero so the next push triggers
   * {@link com.linkedin.venice.utils.PartitionUtils#calculatePartitionCount} against quota, just
   * like a freshly-created store.
   */
  @Test(timeOut = TOTAL_TIMEOUT_FOR_SHORT_TEST_MS)
  public void testUpdateStorePartitionCountToZeroPersistsAsZero() {
    String storeName = Utils.getUniqueString("auto-calc-reset");
    veniceAdmin.createStore(clusterName, storeName, storeOwner, KEY_SCHEMA, VALUE_SCHEMA);

    long partitionSize = controllerConfig.getPartitionSize();
    int minPartitions = controllerConfig.getMinNumberOfPartitions();
    int maxPartitions = controllerConfig.getMaxNumberOfPartitions();
    int explicitNonZeroCount = Math.min(maxPartitions, minPartitions + 3);
    int expectedAutoCalc = Math.min(maxPartitions, Math.max(minPartitions, 5));
    long storageQuota = partitionSize * expectedAutoCalc;

    veniceAdmin.updateStore(
        clusterName,
        storeName,
        new UpdateStoreQueryParams().setStorageQuotaInByte(storageQuota).setPartitionCount(explicitNonZeroCount));

    Store afterExplicit = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertEquals(
        afterExplicit.getPartitionCount(),
        explicitNonZeroCount,
        "Explicit non-zero partition count should persist verbatim.");

    veniceAdmin.updateStore(clusterName, storeName, new UpdateStoreQueryParams().setPartitionCount(0));

    Store afterReset = veniceAdmin.getStore(clusterName, storeName);
    Assert.assertEquals(
        afterReset.getPartitionCount(),
        0,
        "update-store partitionCount=0 must persist as the auto-calc sentinel. "
            + "If this assert fires, setStorePartitionCount is clamping 0 to the cluster minimum "
            + "and defeating the auto-calc-on-push branch.");

    int calculated = veniceAdmin.calculateNumberOfPartitions(clusterName, storeName);
    Assert.assertEquals(
        calculated,
        expectedAutoCalc,
        "After reset to 0, calculateNumberOfPartitions must return the quota-derived value, "
            + "not the previously-set explicit count and not the cluster minimum.");
  }
}
