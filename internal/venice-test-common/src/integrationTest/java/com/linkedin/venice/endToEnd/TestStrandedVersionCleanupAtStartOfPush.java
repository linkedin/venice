package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_BACKUP_VERSION_RETENTION_BASED_CLEANUP_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_EARLY_DELETE_BACKUP_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_ROLLED_BACK_VERSION_RETENTION_MS;
import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.ConfigKeys.DEFAULT_PARTITION_SIZE;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.ConfigKeys.TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.testng.annotations.Test;


/**
 * Integration test for {@code VeniceParentHelixAdmin#deleteStrandedNonCurrentVersions}, complementing the
 * mock-based coverage of its cross-fabric decision matrix in {@code TestVeniceParentHelixAdmin}.
 *
 * <p>Scenario: v1 and v2 are pushed, then a rollback strands v2 — v1 serves again while v2 lingers as
 * ROLLED_BACK in every fabric. The next push must reap v2 fleet-wide at start-of-push, without touching
 * the healthy backup (v1) or the version being pushed (v3). Empty pushes drive this: they go through the
 * same parent {@code incrementVersionIdempotent} path a data push does, minus the cost of a push job.
 *
 * <p>Two other mechanisms can independently delete a rolled-back version in a child region, and both are
 * disabled here so the parent's DELETE_OLD_VERSION admin message is the only thing that can remove v2
 * anywhere, making the assertions attributable to the code under test:
 * <ul>
 *   <li>{@code CONTROLLER_EARLY_DELETE_BACKUP_ENABLED} — the child's own start-of-push
 *       {@code retireOldStoreVersions} sweep.</li>
 *   <li>{@code CONTROLLER_BACKUP_VERSION_RETENTION_BASED_CLEANUP_ENABLED} — the child's background
 *       {@code StoreBackupVersionCleanupService}.</li>
 * </ul>
 * A child also retires old versions once a push completes, but promoting v3 to current resets the retention
 * clock that path keys off, so it leaves v2 alone here. On the parent, {@code cleanupHistoricalVersions}
 * only trims above the user-store retention count (5), which this test stays under.
 *
 * <p>The rolled-back retention is shortened so the rollback-origin guard — which blocks a new push while a
 * rolled-back version is within retention — lifts in time for the v3 push. It has to stay above zero,
 * otherwise the push-completion sweep above becomes eligible to reap v2 on its own.
 */
public class TestStrandedVersionCleanupAtStartOfPush extends AbstractMultiRegionTest {
  private static final int TEST_TIMEOUT = 180_000; // ms
  private static final long ROLLED_BACK_RETENTION_MS = TimeUnit.SECONDS.toMillis(2);
  private static final String PARENT = "parent";

  @Override
  protected int getNumberOfRegions() {
    return 2;
  }

  @Override
  protected int getNumberOfServers() {
    return 1;
  }

  @Override
  protected int getReplicationFactor() {
    return 1;
  }

  @Override
  protected Properties getExtraControllerProperties() {
    Properties controllerProps = new Properties();
    controllerProps.put(DEFAULT_MAX_NUMBER_OF_PARTITIONS, 1);
    controllerProps.put(DEFAULT_PARTITION_SIZE, 10);
    controllerProps
        .setProperty(TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS, String.valueOf(Long.MAX_VALUE));
    controllerProps.put(CONTROLLER_ROLLED_BACK_VERSION_RETENTION_MS, ROLLED_BACK_RETENTION_MS);
    controllerProps.put(CONTROLLER_EARLY_DELETE_BACKUP_ENABLED, false);
    controllerProps.put(CONTROLLER_BACKUP_VERSION_RETENTION_BASED_CLEANUP_ENABLED, false);
    // This store is only ever inspected through the controllers, so skip materializing its system stores.
    controllerProps.put(CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE, false);
    controllerProps.put(CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE, false);
    return controllerProps;
  }

  @Override
  protected Properties getExtraServerProperties() {
    Properties serverProps = new Properties();
    // Every version pushed here waits out this delay in the non-source region before its leader can switch
    // topics, and the test pushes three of them.
    serverProps.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 1L);
    return serverProps;
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testStrandedRolledBackVersionIsReapedAtStartOfNextPush() throws InterruptedException {
    String storeName = Utils.getUniqueString("strandedVersionCleanup");
    // Keyed by region label, with the parent first so it is the first region reported on assertion failures.
    Map<String, ControllerClient> controllerClients = new LinkedHashMap<>();
    try {
      ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentController.getControllerUrl());
      controllerClients.put(PARENT, parentControllerClient);
      for (int i = 0; i < childDatacenters.size(); i++) {
        controllerClients.put(
            multiRegionMultiClusterWrapper.getChildRegionNames().get(i),
            new ControllerClient(CLUSTER_NAME, childDatacenters.get(i).getControllerConnectString()));
      }
      assertCommand(parentControllerClient.createNewStore(storeName, "owner", "\"string\"", "\"string\""));
      assertCommand(
          parentControllerClient.updateStore(
              storeName,
              new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)));

      // Push v1 and v2 so the store has two versions to roll back between.
      pushAndWaitForCompletion(parentControllerClient, storeName, 1);
      pushAndWaitForCompletion(parentControllerClient, storeName, 2);

      // Rollback strands v2: v1 becomes current again and v2 is left behind as ROLLED_BACK.
      ControllerResponse rollbackResponse = parentControllerClient.rollbackToBackupVersion(storeName);
      assertFalse(rollbackResponse.isError(), "rollback failed: " + rollbackResponse.getError());

      // The parent aggregates the rollback from its child regions asynchronously, so wait until it reports
      // ROLLED_BACK — only then is v2 a cleanup candidate there. The child regions must still hold v2 as
      // ROLLED_BACK while serving v1, which is the cross-fabric evidence the parent acts on.
      assertEverywhere(
          controllerClients,
          storeName,
          TestStrandedVersionCleanupAtStartOfPush::assertRolledBackToVersionOne);

      // The rollback-origin guard rejects a new push while v2 is within its retention window, so wait it
      // out before pushing v3. The buffer absorbs clock skew against the rollback's promote timestamp.
      Thread.sleep(ROLLED_BACK_RETENTION_MS + TimeUnit.SECONDS.toMillis(1));

      // The v3 push is what triggers the start-of-push cleanup on the parent.
      pushAndWaitForCompletion(parentControllerClient, storeName, 3);

      // The delete is issued at start-of-push and propagates over the admin channel, so it can land after
      // the push itself completes.
      assertEverywhere(
          controllerClients,
          storeName,
          TestStrandedVersionCleanupAtStartOfPush::assertStrandedVersionReaped);
    } finally {
      controllerClients.values().forEach(Utils::closeQuietlyWithErrorLogged);
    }
  }

  /**
   * Retries {@code assertion} against every region until they all agree or the wait expires. The assertion receives
   * a region's view of the store along with the label naming that region.
   */
  private static void assertEverywhere(
      Map<String, ControllerClient> controllerClients,
      String storeName,
      BiConsumer<StoreInfo, String> assertion) {
    TestUtils.waitForNonDeterministicAssertion(
        60,
        TimeUnit.SECONDS,
        () -> controllerClients.forEach(
            (label, controllerClient) -> assertion.accept(getStore(controllerClient, storeName, label), label)));
  }

  private static void assertRolledBackToVersionOne(StoreInfo store, String label) {
    assertTrue(
        store.getVersion(2).isPresent() && store.getVersion(2).get().getStatus() == VersionStatus.ROLLED_BACK,
        "Expected v2 to be ROLLED_BACK on " + label + " after rollback, got: " + store.getVersion(2));
    assertEquals(store.getCurrentVersion(), 1, "Expected " + label + " to have rolled back to v1");
  }

  private static void assertStrandedVersionReaped(StoreInfo store, String label) {
    assertFalse(
        store.getVersion(2).isPresent(),
        "Stranded v2 should have been deleted on " + label + " at the start of the v3 push, got: "
            + store.getVersion(2));
    // The cleanup must be narrow: the healthy backup and the freshly pushed version stay put.
    assertTrue(store.getVersion(1).isPresent(), "Healthy backup v1 should not have been deleted on " + label);
    assertTrue(store.getVersion(3).isPresent(), "v3 should exist on " + label + " after its push completed");
    assertEquals(store.getCurrentVersion(), 3, "Expected " + label + " to be serving v3");
  }

  private static void pushAndWaitForCompletion(
      ControllerClient parentControllerClient,
      String storeName,
      int versionNumber) {
    VersionCreationResponse response =
        assertCommand(parentControllerClient.emptyPush(storeName, "push-" + versionNumber, 1000));
    assertEquals(response.getVersion(), versionNumber);
    TestUtils.waitForNonDeterministicPushCompletion(
        Version.composeKafkaTopic(storeName, versionNumber),
        parentControllerClient,
        60,
        TimeUnit.SECONDS);
  }

  private static StoreInfo getStore(ControllerClient controllerClient, String storeName, String label) {
    return assertCommand(controllerClient.getStore(storeName), "getStore failed on " + label).getStore();
  }
}
