package com.linkedin.venice.controllerapi;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.ENABLE_STORE_MIGRATION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_MIGRATION;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.meta.IngestionPauseMode;
import com.linkedin.venice.meta.StoreInfo;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.testng.Assert;
import org.testng.annotations.Test;


public class UpdateStoreQueryParamsTest {
  @Test
  public void testSetStoreMigrationSetsBothKeys() {
    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    params.setStoreMigration(true);
    assertEquals(params.getStoreMigration(), Optional.of(Boolean.TRUE));
    assertEquals(params.getString(STORE_MIGRATION).orElse(null), "true");
    assertEquals(params.getString(ENABLE_STORE_MIGRATION).orElse(null), "true");

    params = new UpdateStoreQueryParams();
    params.setStoreMigration(false);
    assertEquals(params.getStoreMigration(), Optional.of(Boolean.FALSE));
    assertEquals(params.getString(STORE_MIGRATION).orElse(null), "false");
    assertEquals(params.getString(ENABLE_STORE_MIGRATION).orElse(null), "false");
    params.setEnumSchemaEvolutionAllowed(true);
    assertEquals(params.isEnumSchemaEvolutionAllowed(), Optional.of(Boolean.TRUE));
  }

  @Test
  public void testIngestionPauseModeRoundTrip() {
    for (IngestionPauseMode mode: IngestionPauseMode.values()) {
      UpdateStoreQueryParams params = new UpdateStoreQueryParams();
      params.setIngestionPauseMode(mode);
      Assert.assertEquals(params.getIngestionPauseMode(), Optional.of(mode));
    }
  }

  @Test
  public void testIngestionPausedRegionsRoundTrip() {
    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    List<String> regions = Arrays.asList("prod-lor1", "prod-ltx1");
    params.setIngestionPausedRegions(regions);
    Assert.assertEquals(params.getIngestionPausedRegions().get(), regions);
  }

  @Test
  public void testIngestionPausedRegionsEmpty() {
    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    params.setIngestionPausedRegions(Collections.emptyList());
    Assert.assertTrue(params.getIngestionPausedRegions().get().isEmpty());
  }

  /**
   * During cross-cluster store migration the source store's replication factor must NOT be carried
   * onto the destination — destination's createNewStore has already applied the dest cluster's
   * default RF, and the dest cluster's topology (e.g. number of Helix fault zones) may require a
   * different RF than the source. Carrying the source RF would clobber the dest cluster default
   * via the subsequent updateStore call, leaving the migrated store with the wrong RF.
   */
  @Test
  public void testReplicationFactorOmittedDuringStoreMigration() {
    StoreInfo srcStore = new StoreInfo();
    srcStore.setReplicationFactor(3);
    srcStore.setReplicationMetadataVersionId(-1);

    UpdateStoreQueryParams params = new UpdateStoreQueryParams(srcStore, /*storeMigrating=*/true);

    assertEquals(
        params.getReplicationFactor(),
        Optional.empty(),
        "Replication factor must not be carried over during cross-cluster store migration; "
            + "the destination cluster's createNewStore default must win.");
    // Sanity: the migration-specific flags should still be present
    assertEquals(params.getStoreMigration(), Optional.of(Boolean.TRUE));
    assertEquals(params.getMigrationDuplicateStore(), Optional.of(Boolean.TRUE));
    assertEquals(params.getLargestUsedVersionNumber(), Optional.of(0));
  }

  /**
   * The non-migration path (within-cluster fabric copy / metadata recovery, e.g. the caller at
   * VeniceParentHelixAdmin#copyOverStoreMetadataFromSrcFabricToDestFabric) MUST preserve the
   * source store's replication factor — the source and destination are the same cluster, so the
   * RF should not change.
   */
  @Test
  public void testReplicationFactorPreservedWhenNotMigrating() {
    StoreInfo srcStore = new StoreInfo();
    srcStore.setReplicationFactor(7);
    srcStore.setReplicationMetadataVersionId(-1);

    UpdateStoreQueryParams params = new UpdateStoreQueryParams(srcStore, /*storeMigrating=*/false);

    assertEquals(
        params.getReplicationFactor(),
        Optional.of(7),
        "Replication factor must be preserved on the non-migration code path "
            + "(within-cluster fabric copy / metadata recovery).");
    // Migration-specific flags must NOT be set on this path
    assertEquals(params.getStoreMigration(), Optional.empty());
    assertEquals(params.getMigrationDuplicateStore(), Optional.empty());
  }
}
