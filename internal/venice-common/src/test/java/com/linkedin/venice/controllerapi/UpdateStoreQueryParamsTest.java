package com.linkedin.venice.controllerapi;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.ENABLE_STORE_MIGRATION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_MIGRATION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.VENICE_UNITS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.WORKLOAD_TYPE;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.meta.ExternalStorageReadMode;
import com.linkedin.venice.meta.IngestionPauseMode;
import com.linkedin.venice.meta.StorageMode;
import com.linkedin.venice.meta.StoreInfo;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.http.NameValuePair;
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
  public void testStorageModeRoundTrip() {
    for (StorageMode mode: StorageMode.values()) {
      UpdateStoreQueryParams params = new UpdateStoreQueryParams();
      params.setStorageMode(mode);
      Assert.assertEquals(params.getStorageMode(), Optional.of(mode));
    }
  }

  @Test
  public void testStorageModeUnsetIsEmpty() {
    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    Assert.assertFalse(params.getStorageMode().isPresent());
  }

  @Test
  public void testVeniceUnitsAndWorkloadTypeTriState() {
    // Not provided at all: leave the config unchanged.
    UpdateStoreQueryParams unset = new UpdateStoreQueryParams();
    Assert.assertFalse(unset.isVeniceUnitsSpecified());
    Assert.assertNull(unset.getVeniceUnits());
    Assert.assertFalse(unset.isWorkloadTypeSpecified());
    Assert.assertNull(unset.getWorkloadType());

    // Provided with a value: set the config.
    UpdateStoreQueryParams set = new UpdateStoreQueryParams().setVeniceUnits(42).setWorkloadType("LOW_LATENCY");
    Assert.assertTrue(set.isVeniceUnitsSpecified());
    Assert.assertEquals(set.getVeniceUnits(), Integer.valueOf(42));
    Assert.assertTrue(set.isWorkloadTypeSpecified());
    Assert.assertEquals(set.getWorkloadType(), "LOW_LATENCY");

    // Provided as null: explicitly clear the config. This must stay distinguishable from "not provided".
    UpdateStoreQueryParams cleared = new UpdateStoreQueryParams().setVeniceUnits(null).setWorkloadType(null);
    Assert.assertTrue(cleared.isVeniceUnitsSpecified());
    Assert.assertNull(cleared.getVeniceUnits());
    Assert.assertTrue(cleared.isWorkloadTypeSpecified());
    Assert.assertNull(cleared.getWorkloadType());

    // A previously set value can be cleared afterwards.
    set.setVeniceUnits(null).setWorkloadType(null);
    Assert.assertTrue(set.isVeniceUnitsSpecified());
    Assert.assertNull(set.getVeniceUnits());
    Assert.assertTrue(set.isWorkloadTypeSpecified());
    Assert.assertNull(set.getWorkloadType());
  }

  @Test
  public void testVeniceUnitsAndWorkloadTypeClearSurvivesWireRoundTrip() {
    // An explicit clear has to survive the query-param serialization used between the client and the
    // controller, otherwise the clear degrades into "leave unchanged" on the server side.
    UpdateStoreQueryParams cleared = new UpdateStoreQueryParams().setVeniceUnits(null).setWorkloadType(null);
    UpdateStoreQueryParams received = new UpdateStoreQueryParams(toWireMap(cleared));
    Assert.assertTrue(received.isVeniceUnitsSpecified());
    Assert.assertNull(received.getVeniceUnits());
    Assert.assertTrue(received.isWorkloadTypeSpecified());
    Assert.assertNull(received.getWorkloadType());

    UpdateStoreQueryParams valued = new UpdateStoreQueryParams().setVeniceUnits(7).setWorkloadType("GENERIC");
    UpdateStoreQueryParams receivedValued = new UpdateStoreQueryParams(toWireMap(valued));
    Assert.assertEquals(receivedValued.getVeniceUnits(), Integer.valueOf(7));
    Assert.assertEquals(receivedValued.getWorkloadType(), "GENERIC");

    UpdateStoreQueryParams receivedUnset = new UpdateStoreQueryParams(toWireMap(new UpdateStoreQueryParams()));
    Assert.assertFalse(receivedUnset.isVeniceUnitsSpecified());
    Assert.assertFalse(receivedUnset.isWorkloadTypeSpecified());
  }

  @Test
  public void testVeniceUnitsAndWorkloadTypeClearSurvivesCloneConfig() {
    // The child controller rebuilds params by cloning only the configs named in updatedConfigsList,
    // so an explicit clear must survive that filtering too.
    UpdateStoreQueryParams source = new UpdateStoreQueryParams().setVeniceUnits(null).setWorkloadType(null);
    UpdateStoreQueryParams target = new UpdateStoreQueryParams();
    target.cloneConfig(VENICE_UNITS, source);
    target.cloneConfig(WORKLOAD_TYPE, source);
    Assert.assertTrue(target.isVeniceUnitsSpecified());
    Assert.assertNull(target.getVeniceUnits());
    Assert.assertTrue(target.isWorkloadTypeSpecified());
    Assert.assertNull(target.getWorkloadType());
  }

  private static Map<String, String> toWireMap(UpdateStoreQueryParams params) {
    Map<String, String> wireMap = new HashMap<>();
    for (NameValuePair pair: params.getNameValuePairs()) {
      wireMap.put(pair.getName(), pair.getValue());
    }
    return wireMap;
  }

  @Test
  public void testThroughputQuotaRoundTrip() {
    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    params.setThroughputQuotaInBytes(123456L);
    params.setThroughputQuotaInRecords(789L);
    Assert.assertEquals(params.getThroughputQuotaInBytes(), Optional.of(123456L));
    Assert.assertEquals(params.getThroughputQuotaInRecords(), Optional.of(789L));

    // -1 is the "no limit" sentinel and must round-trip like any other value.
    params.setThroughputQuotaInBytes(-1L);
    params.setThroughputQuotaInRecords(-1L);
    Assert.assertEquals(params.getThroughputQuotaInBytes(), Optional.of(-1L));
    Assert.assertEquals(params.getThroughputQuotaInRecords(), Optional.of(-1L));
  }

  @Test
  public void testThroughputQuotaUnsetIsEmpty() {
    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    Assert.assertFalse(params.getThroughputQuotaInBytes().isPresent());
    Assert.assertFalse(params.getThroughputQuotaInRecords().isPresent());
  }

  @Test
  public void testStoreInfoCopyIncludesPubSubEncryptionKeyUrn() {
    String pubSubEncryptionKeyUrn = "keyUrn:abc";
    StoreInfo storeInfo = new StoreInfo();
    storeInfo.setEncryptionEnabled(true);
    storeInfo.setPubSubEncryptionKeyUrn(pubSubEncryptionKeyUrn);
    storeInfo.setReplicationMetadataVersionId(-1);
    UpdateStoreQueryParams copiedParams = new UpdateStoreQueryParams(storeInfo, false);
    assertEquals(copiedParams.getPubSubEncryptionKeyUrn(), Optional.of(pubSubEncryptionKeyUrn));
  }

  @Test
  public void testStoreInfoCopyIncludesPubSubEncryptionKeyUrnWhenEncryptionIsDisabled() {
    StoreInfo storeInfo = new StoreInfo();
    String pubSubEncryptionKeyUrn = "keyUrn:abc";
    storeInfo.setPubSubEncryptionKeyUrn(pubSubEncryptionKeyUrn);
    storeInfo.setReplicationMetadataVersionId(-1);

    UpdateStoreQueryParams copiedParams = new UpdateStoreQueryParams(storeInfo, false);

    assertEquals(copiedParams.getPubSubEncryptionKeyUrn(), Optional.of(pubSubEncryptionKeyUrn));
  }

  @Test
  public void testExternalStorageReadModeRoundTrip() {
    for (ExternalStorageReadMode mode: ExternalStorageReadMode.values()) {
      UpdateStoreQueryParams params = new UpdateStoreQueryParams();
      params.setExternalStorageReadMode(mode);
      Assert.assertEquals(params.getExternalStorageReadMode(), Optional.of(mode));
    }
  }

  @Test
  public void testExternalStorageReadModeUnsetIsEmpty() {
    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    Assert.assertFalse(params.getExternalStorageReadMode().isPresent());
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

  @Test
  public void testEtlActiveFabricsRoundTrip() {
    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    List<String> fabrics = Arrays.asList("dc-0", "dc-1");
    params.setEtlActiveFabrics(fabrics);
    Assert.assertEquals(params.getEtlActiveFabrics().get(), fabrics);
  }

  @Test
  public void testEtlActiveFabricsEmpty() {
    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    params.setEtlActiveFabrics(Collections.emptyList());
    Assert.assertTrue(params.getEtlActiveFabrics().get().isEmpty());
  }

  @Test
  public void testEtlActiveFabricsNotSet() {
    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    Assert.assertFalse(params.getEtlActiveFabrics().isPresent());
  }

  @Test
  public void testEtlActiveFabricsNormalizesWhitespace() {
    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    params.setEtlActiveFabrics(Arrays.asList(" dc-0 ", "", " dc-1 "));
    Assert.assertEquals(params.getEtlActiveFabrics().get(), Arrays.asList("dc-0", "dc-1"));
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
