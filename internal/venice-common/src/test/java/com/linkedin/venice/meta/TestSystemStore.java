package com.linkedin.venice.meta;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import java.util.Optional;
import org.testng.annotations.Test;


public class TestSystemStore {
  @Test
  public void testSystemStoreAccessor() {
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.META_STORE;
    // Setup zk shared store
    Store zkSharedSystemStore = new ZKStore(
        systemStoreType.getZkSharedStoreName(),
        "test_zk_shared_store_owner",
        10L,
        PersistenceType.IN_MEMORY,
        RoutingStrategy.HASH,
        ReadStrategy.FASTER_OF_TWO_ONLINE,
        OfflinePushStrategy.WAIT_ALL_REPLICAS,
        -1,
        10000L,
        10000L,
        null,
        null,
        3);
    zkSharedSystemStore.setLargestUsedVersionNumber(-1);
    zkSharedSystemStore.setHybridStoreConfig(
        new HybridStoreConfigImpl(
            100,
            100,
            100,
            DataReplicationPolicy.NON_AGGREGATE,
            BufferReplayPolicy.REWIND_FROM_EOP));
    zkSharedSystemStore.setWriteComputationEnabled(true);
    zkSharedSystemStore.setPartitionCount(1);
    // Setup a regular Venice store
    String testStoreName = "test_store";
    Store veniceStore = new ZKStore(
        testStoreName,
        "test_customer",
        0L,
        PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION,
        0,
        1000L,
        1000L,
        null,
        null,
        1);

    SystemStore systemStore = new SystemStore(zkSharedSystemStore, systemStoreType, veniceStore);

    // Test all the get accessors
    assertEquals(systemStore.getName(), systemStoreType.getSystemStoreName(testStoreName));
    assertEquals(systemStore.getOwner(), "test_zk_shared_store_owner");
    assertEquals(systemStore.getCreatedTime(), 10L);
    assertEquals(systemStore.getCurrentVersion(), 0);
    assertEquals(systemStore.getPersistenceType(), PersistenceType.IN_MEMORY);
    assertEquals(systemStore.getRoutingStrategy(), RoutingStrategy.HASH);
    assertEquals(systemStore.getReadStrategy(), ReadStrategy.FASTER_OF_TWO_ONLINE);
    assertEquals(systemStore.getOffLinePushStrategy(), OfflinePushStrategy.WAIT_ALL_REPLICAS);
    assertEquals(systemStore.getLargestUsedVersionNumber(), 0);
    assertEquals(systemStore.getStorageQuotaInByte(), 10000L);
    assertEquals(systemStore.getPartitionCount(), 1);
    assertEquals(systemStore.getPartitionerConfig().getPartitionerClass(), DefaultVenicePartitioner.class.getName());
    assertTrue(systemStore.isEnableReads());
    assertTrue(systemStore.isEnableWrites());
    assertEquals(systemStore.getReadQuotaInCU(), 10000L);
    assertTrue(systemStore.isHybrid());
    assertEquals(systemStore.getCompressionStrategy(), CompressionStrategy.NO_OP);
    assertTrue(systemStore.getClientDecompressionEnabled());
    assertFalse(systemStore.isChunkingEnabled());
    assertEquals(systemStore.getBatchGetLimit(), -1);
    assertFalse(systemStore.isIncrementalPushEnabled());
    assertTrue(systemStore.isAccessControlled());
    assertFalse(systemStore.isMigrating());
    assertEquals(systemStore.getNumVersionsToPreserve(), 0);
    assertTrue(systemStore.isWriteComputationEnabled());
    assertFalse(systemStore.isReadComputationEnabled());
    assertEquals(systemStore.getBootstrapToOnlineTimeoutInHours(), 24);
    assertEquals(systemStore.getPushStreamSourceAddress(), "");
    assertFalse(systemStore.isNativeReplicationEnabled());
    assertEquals(systemStore.getBackupStrategy(), BackupStrategy.DELETE_ON_NEW_PUSH_START);
    assertFalse(systemStore.isSchemaAutoRegisterFromPushJobEnabled());
    assertEquals(systemStore.getLatestSuperSetValueSchemaId(), -1);
    assertFalse(systemStore.isHybridStoreDiskQuotaEnabled());
    assertFalse(systemStore.getEtlStoreConfig().isRegularVersionETLEnabled());
    assertFalse(systemStore.getEtlStoreConfig().isFutureVersionETLEnabled());
    assertFalse(systemStore.isStoreMetadataSystemStoreEnabled());
    assertEquals(systemStore.getLatestVersionPromoteToCurrentTimestamp(), -1);
    assertEquals(systemStore.getBackupVersionRetentionMs(), -1);
    assertEquals(systemStore.getRetentionTime(), 432000000);
    assertEquals(systemStore.getReplicationFactor(), 3);
    assertFalse(systemStore.isMigrationDuplicateStore());
    assertEquals(systemStore.getNativeReplicationSourceFabric(), "");
    assertFalse(systemStore.isDaVinciPushStatusStoreEnabled());

    // All the shared store-level property update should throw exception
    assertThrows(() -> systemStore.setOwner("test"));
    assertThrows(() -> systemStore.setStorageQuotaInByte(1));
    assertThrows(() -> systemStore.setPartitionCount(1));
    assertThrows(() -> systemStore.setPartitionerConfig(null));
    assertThrows(() -> systemStore.setReadQuotaInCU(1));
    assertThrows(() -> systemStore.setHybridStoreConfig(null));
    assertThrows(() -> systemStore.setCompressionStrategy(CompressionStrategy.GZIP));
    assertThrows(() -> systemStore.setClientDecompressionEnabled(false));
    assertThrows(() -> systemStore.setChunkingEnabled(true));
    assertThrows(() -> systemStore.setBatchGetLimit(100));
    assertThrows(() -> systemStore.setIncrementalPushEnabled(true));
    assertThrows(() -> systemStore.setAccessControlled(true));
    assertThrows(() -> systemStore.setMigrating(true));
    assertThrows(() -> systemStore.setNumVersionsToPreserve(2));
    assertThrows(() -> systemStore.setWriteComputationEnabled(true));
    assertThrows(() -> systemStore.setReadComputationEnabled(true));
    assertThrows(() -> systemStore.setBootstrapToOnlineTimeoutInHours(12));
    assertThrows(() -> systemStore.setPushStreamSourceAddress(""));
    assertThrows(() -> systemStore.setNativeReplicationEnabled(true));
    assertThrows(() -> systemStore.setBackupStrategy(BackupStrategy.KEEP_MIN_VERSIONS));
    assertThrows(() -> systemStore.setSchemaAutoRegisterFromPushJobEnabled(true));
    assertThrows(() -> systemStore.setLatestSuperSetValueSchemaId(1));
    assertThrows(() -> systemStore.setHybridStoreDiskQuotaEnabled(true));
    assertThrows(() -> systemStore.setEtlStoreConfig(null));
    assertThrows(() -> systemStore.setStoreMetadataSystemStoreEnabled(true));
    assertThrows(() -> systemStore.setBackupVersionRetentionMs(11));
    assertThrows(() -> systemStore.setReplicationFactor(1));
    assertThrows(() -> systemStore.setMigrationDuplicateStore(true));
    assertThrows(() -> systemStore.setNativeReplicationSourceFabric(""));
    assertThrows(() -> systemStore.setDaVinciPushStatusStoreEnabled(true));

    // SystemStores property for SystemStore is not supported.
    assertThrows(() -> systemStore.getSystemStores());
    assertThrows(() -> systemStore.setSystemStores(null));
    assertThrows(() -> systemStore.putSystemStore(VeniceSystemStoreType.META_STORE, new SystemStoreAttributesImpl()));

    // Test the non-sharable property update
    systemStore.setCurrentVersion(10);
    assertEquals(systemStore.getCurrentVersion(), 10);
    systemStore.setLargestUsedVersionNumber(10);
    assertEquals(systemStore.getLargestUsedVersionNumber(), 10);
    systemStore.setLatestVersionPromoteToCurrentTimestamp(100);
    assertEquals(systemStore.getLatestVersionPromoteToCurrentTimestamp(), 100);

    // Test version related operations
    systemStore.addVersion(new VersionImpl(systemStore.getName(), 1, "test_push_id_1"));
    assertTrue(systemStore.containsVersion(1));
    Optional<Version> version = systemStore.getVersion(1);
    assertTrue(version.isPresent(), "Version 1 must be present");
    systemStore.deleteVersion(1);
    assertFalse(systemStore.containsVersion(1));
  }
}
