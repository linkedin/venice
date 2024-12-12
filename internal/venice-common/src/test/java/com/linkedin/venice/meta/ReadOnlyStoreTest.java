package com.linkedin.venice.meta;

import static org.testng.Assert.*;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.systemstore.schemas.StoreProperties;
import com.linkedin.venice.utils.TestUtils;
import java.util.Random;
import org.testng.annotations.Test;


public class ReadOnlyStoreTest {
  @Test
  public void testCloneStoreProperties() {

    Random random = new Random();

    ZKStore store = populateZKStore(
        (ZKStore) TestUtils.createTestStore(
            Long.toString(random.nextLong()),
            Long.toString(random.nextLong()),
            System.currentTimeMillis()));

    ReadOnlyStore readOnlyStore = new ReadOnlyStore(store);
    StoreProperties storeProperties = readOnlyStore.cloneStoreProperties();

    // Assert
    assertEquals(storeProperties.getName(), store.getName());
    assertEquals(storeProperties.getOwner(), store.getOwner());
    assertEquals(storeProperties.getCreatedTime(), store.getCreatedTime());
    assertEquals(storeProperties.getCurrentVersion(), store.getCurrentVersion());
    assertEquals(storeProperties.getPartitionCount(), store.getPartitionCount());
    assertEquals(storeProperties.getLowWatermark(), store.getLowWatermark());
    assertEquals(storeProperties.getEnableWrites(), store.isEnableWrites());
    assertEquals(storeProperties.getEnableReads(), store.isEnableReads());
    assertEquals(storeProperties.getStorageQuotaInByte(), store.getStorageQuotaInByte());
    assertEquals(storeProperties.getPersistenceType(), store.getPersistenceType().value);
    assertEquals(storeProperties.getRoutingStrategy(), store.getRoutingStrategy().value);
    assertEquals(storeProperties.getReadStrategy(), store.getReadStrategy().value);
    assertEquals(storeProperties.getOfflinePushStrategy(), store.getOffLinePushStrategy().value);
    assertEquals(storeProperties.getLargestUsedVersionNumber(), store.getLargestUsedVersionNumber());
    assertEquals(storeProperties.getReadQuotaInCU(), store.getReadQuotaInCU());

    // TODO Hybrid Config
    assertEquals(storeProperties.getName(), store.getName());

    // TODO Views
    assertEquals(storeProperties.getName(), store.getName());

    assertEquals(storeProperties.getAccessControlled(), store.isAccessControlled());
    assertEquals(storeProperties.getCompressionStrategy(), store.getCompressionStrategy().getValue());
    assertEquals(storeProperties.getClientDecompressionEnabled(), store.getClientDecompressionEnabled());
    assertEquals(storeProperties.getChunkingEnabled(), store.isChunkingEnabled());
    assertEquals(storeProperties.getRmdChunkingEnabled(), store.isRmdChunkingEnabled());
    assertEquals(storeProperties.getBatchGetLimit(), store.getBatchGetLimit());
    assertEquals(storeProperties.getNumVersionsToPreserve(), store.getNumVersionsToPreserve());
    assertEquals(storeProperties.getIncrementalPushEnabled(), store.isIncrementalPushEnabled());
    assertEquals(storeProperties.getSeparateRealTimeTopicEnabled(), store.isSeparateRealTimeTopicEnabled());
    assertEquals(storeProperties.getMigrating(), store.isMigrating());
    assertEquals(storeProperties.getWriteComputationEnabled(), store.isWriteComputationEnabled());
    assertEquals(storeProperties.getReadComputationEnabled(), store.isReadComputationEnabled());
    assertEquals(storeProperties.getBootstrapToOnlineTimeoutInHours(), store.getBootstrapToOnlineTimeoutInHours());
    assertEquals(storeProperties.getNativeReplicationEnabled(), store.isNativeReplicationEnabled());
    assertEquals(storeProperties.getPushStreamSourceAddress(), store.getPushStreamSourceAddress());
    assertEquals(storeProperties.getBackupStrategy(), store.getBackupStrategy().getValue());
    assertEquals(
        storeProperties.getSchemaAutoRegisteFromPushJobEnabled(),
        store.isSchemaAutoRegisterFromPushJobEnabled());
    assertEquals(storeProperties.getLatestSuperSetValueSchemaId(), store.getLatestSuperSetValueSchemaId());
    assertEquals(storeProperties.getHybridStoreDiskQuotaEnabled(), store.isHybridStoreDiskQuotaEnabled());
    assertEquals(storeProperties.getStoreMetaSystemStoreEnabled(), store.isStoreMetaSystemStoreEnabled());

    // TODO ETL Store Config
    assertEquals(storeProperties.getName(), store.getName());

    // TODO Create Test Partitioner Config
    assertEquals(storeProperties.getName(), store.getName());

    assertEquals(
        storeProperties.getLatestVersionPromoteToCurrentTimestamp(),
        store.getLatestVersionPromoteToCurrentTimestamp());
    assertEquals(storeProperties.getBackupVersionRetentionMs(), store.getBackupVersionRetentionMs());
    assertEquals(storeProperties.getMigrationDuplicateStore(), store.isMigrationDuplicateStore());
    assertEquals(storeProperties.getNativeReplicationEnabled(), store.isNativeReplicationEnabled());
    assertEquals(storeProperties.getDaVinciPushStatusStoreEnabled(), store.isDaVinciPushStatusStoreEnabled());
    assertEquals(storeProperties.getStoreMetadataSystemStoreEnabled(), store.isStoreMetadataSystemStoreEnabled());
    assertEquals(storeProperties.getActiveActiveReplicationEnabled(), store.isActiveActiveReplicationEnabled());
    assertEquals(storeProperties.getMinCompactionLagSeconds(), store.getMinCompactionLagSeconds());
    assertEquals(storeProperties.getMaxCompactionLagSeconds(), store.getMaxCompactionLagSeconds());
    assertEquals(storeProperties.getMaxRecordSizeBytes(), store.getMaxRecordSizeBytes());
    assertEquals(storeProperties.getMaxNearlineRecordSizeBytes(), store.getMaxNearlineRecordSizeBytes());
    assertEquals(storeProperties.getUnusedSchemaDeletionEnabled(), store.isUnusedSchemaDeletionEnabled());

    // TODO Versions
    assertEquals(storeProperties.getName(), store.getName());

    // TODO System Stores
    assertEquals(storeProperties.getName(), store.getName());

    assertEquals(storeProperties.getStorageNodeReadQuotaEnabled(), store.isStorageNodeReadQuotaEnabled());
    assertEquals(storeProperties.getBlobTransferEnabled(), store.isBlobTransferEnabled());
    assertEquals(storeProperties.getNearlineProducerCompressionEnabled(), store.isNearlineProducerCompressionEnabled());
    assertEquals(storeProperties.getNearlineProducerCountPerWriter(), store.getNearlineProducerCountPerWriter());

    System.out.println(storeProperties.toString());
  }

  private static ZKStore populateZKStore(ZKStore store) {
    Random random = new Random();
    store.setCurrentVersion(random.nextInt());
    store.setPartitionCount(random.nextInt());
    store.setLowWatermark(random.nextLong());
    store.setEnableWrites(false);
    store.setEnableReads(true);
    store.setStorageQuotaInByte(random.nextLong());
    store.setReadQuotaInCU(random.nextLong());
    store.setHybridStoreConfig(TestUtils.createTestHybridStoreConfig());
    store.setViewConfigs(TestUtils.createTestViewConfigs());
    store.setCompressionStrategy(CompressionStrategy.GZIP);
    store.setClientDecompressionEnabled(true);
    store.setChunkingEnabled(true);
    store.setRmdChunkingEnabled(true);
    store.setBatchGetLimit(random.nextInt());
    store.setNumVersionsToPreserve(random.nextInt());
    store.setIncrementalPushEnabled(true);
    store.setSeparateRealTimeTopicEnabled(true);
    store.setMigrating(true);
    store.setWriteComputationEnabled(true);
    store.setReadComputationEnabled(true);
    store.setBootstrapToOnlineTimeoutInHours(random.nextInt());
    store.setNativeReplicationEnabled(true);
    store.setPushStreamSourceAddress("push_stream_source");
    store.setBackupStrategy(BackupStrategy.DELETE_ON_NEW_PUSH_START);
    store.setSchemaAutoRegisterFromPushJobEnabled(true);
    store.setLatestSuperSetValueSchemaId(random.nextInt());
    store.setHybridStoreDiskQuotaEnabled(true);
    store.setStoreMetaSystemStoreEnabled(true);
    store.setEtlStoreConfig(TestUtils.createTestETLStoreConfig());
    store.setPartitionerConfig(TestUtils.createTestPartitionerConfig());
    store.setLatestVersionPromoteToCurrentTimestamp(random.nextLong());
    store.setBackupVersionRetentionMs(random.nextLong());
    store.setMigrationDuplicateStore(true);
    store.setNativeReplicationSourceFabric("native_replication_source_fabric");
    store.setDaVinciPushStatusStoreEnabled(true);
    store.setStoreMetadataSystemStoreEnabled(true);
    store.setActiveActiveReplicationEnabled(true);
    store.setMinCompactionLagSeconds(random.nextLong());
    store.setMaxCompactionLagSeconds(random.nextLong());
    store.setMaxRecordSizeBytes(random.nextInt());
    store.setMaxNearlineRecordSizeBytes(random.nextInt());
    store.setUnusedSchemaDeletionEnabled(true);
    store.setVersions(TestUtils.createTestVersions(store.getName()));
    store.setSystemStores(TestUtils.createTestSystemStores(store.getName()));
    store.setStorageNodeReadQuotaEnabled(true);
    store.setBlobTransferEnabled(true);
    store.setNearlineProducerCompressionEnabled(true);
    store.setNearlineProducerCountPerWriter(random.nextInt());
    return store;
  }
}
