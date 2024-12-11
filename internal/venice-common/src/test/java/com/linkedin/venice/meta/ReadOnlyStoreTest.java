package com.linkedin.venice.meta;

import static org.testng.Assert.*;

import org.testng.annotations.Test;


public class ReadOnlyStoreTest {
  @Test
  public void testCloneStoreProperties() {

    //
    // ZKStore zkStore = TestUtils.createTestStore("s1", "o1", System.currentTimeMillis());
    //
    //// = new ZKStore(
    //// "store_name",
    //// "store_owner",
    //// System.currentTimeMillis(),
    //// PersistenceType.IN_MEMORY,
    //// RoutingStrategy.CONSISTENT_HASH,
    //// ReadStrategy.ANY_OF_ONLINE,
    //// OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION,
    //// 1);
    //
    // zkStore.setCurrentVersion(1);
    // zkStore.setPartitionCount(new Random().nextInt());
    // zkStore.setLowWatermark(new Random().nextLong());
    // zkStore.setEnableWrites(false);
    // zkStore.setEnableReads(true);
    // zkStore.setStorageQuotaInByte(new Random().nextLong());
    // zkStore.setReadQuotaInCU(new Random().nextLong());
    //
    // HybridStoreConfig hybridStoreConfig = new HybridStoreConfigImpl(
    // new Random().nextLong(),
    // new Random().nextLong(),
    // new Random().nextLong(),
    // DataReplicationPolicy.AGGREGATE,
    // BufferReplayPolicy.REWIND_FROM_EOP
    // );
    // zkStore.setHybridStoreConfig(hybridStoreConfig);
    //
    // zkStore.setViewConfigs();
    // zkStore.setViews(convertViewConfigs(getViewConfigs()));
    // zkStore.setAccessControlled(isAccessControlled());
    // zkStore.setCompressionStrategy(getCompressionStrategy().getValue());
    // zkStore.setClientDecompressionEnabled(getClientDecompressionEnabled());
    // zkStore.setChunkingEnabled(isChunkingEnabled());
    // zkStore.setRmdChunkingEnabled(isRmdChunkingEnabled());
    // zkStore.setBatchGetLimit(getBatchGetLimit());
    // zkStore.setNumVersionsToPreserve(getNumVersionsToPreserve());
    // zkStore.setIncrementalPushEnabled(isIncrementalPushEnabled());
    // zkStore.setSeparateRealTimeTopicEnabled(isSeparateRealTimeTopicEnabled());
    // zkStore.setMigrating(isMigrating());
    // zkStore.setWriteComputationEnabled(isWriteComputationEnabled());
    // zkStore.setReadComputationEnabled(isReadComputationEnabled());
    // zkStore.setBootstrapToOnlineTimeoutInHours(getBootstrapToOnlineTimeoutInHours());
    // zkStore.setNativeReplicationEnabled(isNativeReplicationEnabled());
    // zkStore.setPushStreamSourceAddress(getPushStreamSourceAddress());
    // zkStore.setBackupStrategy(getBackupStrategy().getValue());
    // zkStore.setSchemaAutoRegisteFromPushJobEnabled(isSchemaAutoRegisterFromPushJobEnabled());
    // zkStore.setLatestSuperSetValueSchemaId(getLatestSuperSetValueSchemaId());
    // zkStore.setHybridStoreDiskQuotaEnabled(isHybridStoreDiskQuotaEnabled());
    // zkStore.setStoreMetadataSystemStoreEnabled(isStoreMetadataSystemStoreEnabled());
    // zkStore.setEtlConfig(convertETLStoreConfig(getEtlStoreConfig()));
    // zkStore.setPartitionerConfig(convertPartitionerConfig(getPartitionerConfig()));
    // zkStore.setLatestVersionPromoteToCurrentTimestamp(getLatestVersionPromoteToCurrentTimestamp());
    // zkStore.setBackupVersionRetentionMs(getBackupVersionRetentionMs());
    // zkStore.setMigrationDuplicateStore(isMigrationDuplicateStore());
    // zkStore.setNativeReplicationSourceFabric(getNativeReplicationSourceFabric());
    // zkStore.setDaVinciPushStatusStoreEnabled(isDaVinciPushStatusStoreEnabled());
    // zkStore.setStoreMetadataSystemStoreEnabled(isStoreMetaSystemStoreEnabled());
    // zkStore.setActiveActiveReplicationEnabled(isActiveActiveReplicationEnabled());
    // zkStore.setMinCompactionLagSeconds(getMinCompactionLagSeconds());
    // zkStore.setMaxCompactionLagSeconds(getMaxCompactionLagSeconds());
    // zkStore.setMaxRecordSizeBytes(getMaxRecordSizeBytes());
    // zkStore.setMaxNearlineRecordSizeBytes(getMaxNearlineRecordSizeBytes());
    // zkStore.setUnusedSchemaDeletionEnabled(isUnusedSchemaDeletionEnabled());
    //
    // // Version
    // List<Version> versions = new ArrayList<>();
    // versions.add(new VersionImpl(zkStore.getName(), 0, "push_job_0"));
    // versions.add(new VersionImpl(zkStore.getName(), 1, "push_job_1"));
    // versions.add(new VersionImpl(zkStore.getName(), 2, "push_job_2"));
    // zkStore.setVersions(versions);
    //
    // zkStore.setSystemStores(convertSystemStores(getSystemStores()));
    // zkStore.setStorageNodeReadQuotaEnabled(isStorageNodeReadQuotaEnabled());
    // zkStore.setBlobTransferEnabled(isBlobTransferEnabled());
    // zkStore.setNearlineProducerCompressionEnabled(isNearlineProducerCompressionEnabled());
    // zkStore.setNearlineProducerCountPerWriter(getNearlineProducerCountPerWriter());
    //
    // StoreProperties storeProperties = new StoreProperties();
    // storeProperties.
    //
    //
    // ReadOnlyStore readOnlyStore = new ReadOnlyStore(zkStore);
    //
    // StoreProperties storeProperties = readOnlyStore.cloneStoreProperties();
    //
    // System.out.println(storeProperties.toString());
    return;
  }
}
