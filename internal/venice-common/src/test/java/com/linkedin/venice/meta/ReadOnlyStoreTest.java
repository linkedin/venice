package com.linkedin.venice.meta;

import static com.linkedin.venice.utils.ConfigCommonUtils.ActivationState;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.systemstore.schemas.StoreETLConfig;
import com.linkedin.venice.systemstore.schemas.StoreHybridConfig;
import com.linkedin.venice.systemstore.schemas.StorePartitionerConfig;
import com.linkedin.venice.systemstore.schemas.StoreProperties;
import com.linkedin.venice.systemstore.schemas.StoreViewConfig;
import com.linkedin.venice.systemstore.schemas.SystemStoreProperties;
import com.linkedin.venice.utils.TestUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class ReadOnlyStoreTest {
  private static final Logger LOGGER = LogManager.getLogger(ReadOnlyStoreTest.class);

  private Random RANDOM;

  @BeforeClass
  public void setupReadOnlyStore() {
    long seed = System.nanoTime();
    RANDOM = new Random(seed);
    LOGGER.info("Random seed set: {}", seed);
  }

  @Test
  public void testCloneStoreProperties() {

    ZKStore store = TestUtils.populateZKStore(
        (ZKStore) TestUtils.createTestStore(
            Long.toString(RANDOM.nextLong()),
            Long.toString(RANDOM.nextLong()),
            System.currentTimeMillis()),
        RANDOM);

    List<LifecycleHooksRecord> storeLifecycleHooks = new ArrayList<>();
    storeLifecycleHooks.add(new LifecycleHooksRecordImpl("testLifecycleHooksClassName", Collections.emptyMap()));
    store.setStoreLifecycleHooks(storeLifecycleHooks);
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
    assertEqualHybridConfig(storeProperties.getHybridConfig(), store.getHybridStoreConfig());
    assertEqualViewConfig(storeProperties.getViews(), store.getViewConfigs());
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
    assertEqualsETLStoreConfig(storeProperties.getEtlConfig(), store.getEtlStoreConfig());
    assertEqualsPartitionerConfig(storeProperties.getPartitionerConfig(), store.getPartitionerConfig());
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
    assertEquals(storeProperties.getVersions().size(), store.getVersions().size());
    assertEqualsSystemStores(storeProperties.getSystemStores(), store.getSystemStores());
    assertEquals(storeProperties.getStorageNodeReadQuotaEnabled(), store.isStorageNodeReadQuotaEnabled());
    assertEquals(storeProperties.getBlobTransferEnabled(), store.isBlobTransferEnabled());
    assertEquals(storeProperties.getBlobTransferInServerEnabled(), store.getBlobTransferInServerEnabled());
    assertEquals(storeProperties.getBlobTransferInServerEnabled(), ActivationState.NOT_SPECIFIED.name());
    assertEquals(storeProperties.getNearlineProducerCompressionEnabled(), store.isNearlineProducerCompressionEnabled());
    assertEquals(storeProperties.getNearlineProducerCountPerWriter(), store.getNearlineProducerCountPerWriter());
    assertEquals(storeProperties.getStoreLifecycleHooks().size(), store.getStoreLifecycleHooks().size());
  }

  private void assertEqualHybridConfig(StoreHybridConfig actual, HybridStoreConfig expected) {
    assertEquals(actual.getRewindTimeInSeconds(), expected.getRewindTimeInSeconds());
    assertEquals(actual.getOffsetLagThresholdToGoOnline(), expected.getOffsetLagThresholdToGoOnline());
    assertEquals(
        actual.getProducerTimestampLagThresholdToGoOnlineInSeconds(),
        expected.getProducerTimestampLagThresholdToGoOnlineInSeconds());
    assertEquals(actual.getDataReplicationPolicy(), expected.getDataReplicationPolicy().getValue());
    assertEquals(actual.getBufferReplayPolicy(), expected.getBufferReplayPolicy().getValue());
    assertEquals(actual.getRealTimeTopicName(), expected.getRealTimeTopicName());
  }

  private static void assertEqualViewConfig(
      Map<CharSequence, StoreViewConfig> actual,
      Map<String, ViewConfig> expected) {

    for (Map.Entry<CharSequence, StoreViewConfig> viewConfigEntry: actual.entrySet()) {
      StoreViewConfig actualViewConfig = viewConfigEntry.getValue();
      ViewConfig expectedViewConfig = expected.get(viewConfigEntry.getKey().toString());

      assertEquals(actualViewConfig.getViewClassName().toString(), expectedViewConfig.getViewClassName());

      for (Map.Entry<String, CharSequence> viewParamsEntry: actualViewConfig.getViewParameters().entrySet()) {
        CharSequence actualViewParam = viewParamsEntry.getValue();
        String expectedViewParam = expectedViewConfig.getViewParameters().get(viewParamsEntry.getKey());

        assertEquals(actualViewParam.toString(), expectedViewParam);
      }
    }
  }

  private void assertEqualsETLStoreConfig(StoreETLConfig actual, ETLStoreConfig expected) {
    assertEquals(actual.getEtledUserProxyAccount(), expected.getEtledUserProxyAccount());
    assertEquals(actual.getFutureVersionETLEnabled(), expected.isFutureVersionETLEnabled());
    assertEquals(actual.getRegularVersionETLEnabled(), expected.isFutureVersionETLEnabled());
  }

  private void assertEqualsPartitionerConfig(StorePartitionerConfig actual, PartitionerConfig expected) {

    assertEquals(actual.getPartitionerClass(), expected.getPartitionerClass());
    assertEquals(actual.getAmplificationFactor(), expected.getAmplificationFactor());

    for (Map.Entry<CharSequence, CharSequence> entry: actual.getPartitionerParams().entrySet()) {
      CharSequence actualPartitionerParam = entry.getValue();
      String expectedPartitionerParam = expected.getPartitionerParams().get(entry.getKey().toString());

      assertEquals(actualPartitionerParam.toString(), expectedPartitionerParam);
    }
  }

  private void assertEqualsSystemStores(
      Map<CharSequence, SystemStoreProperties> actual,
      Map<String, SystemStoreAttributes> expected) {

    for (Map.Entry<CharSequence, SystemStoreProperties> entry: actual.entrySet()) {
      SystemStoreProperties actualSystemStoreProperties = entry.getValue();
      SystemStoreAttributes expectedSystemStoreAttributes = expected.get(entry.getKey().toString());

      assertEquals(actualSystemStoreProperties.getCurrentVersion(), expectedSystemStoreAttributes.getCurrentVersion());
      assertEquals(
          actualSystemStoreProperties.getVersions().size(),
          expectedSystemStoreAttributes.getVersions().size());
      assertEquals(
          actualSystemStoreProperties.getLatestVersionPromoteToCurrentTimestamp(),
          expectedSystemStoreAttributes.getLatestVersionPromoteToCurrentTimestamp());
      assertEquals(
          actualSystemStoreProperties.getLargestUsedVersionNumber(),
          expectedSystemStoreAttributes.getLargestUsedVersionNumber());
    }
  }
}
