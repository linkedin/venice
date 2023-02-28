package com.linkedin.venice.pushstatushelper;

import com.linkedin.venice.common.PushStatusStoreUtils;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pushstatus.PushStatusKey;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.util.Optional;
import java.util.concurrent.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * PushStatusRecordDeleter is a class help controller purge push status of outdated versions.
 */
public class PushStatusStoreRecordDeleter implements AutoCloseable {
  private static final Logger LOGGER = LogManager.getLogger(PushStatusStoreRecordDeleter.class);
  private final PushStatusStoreVeniceWriterCache veniceWriterCache;

  public PushStatusStoreRecordDeleter(VeniceWriterFactory veniceWriterFactory) {
    this.veniceWriterCache = new PushStatusStoreVeniceWriterCache(veniceWriterFactory);
  }

  public void deletePushStatus(
      String storeName,
      int version,
      Optional<String> incrementalPushVersion,
      int partitionCount) {
    VeniceWriter writer = veniceWriterCache.prepareVeniceWriter(storeName);
    LOGGER.info("Deleting pushStatus of storeName: {}, version: {}", storeName, version);
    for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
      PushStatusKey pushStatusKey = PushStatusStoreUtils.getPushKey(version, partitionId, incrementalPushVersion);
      writer.delete(pushStatusKey, null);
    }
  }

  /**
   * N.B.: Currently used by tests only.
   * @return
   */
  public Future<PubSubProduceResult> deletePartitionIncrementalPushStatus(
      String storeName,
      int version,
      String incrementalPushVersion,
      int partitionId) {
    PushStatusKey pushStatusKey = PushStatusStoreUtils.getPushKey(
        version,
        partitionId,
        Optional.ofNullable(incrementalPushVersion),
        Optional.of(PushStatusStoreUtils.SERVER_INCREMENTAL_PUSH_PREFIX));
    LOGGER.info(
        "Deleting incremental push status belonging to a partition:{}. pushStatusKey:{}",
        partitionId,
        pushStatusKey);
    return veniceWriterCache.prepareVeniceWriter(storeName).delete(pushStatusKey, null);
  }

  public void removePushStatusStoreVeniceWriter(String storeName) {
    LOGGER.info("Removing push status store writer for store {}", storeName);
    long veniceWriterRemovingStartTimeInNs = System.nanoTime();
    veniceWriterCache.removeVeniceWriter(storeName);
    LOGGER.info(
        "Removed push status store writer for store {} in {}ms.",
        storeName,
        LatencyUtils.getLatencyInMS(veniceWriterRemovingStartTimeInNs));
  }

  @Override
  public void close() {
    LOGGER.info("Closing VeniceWriter cache");
    long cacheClosingStartTimeInNs = System.nanoTime();
    veniceWriterCache.close();
    LOGGER.info("Closed VeniceWriter cache in {}ms.", LatencyUtils.getLatencyInMS(cacheClosingStartTimeInNs));
  }
}
