package com.linkedin.venice.pushstatushelper;

import com.linkedin.venice.common.PushStatusStoreUtils;
import com.linkedin.venice.pushstatus.PushStatusKey;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.util.Optional;
import org.apache.log4j.Logger;


/**
 * PushStatusRecordDeleter is a class help controller purge push status of outdated versions.
 */
public class PushStatusStoreRecordDeleter implements AutoCloseable {
  private static final Logger logger = Logger.getLogger(PushStatusStoreRecordDeleter.class);
  private final PushStatusStoreVeniceWriterCache veniceWriterCache;

  public PushStatusStoreRecordDeleter(VeniceWriterFactory veniceWriterFactory) {
    this.veniceWriterCache = new PushStatusStoreVeniceWriterCache(veniceWriterFactory);
  }

  public void deletePushStatus(String storeName, int version, Optional<String> incrementalPushVersion, int partitionCount) {
    VeniceWriter writer = veniceWriterCache.prepareVeniceWriter(storeName);
    logger.info("Deleting pushStatus of storeName: " + storeName + " , version: " + version);
    for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
      PushStatusKey pushStatusKey = PushStatusStoreUtils.getPushKey(version, partitionId, incrementalPushVersion);
      writer.delete(pushStatusKey, null);
    }
  }

  public void removePushStatusStoreVeniceWriter(String storeName) {
    veniceWriterCache.removeVeniceWriter(storeName);
  }

  public VeniceWriter getPushStatusStoreVeniceWriter(String storeName) {
    return veniceWriterCache.getVeniceWriterFromMap(storeName);
  }

  @Override
  public void close() {
    veniceWriterCache.close();
  }
}
