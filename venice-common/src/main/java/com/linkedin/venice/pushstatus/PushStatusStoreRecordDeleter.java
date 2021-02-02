package com.linkedin.venice.pushstatus;

import com.linkedin.venice.common.PushStatusStoreUtils;
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
    VeniceWriter writer = veniceWriterCache.getVeniceWriter(storeName);
    logger.info("Deleting pushStatus of storeName: " + storeName + " , version: " + version);
    for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
      PushStatusKey pushStatusKey = PushStatusStoreUtils.getPushKey(version, partitionId, incrementalPushVersion);
      writer.delete(pushStatusKey, null);
    }
  }

  @Override
  public void close() {
    veniceWriterCache.close();
  }
}
