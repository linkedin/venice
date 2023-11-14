package com.linkedin.venice.pushstatushelper;

import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.writer.VeniceWriterFactory;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * PushStatusRecordDeleter is a class help controller purge push status of outdated versions.
 */
public class PushStatusStoreRecordDeleter implements AutoCloseable {
  private static final Logger LOGGER = LogManager.getLogger(PushStatusStoreRecordDeleter.class);
  private final PushStatusStoreVeniceWriterCache veniceWriterCache;

  public PushStatusStoreRecordDeleter(VeniceWriterFactory veniceWriterFactory, Schema updateSchema) {
    this.veniceWriterCache = new PushStatusStoreVeniceWriterCache(veniceWriterFactory, updateSchema);
  }

  @Override
  public void close() {
    LOGGER.info("Closing VeniceWriter cache");
    long cacheClosingStartTimeInNs = System.nanoTime();
    veniceWriterCache.close();
    LOGGER.info("Closed VeniceWriter cache in {}ms.", LatencyUtils.getLatencyInMS(cacheClosingStartTimeInNs));
  }
}
