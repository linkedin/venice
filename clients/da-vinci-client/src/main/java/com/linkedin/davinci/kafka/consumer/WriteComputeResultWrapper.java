package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;


/**
 * This result wrapper is for non-Active-Active stores.
 */
public class WriteComputeResultWrapper {
  private final Put newPut;
  private final ChunkedValueManifest oldValueManifest;
  /**
   * This can be true when there is some delete op against a non-existing entry.
   */
  private final boolean skipProduce;

  public WriteComputeResultWrapper(Put newPut, ChunkedValueManifest oldValueManifest, boolean skipProduce) {
    this.newPut = newPut;
    this.oldValueManifest = oldValueManifest;
    this.skipProduce = skipProduce;
  }

  public Put getNewPut() {
    return newPut;
  }

  public ChunkedValueManifest getOldValueManifest() {
    return oldValueManifest;
  }

  public boolean isSkipProduce() {
    return skipProduce;
  }
}
