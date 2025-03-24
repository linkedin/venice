package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.lazy.Lazy;
import org.apache.avro.generic.GenericRecord;


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
  private final Lazy<GenericRecord> valueProvider;
  private final Lazy<GenericRecord> oldValueProvider;

  public WriteComputeResultWrapper(Put newPut, ChunkedValueManifest oldValueManifest, boolean skipProduce) {
    this(newPut, oldValueManifest, skipProduce, Lazy.of(() -> null), Lazy.of(() -> null));
  }

  public WriteComputeResultWrapper(
      Put newPut,
      ChunkedValueManifest oldValueManifest,
      boolean skipProduce,
      Lazy<GenericRecord> valueProvider,
      Lazy<GenericRecord> oldValueProvider) {
    this.newPut = newPut;
    this.oldValueManifest = oldValueManifest;
    this.skipProduce = skipProduce;
    this.valueProvider = valueProvider;
    this.oldValueProvider = oldValueProvider;
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

  /**
   * Return a best-effort value provider with the following behaviors:
   *   1. returns the new value provider for PUT and UPDATE.
   *   2. returns null if the value is not available.
   */
  public Lazy<GenericRecord> getValueProvider() {
    return valueProvider;
  }

  /**
   * Return a best-effort old value provider that returns the old value prior to the PUT/UPDATE/DELETE. Returns null if
   * the k/v didn't exist or not available.
   */
  public Lazy<GenericRecord> getOldValueProvider() {
    return oldValueProvider;
  }
}
