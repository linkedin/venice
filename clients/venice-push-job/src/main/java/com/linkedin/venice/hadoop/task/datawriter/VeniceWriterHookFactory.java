package com.linkedin.venice.hadoop.task.datawriter;

import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriterHook;


/**
 * Optional VPJ executor-side factory for the hook attached to the primary data {@code VeniceWriter}.
 *
 * <p>Implementations must have a public no-arg constructor. VPJ initializes the factory inside the executor task JVM
 * and invokes {@link #createWriterHook(String, VeniceProperties)} exactly once per partition writer.
 *
 * <p>The hook is attached only to the primary data writer. It is not attached to control-message,
 * heartbeat, or materialized-view child writers.
 */
public interface VeniceWriterHookFactory {
  /**
   * Creates the hook for a VPJ partition writer.
   *
   * @param storeName the destination Venice store name
   * @param taskProperties the executor task properties, including any {@code push.job.writer.hook.*} settings
   * @return the non-null hook to attach to the primary data writer
   */
  VeniceWriterHook createWriterHook(String storeName, VeniceProperties taskProperties);
}
