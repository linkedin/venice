package com.linkedin.davinci.client;

import com.linkedin.davinci.stats.AggVersionedDaVinciRecordTransformerStats;
import com.linkedin.venice.exceptions.VeniceException;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Internal config used for {@link DaVinciRecordTransformer}.
 * This is what gets passed into the {@link com.linkedin.davinci.kafka.consumer.StoreIngestionTask}.
 */
public class InternalDaVinciRecordTransformerConfig {
  private final DaVinciRecordTransformerConfig recordTransformerConfig;
  private final AggVersionedDaVinciRecordTransformerStats recordTransformerStats;
  // Default = 0 to guard against NPE downstream, which shouldn't be possible.
  private AtomicInteger startConsumptionLatchCount = new AtomicInteger(0);;

  public InternalDaVinciRecordTransformerConfig(
      DaVinciRecordTransformerConfig recordTransformerConfig,
      AggVersionedDaVinciRecordTransformerStats recordTransformerStats) {
    this.recordTransformerConfig = recordTransformerConfig;
    this.recordTransformerStats = recordTransformerStats;
  }

  public DaVinciRecordTransformerConfig getRecordTransformerConfig() {
    return recordTransformerConfig;
  }

  public AggVersionedDaVinciRecordTransformerStats getRecordTransformerStats() {
    return recordTransformerStats;
  }

  /**
   * @param startConsumptionLatchCount the count used for the latch to guarantee we finish scanning every RocksDB partition before starting remote consumption.
   */
  synchronized public void setStartConsumptionLatchCount(int startConsumptionLatchCount) {
    if (this.startConsumptionLatchCount.get() > 0) {
      throw new VeniceException("startConsumptionLatchCount should only be modified once");
    }
    this.startConsumptionLatchCount.set(startConsumptionLatchCount);
  }

  /**
   * @return {@link #startConsumptionLatchCount}
   */
  synchronized public int getStartConsumptionLatchCount() {
    return startConsumptionLatchCount.get();
  }
}
