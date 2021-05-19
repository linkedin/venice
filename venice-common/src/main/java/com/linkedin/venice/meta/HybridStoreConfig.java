package com.linkedin.venice.meta;

import com.linkedin.venice.systemstore.schemas.StoreHybridConfig;
import org.codehaus.jackson.map.annotate.JsonDeserialize;


/**
 * This interface defines all the public APIs, and if you need to add accessors to
 * some new fields, this interface needs to be changed accordingly.
 */
@JsonDeserialize(as = HybridStoreConfigImpl.class)
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(as = HybridStoreConfigImpl.class)
public interface HybridStoreConfig extends DataModelBackedStructure<StoreHybridConfig> {

  long getRewindTimeInSeconds();

  long getOffsetLagThresholdToGoOnline();

  void setRewindTimeInSeconds(long rewindTimeInSeconds);

  void setOffsetLagThresholdToGoOnline(long offsetLagThresholdToGoOnline);

  long getProducerTimestampLagThresholdToGoOnlineInSeconds();

  HybridStoreConfig clone();
}
