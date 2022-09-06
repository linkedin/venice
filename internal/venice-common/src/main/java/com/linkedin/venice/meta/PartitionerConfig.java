package com.linkedin.venice.meta;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.linkedin.venice.systemstore.schemas.StorePartitionerConfig;
import java.util.Map;


/**
 * This interface defines all the public APIs, and if you need to add accessors to
 * some new fields, this interface needs to be changed accordingly.
 */
@JsonDeserialize(as = PartitionerConfigImpl.class)
public interface PartitionerConfig extends DataModelBackedStructure<StorePartitionerConfig> {
  String getPartitionerClass();

  Map<String, String> getPartitionerParams();

  int getAmplificationFactor();

  void setAmplificationFactor(int amplificationFactor);

  void setPartitionerClass(String partitionerClass);

  void setPartitionerParams(Map<String, String> partitionerParams);

  PartitionerConfig clone();
}
