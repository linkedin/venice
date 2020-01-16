package com.linkedin.venice.meta;

import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import java.util.HashMap;
import java.util.Map;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;


@JsonIgnoreProperties(ignoreUnknown = true)
public class PartitionerConfig {
  private String partitionerClass;
  private Map<String, String> partitionerParams;
  private int amplificationFactor;

  public PartitionerConfig(
      @JsonProperty("partitionerClass") String partitionerClass,
      @JsonProperty("partitionerParams") Map<String, String> partitionerParams,
      @JsonProperty("amplificationFactor") int amplificationFactor
  ) {
    this.partitionerClass = partitionerClass;
    this.partitionerParams = partitionerParams;
    this.amplificationFactor = amplificationFactor;
  }

  public PartitionerConfig() {
    this(DefaultVenicePartitioner.class.getName(), new HashMap<>(), 1);
  }

  public String getPartitionerClass() {
    return this.partitionerClass;
  }

  public Map<String, String> getPartitionerParams() {
    return this.partitionerParams;
  }

  public int getAmplificationFactor() {
    return this.amplificationFactor;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    PartitionerConfig that = (PartitionerConfig) o;

    if (!partitionerClass.equals(that.partitionerClass)) return false;
    if (!partitionerParams.equals(that.partitionerParams)) return false;
    return amplificationFactor == that.amplificationFactor;
  }
  @Override
  public int hashCode() {
    int result = partitionerClass.hashCode();
    int partitionerParamsHashCode = partitionerParams == null ? 0 : partitionerParams.toString().hashCode();
    result = 31 * result + (partitionerParamsHashCode ^ (partitionerParamsHashCode >>> 32));
    result = 31 * result + (amplificationFactor ^ (amplificationFactor >>> 32));
    return result;
  }

  @JsonIgnore
  public PartitionerConfig clone(){
    return new PartitionerConfig(partitionerClass, partitionerParams, amplificationFactor);
  }
}
