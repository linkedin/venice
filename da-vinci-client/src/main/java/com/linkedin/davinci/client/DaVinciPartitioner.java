package com.linkedin.davinci.client;

import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.utils.ReflectUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.nio.ByteBuffer;
import java.util.Properties;


public class DaVinciPartitioner extends VenicePartitioner {
  VenicePartitioner partitioner;
  int numPartitions;
  int amplificationFactor;

  public DaVinciPartitioner(PartitionerConfig partitionerConfig) {
    this.numPartitions = numPartitions;
    this.amplificationFactor = partitionerConfig.getAmplificationFactor();
    Properties params = new Properties();
    params.putAll(partitionerConfig.getPartitionerParams());
    VeniceProperties partitionerProperties = new VeniceProperties(params);
    Class<? extends VenicePartitioner> partitionerClass = ReflectUtils.loadClass(partitionerConfig.getPartitionerClass());
    this.partitioner = ReflectUtils.callConstructor(partitionerClass,
        new Class<?>[]{VeniceProperties.class}, new Object[]{partitionerProperties});
  }

  @Override
  public int getPartitionId(byte[] keyBytes, int numPartitions) {
    return this.partitioner.getPartitionId(keyBytes, numPartitions);
  }

  @Override
  public int getPartitionId(ByteBuffer keyByteBuffer, int numPartitions) {
    return this.partitioner.getPartitionId(keyByteBuffer, numPartitions);
  }
}
