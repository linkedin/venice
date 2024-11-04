package com.linkedin.venice.writer;

import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import java.util.Objects;


/**
 * VeniceWriterOptions is used to pass arguments to VeniceWriter constructor.
 * Before passing VeniceWriterOptions object to VeniceWriter, VeniceWriterFactory
 * methods use it to set some configs.
 */
public class VeniceWriterOptions {
  private final String topicName;
  // TODO: Update to use generic serializers
  private final VeniceKafkaSerializer keySerializer;
  private final VeniceKafkaSerializer valueSerializer;
  private final VeniceKafkaSerializer writeComputeSerializer;
  private final VenicePartitioner partitioner;
  private final Time time;
  private final Integer partitionCount;
  private final boolean chunkingEnabled;
  private final boolean rmdChunkingEnabled;
  private final int maxRecordSizeBytes;
  // Set this field if you want to use different broker address than the local broker address
  private final String brokerAddress;
  private final boolean producerCompressionEnabled;
  private final int producerCount;
  private final int producerThreadCount;
  private final int producerQueueSize;

  public String getBrokerAddress() {
    return brokerAddress;
  }

  public String getTopicName() {
    return topicName;
  }

  public VeniceKafkaSerializer getKeySerializer() {
    return keySerializer;
  }

  public VeniceKafkaSerializer getValueSerializer() {
    return valueSerializer;
  }

  public VeniceKafkaSerializer getWriteComputeSerializer() {
    return writeComputeSerializer;
  }

  public VenicePartitioner getPartitioner() {
    return partitioner;
  }

  public Time getTime() {
    return time;
  }

  public Integer getPartitionCount() {
    return partitionCount;
  }

  public boolean isChunkingEnabled() {
    return chunkingEnabled;
  }

  public boolean isRmdChunkingEnabled() {
    return rmdChunkingEnabled;
  }

  public int getMaxRecordSizeBytes() {
    return maxRecordSizeBytes;
  }

  public boolean isProducerCompressionEnabled() {
    return producerCompressionEnabled;
  }

  public int getProducerCount() {
    return producerCount;
  }

  public int getProducerThreadCount() {
    return producerThreadCount;
  }

  public int getProducerQueueSize() {
    return producerQueueSize;
  }

  private VeniceWriterOptions(Builder builder) {
    topicName = builder.topicName;
    keySerializer = builder.keySerializer;
    valueSerializer = builder.valueSerializer;
    writeComputeSerializer = builder.writeComputeSerializer;
    partitioner = builder.partitioner;
    time = builder.time;
    partitionCount = builder.partitionCount;
    chunkingEnabled = builder.chunkingEnabled;
    rmdChunkingEnabled = builder.rmdChunkingEnabled;
    maxRecordSizeBytes = builder.maxRecordSizeBytes;
    brokerAddress = builder.brokerAddress;
    producerCompressionEnabled = builder.producerCompressionEnabled;
    producerCount = builder.producerCount;
    producerThreadCount = builder.producerThreadCount;
    producerQueueSize = builder.producerQueueSize;
  }

  @Override
  public String toString() {
    return new StringBuilder("VeniceWriterOptions:{").append("topic:")
        .append(topicName)
        .append(", ")
        .append("brokerAddress:")
        .append(brokerAddress)
        .append(", ")
        .append("chunkingEnabled:")
        .append(chunkingEnabled)
        .append(", ")
        .append("partitionCount:")
        .append(partitionCount != null ? partitionCount : "-")
        .append(", ")
        .append("chunkingEnabled:")
        .append(chunkingEnabled)
        .append(", ")
        .append("rmdChunkingEnabled:")
        .append(rmdChunkingEnabled)
        .append(", ")
        .append("maxRecordSizeBytes:")
        .append(maxRecordSizeBytes)
        .append(", ")
        .append("producerCompressionEnabled:")
        .append(producerCompressionEnabled)
        .append(", ")
        .append("producerCount:")
        .append(producerCount)
        .append(", ")
        .append("producerThreadCount:")
        .append(producerThreadCount)
        .append(", ")
        .append("producerQueueSize:")
        .append(producerQueueSize)
        .append("}")
        .toString();
  }

  public static class Builder {
    private final String topicName;
    private VeniceKafkaSerializer keySerializer = null;
    private VeniceKafkaSerializer valueSerializer = null;
    private VeniceKafkaSerializer writeComputeSerializer = null;
    private VenicePartitioner partitioner = null;
    private Time time = null;
    private Integer partitionCount = null; // default null
    private boolean chunkingEnabled; // default false
    private boolean rmdChunkingEnabled; // default false
    private int maxRecordSizeBytes = VeniceWriter.UNLIMITED_MAX_RECORD_SIZE; // default -1
    private String brokerAddress = null; // default null
    private boolean producerCompressionEnabled = true;
    private int producerCount = 1;
    private int producerThreadCount = 1;
    private int producerQueueSize = 5 * 1024 * 1024; // 5MB by default

    private void addDefaults() {
      if (keySerializer == null) {
        keySerializer = new DefaultSerializer();
      }
      if (valueSerializer == null) {
        valueSerializer = new DefaultSerializer();
      }
      if (writeComputeSerializer == null) {
        writeComputeSerializer = new DefaultSerializer();
      }
      if (partitioner == null) {
        partitioner = new DefaultVenicePartitioner();
      }
      if (time == null) {
        time = SystemTime.INSTANCE;
      }
    }

    public VeniceWriterOptions build() {
      addDefaults();
      return new VeniceWriterOptions(this);
    }

    public Builder setBrokerAddress(String brokerAddress) {
      this.brokerAddress = brokerAddress;
      return this;
    }

    public Builder setUseKafkaKeySerializer(boolean useKafkaKeySerializer) {
      if (useKafkaKeySerializer) {
        this.keySerializer = new KafkaKeySerializer();
      }
      return this;
    }

    public Builder setChunkingEnabled(boolean chunkingEnabled) {
      this.chunkingEnabled = chunkingEnabled;
      return this;
    }

    public Builder setRmdChunkingEnabled(boolean rmdChunkingEnabled) {
      this.rmdChunkingEnabled = rmdChunkingEnabled;
      return this;
    }

    public Builder(String topic) {
      this.topicName = Objects.requireNonNull(topic, "Topic name cannot be null for VeniceWriterOptions");
    }

    public Builder setKeySerializer(VeniceKafkaSerializer keySerializer) {
      this.keySerializer = keySerializer;
      return this;
    }

    public Builder setValueSerializer(VeniceKafkaSerializer valueSerializer) {
      this.valueSerializer = valueSerializer;
      return this;
    }

    public Builder setWriteComputeSerializer(VeniceKafkaSerializer writeComputeSerializer) {
      this.writeComputeSerializer = writeComputeSerializer;
      return this;
    }

    public Builder setPartitioner(VenicePartitioner partitioner) {
      this.partitioner = partitioner;
      return this;
    }

    public Builder setTime(Time time) {
      this.time = time;
      return this;
    }

    public Builder setPartitionCount(Integer partitionCount) {
      this.partitionCount = partitionCount;
      return this;
    }

    public Builder setMaxRecordSizeBytes(int maxRecordSizeBytes) {
      this.maxRecordSizeBytes = maxRecordSizeBytes;
      return this;
    }

    public Builder setProducerCompressionEnabled(boolean producerCompressionEnabled) {
      this.producerCompressionEnabled = producerCompressionEnabled;
      return this;
    }

    public Builder setProducerCount(int producerCount) {
      this.producerCount = producerCount;
      return this;
    }

    public Builder setProducerThreadCount(int producerThreadCount) {
      this.producerThreadCount = producerThreadCount;
      return this;
    }

    public Builder setProducerQueueSize(int producerQueueSize) {
      this.producerQueueSize = producerQueueSize;
      return this;
    }
  }
}
