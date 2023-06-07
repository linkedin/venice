package com.linkedin.venice.pubsub.api;

import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer;


/**
 * Additional configuration required to setup pub-sub clients
 */
public class PubSubClientConfigs {
  private KafkaValueSerializer kafkaValueSerializer;

  public KafkaValueSerializer getKafkaValueSerializer() {
    return kafkaValueSerializer;
  }

  private PubSubClientConfigs(Builder builder) {
    this.kafkaValueSerializer = builder.kafkaValueSerializer;
  }

  public static class Builder {
    private KafkaValueSerializer kafkaValueSerializer;

    public Builder setKafkaValueSerializer(KafkaValueSerializer kafkaValueSerializer) {
      this.kafkaValueSerializer = kafkaValueSerializer;
      return this;
    }

    public void addDefaultConfigs() {
      if (kafkaValueSerializer == null) {
        kafkaValueSerializer = new OptimizedKafkaValueSerializer();
      }
    }

    public PubSubClientConfigs build() {
      addDefaultConfigs();
      return new PubSubClientConfigs(this);
    }
  }
}
