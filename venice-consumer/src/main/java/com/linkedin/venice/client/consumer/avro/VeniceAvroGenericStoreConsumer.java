package com.linkedin.venice.client.consumer.avro;

import com.linkedin.venice.client.consumer.VeniceConsumerRecords;
import com.linkedin.venice.client.consumer.VeniceStoreConsumer;
import java.util.Properties;


public class VeniceAvroGenericStoreConsumer<K, V> extends VeniceStoreConsumer<K, V> {
  public VeniceAvroGenericStoreConsumer(Properties properties) {
    super(properties);
  }

  @Override
  public VeniceConsumerRecords<K, V> poll(long timeout) {
    return null;
  }
}
