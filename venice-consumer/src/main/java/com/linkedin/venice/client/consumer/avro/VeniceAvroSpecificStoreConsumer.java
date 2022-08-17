package com.linkedin.venice.client.consumer.avro;

import com.linkedin.venice.client.consumer.VeniceConsumerRecords;
import com.linkedin.venice.client.consumer.VeniceStoreConsumer;
import java.util.Properties;
import org.apache.avro.specific.SpecificRecord;


public class VeniceAvroSpecificStoreConsumer<K extends SpecificRecord, V extends SpecificRecord>
    extends VeniceStoreConsumer<K, V> {
  public VeniceAvroSpecificStoreConsumer(Properties properties) {
    super(properties);
  }

  @Override
  public VeniceConsumerRecords<K, V> poll(long timeout) {
    return null;
  }
}
