package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.offsets.OffsetRecord;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecords;


/**
 * Created by athirupa on 2/3/16.
 */
public interface VeniceConsumer<K, V> {
  public void subscribe(String topic, int partition, OffsetRecord offset);

  public void unSubscribe(String topic, int partition);

  public void resetOffset(String topic, int partition);

  public void close();

  public ConsumerRecords<K, V> poll(long timeout);
}
