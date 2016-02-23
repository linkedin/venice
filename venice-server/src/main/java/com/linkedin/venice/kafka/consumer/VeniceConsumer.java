package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.offsets.OffsetRecord;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecords;


/**
 * Created by athirupa on 2/3/16.
 */
public interface VeniceConsumer<K,V> {
    public long getLastOffset(String topic, int partition);

    public void subscribe(String topic, int partition, OffsetRecord offset);

    public void unSubscribe(String topic, int partition);

    public long resetOffset(String topic, int partition);

    public void seek(String topic, int partition, long offset);

    public void close();

    public ConsumerRecords<K,V> poll(long timeout);
}
