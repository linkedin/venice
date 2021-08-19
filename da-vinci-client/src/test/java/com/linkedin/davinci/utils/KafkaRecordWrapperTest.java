package com.linkedin.davinci.utils;

import com.linkedin.davinci.kafka.consumer.VeniceConsumerRecordWrapper;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


public class KafkaRecordWrapperTest {

  @Test
  public void testWrappingIsCorrect() {
    ConsumerRecord<KafkaKey, KafkaMessageEnvelope> r1 = new ConsumerRecord("topic1", 1, -1, null, null);
    ConsumerRecord<KafkaKey, KafkaMessageEnvelope> r2 = new ConsumerRecord("topic2", 2, -1, null, null);
    ConsumerRecord<KafkaKey, KafkaMessageEnvelope> r3 = new ConsumerRecord("topic3", 3, -1, null, null);

    List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>> recordList = new LinkedList<>();
    recordList.add(r1);
    recordList.add(r2);
    recordList.add(r3);

    Iterable<VeniceConsumerRecordWrapper<KafkaKey, KafkaMessageEnvelope>> wrappedRecordList = KafkaRecordWrapper.wrap("kafka-url1", recordList, 1);

    Iterator<VeniceConsumerRecordWrapper<KafkaKey, KafkaMessageEnvelope>> iter = wrappedRecordList.iterator();
    VeniceConsumerRecordWrapper<KafkaKey, KafkaMessageEnvelope> wrappedRecord1 = iter.next();
    Assert.assertEquals(wrappedRecord1.kafkaUrl(), "kafka-url1");
    Assert.assertEquals(wrappedRecord1.consumerRecord().topic(), "topic1");
    Assert.assertEquals(wrappedRecord1.consumerRecord().partition(), 1);

    VeniceConsumerRecordWrapper<KafkaKey, KafkaMessageEnvelope> wrappedRecord2 = iter.next();
    Assert.assertEquals(wrappedRecord2.kafkaUrl(), "kafka-url1");
    Assert.assertEquals(wrappedRecord2.consumerRecord().topic(), "topic2");
    Assert.assertEquals(wrappedRecord2.consumerRecord().partition(), 2);

    VeniceConsumerRecordWrapper<KafkaKey, KafkaMessageEnvelope> wrappedRecord3 = iter.next();
    Assert.assertEquals(wrappedRecord3.kafkaUrl(), "kafka-url1");
    Assert.assertEquals(wrappedRecord3.consumerRecord().topic(), "topic3");
    Assert.assertEquals(wrappedRecord3.consumerRecord().partition(), 3);

  }
}
