package com.linkedin.venice;

import static com.linkedin.venice.kafka.protocol.enums.MessageType.PUT;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.consumer.ApacheKafkaConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;


public class TopicMessageFinderTest {
  @Test
  public void testTopicMessageFinder() {
    ApacheKafkaConsumerAdapter apacheKafkaConsumer = mock(ApacheKafkaConsumerAdapter.class);
    PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
    String topic = "1_rt";
    int assignedPartition = 0;
    long startOffset = 0;
    long endOffset = 1;
    long progressInterval = 1;
    byte[] serializedKey = new byte[] { 0, 1, 2 };
    KafkaKey kafkaKey = new KafkaKey(PUT, serializedKey);
    KafkaMessageEnvelope messageEnvelope = new KafkaMessageEnvelope();
    messageEnvelope.producerMetadata = new ProducerMetadata();
    messageEnvelope.producerMetadata.messageTimestamp = 0;
    messageEnvelope.producerMetadata.messageSequenceNumber = 0;
    messageEnvelope.producerMetadata.segmentNumber = 0;
    messageEnvelope.producerMetadata.producerGUID = new GUID();
    Put put = new Put();
    put.putValue = ByteBuffer.allocate(0);
    put.replicationMetadataPayload = ByteBuffer.allocate(0);
    messageEnvelope.payloadUnion = put;

    KafkaMessageEnvelope messageEnvelope2 = new KafkaMessageEnvelope();
    messageEnvelope2.producerMetadata = new ProducerMetadata();
    messageEnvelope2.producerMetadata.messageTimestamp = 0;
    messageEnvelope2.producerMetadata.messageSequenceNumber = 0;
    messageEnvelope2.producerMetadata.segmentNumber = 0;
    messageEnvelope2.producerMetadata.producerGUID = new GUID();
    Delete delete = new Delete();
    delete.replicationMetadataPayload = ByteBuffer.allocate(0);
    messageEnvelope2.payloadUnion = delete;
    PubSubTopicPartition pubSubTopicPartition =
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(topic), assignedPartition);

    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> pubSubMessage1 =
        new ImmutablePubSubMessage<>(kafkaKey, messageEnvelope, pubSubTopicPartition, 0, 0, 20);
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> pubSubMessage2 =
        new ImmutablePubSubMessage<>(kafkaKey, messageEnvelope2, pubSubTopicPartition, 0, 0, 10);

    List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> pubSubMessageList = new ArrayList<>();
    pubSubMessageList.add(pubSubMessage1);
    pubSubMessageList.add(pubSubMessage2);
    Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> messagesMap = new HashMap<>();
    messagesMap.put(pubSubTopicPartition, pubSubMessageList);
    when(apacheKafkaConsumer.poll(anyLong())).thenReturn(messagesMap, new HashMap<>());

    TopicMessageFinder.consume(
        apacheKafkaConsumer,
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(topic), assignedPartition),
        startOffset,
        endOffset,
        progressInterval,
        serializedKey);
  }
}
