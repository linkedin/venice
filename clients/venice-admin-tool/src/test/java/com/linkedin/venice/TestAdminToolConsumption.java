package com.linkedin.venice;

import static com.linkedin.venice.kafka.protocol.enums.MessageType.PUT;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.EndOfPush;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.StoreInfo;
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
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestAdminToolConsumption {
  @Test
  public void testAdminToolConsumption() {
    String schemaStr = "\"string\"";
    String storeName = "test_store";
    String topic = storeName + "_rt";
    ControllerClient controllerClient = mock(ControllerClient.class);
    SchemaResponse schemaResponse = mock(SchemaResponse.class);
    when(schemaResponse.getSchemaStr()).thenReturn(schemaStr);
    when(controllerClient.getKeySchema(storeName)).thenReturn(schemaResponse);
    StoreResponse storeResponse = mock(StoreResponse.class);
    StoreInfo storeInfo = mock(StoreInfo.class);
    when(storeInfo.getPartitionCount()).thenReturn(2);
    when(controllerClient.getStore(storeName)).thenReturn(storeResponse);
    when(storeResponse.getStore()).thenReturn(storeInfo);

    PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
    int assignedPartition = 0;
    long startOffset = 0;
    long endOffset = 1;
    long progressInterval = 1;
    String keyString = "test";
    byte[] serializedKey = TopicMessageFinder.serializeKey(keyString, schemaStr);
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
        new ImmutablePubSubMessage<>(kafkaKey, messageEnvelope2, pubSubTopicPartition, 1, 0, 10);
    KafkaKey kafkaControlMessageKey = new KafkaKey(MessageType.CONTROL_MESSAGE, null);
    EndOfPush endOfPush = new EndOfPush();
    KafkaMessageEnvelope kafkaMessageEnvelope = new KafkaMessageEnvelope();
    kafkaMessageEnvelope.messageType = MessageType.CONTROL_MESSAGE.getValue();
    ControlMessage controlMessage = new ControlMessage();
    controlMessage.controlMessageUnion = endOfPush;
    controlMessage.controlMessageType = ControlMessageType.START_OF_PUSH.getValue();
    kafkaMessageEnvelope.payloadUnion = controlMessage;
    kafkaMessageEnvelope.producerMetadata = new ProducerMetadata();
    kafkaMessageEnvelope.producerMetadata.messageTimestamp = 0;
    kafkaMessageEnvelope.producerMetadata.messageSequenceNumber = 0;
    kafkaMessageEnvelope.producerMetadata.segmentNumber = 0;
    kafkaMessageEnvelope.producerMetadata.producerGUID = new GUID();
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> pubSubMessage3 =
        new ImmutablePubSubMessage<>(kafkaControlMessageKey, kafkaMessageEnvelope, pubSubTopicPartition, 2, 0, 20);

    List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> pubSubMessageList = new ArrayList<>();
    pubSubMessageList.add(pubSubMessage1);
    pubSubMessageList.add(pubSubMessage2);
    pubSubMessageList.add(pubSubMessage3);
    Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> messagesMap = new HashMap<>();
    messagesMap.put(pubSubTopicPartition, pubSubMessageList);
    ApacheKafkaConsumerAdapter apacheKafkaConsumer = mock(ApacheKafkaConsumerAdapter.class);
    when(apacheKafkaConsumer.poll(anyLong())).thenReturn(messagesMap, new HashMap<>());
    long startTimestamp = 10;
    long endTimestamp = 20;
    when(apacheKafkaConsumer.offsetForTime(pubSubTopicPartition, startTimestamp)).thenReturn(startOffset);
    when(apacheKafkaConsumer.offsetForTime(pubSubTopicPartition, endTimestamp)).thenReturn(endOffset);
    when(apacheKafkaConsumer.endOffset(pubSubTopicPartition)).thenReturn(endOffset);
    long messageCount = TopicMessageFinder
        .find(controllerClient, apacheKafkaConsumer, topic, keyString, startTimestamp, endTimestamp, progressInterval);
    Assert.assertEquals(messageCount, endOffset - startOffset);

    when(apacheKafkaConsumer.poll(anyLong())).thenReturn(messagesMap, new HashMap<>());

    when(apacheKafkaConsumer.poll(anyLong())).thenReturn(messagesMap, new HashMap<>());
    ControlMessageDumper controlMessageDumper =
        new ControlMessageDumper(apacheKafkaConsumer, topic, 0, 0, pubSubMessageList.size());
    Assert.assertEquals(controlMessageDumper.fetch().display(), 1);

    when(apacheKafkaConsumer.poll(anyLong())).thenReturn(messagesMap, new HashMap<>());
    int consumedMessageCount = pubSubMessageList.size() - 1;
    KafkaTopicDumper kafkaTopicDumper =
        new KafkaTopicDumper(controllerClient, apacheKafkaConsumer, topic, assignedPartition, 0, 2, "", 3, true);
    Assert.assertEquals(kafkaTopicDumper.fetchAndProcess(), consumedMessageCount);
  }
}
