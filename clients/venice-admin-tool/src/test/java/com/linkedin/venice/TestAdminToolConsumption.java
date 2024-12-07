package com.linkedin.venice;

import static com.linkedin.venice.kafka.protocol.enums.MessageType.PUT;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.admin.SchemaMeta;
import com.linkedin.venice.controller.kafka.protocol.admin.StoreCreation;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.kafka.protocol.enums.SchemaType;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
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
import com.linkedin.venice.utils.Utils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestAdminToolConsumption {
  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
  private static final String SCHEMA_STRING = "\"string\"";
  private static final String STORE_NAME = "test_store";

  @Test
  void testAdminToolAdminMessageConsumption() {
    int assignedPartition = 0;
    String topic = Utils.composeRealTimeTopic(STORE_NAME);
    PubSubTopicPartition pubSubTopicPartition =
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(topic), assignedPartition);
    int adminMessageNum = 10;
    int dumpedMessageNum = 2;
    List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> pubSubMessageList =
        prepareAdminPubSubMessageList(STORE_NAME, pubSubTopicPartition, adminMessageNum);
    Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> messagesMap = new HashMap<>();
    messagesMap.put(pubSubTopicPartition, pubSubMessageList);
    ApacheKafkaConsumerAdapter apacheKafkaConsumer = mock(ApacheKafkaConsumerAdapter.class);
    when(apacheKafkaConsumer.poll(anyLong())).thenReturn(messagesMap, Collections.EMPTY_MAP);
    List<DumpAdminMessages.AdminOperationInfo> adminOperationInfos =
        DumpAdminMessages.dumpAdminMessages(apacheKafkaConsumer, "cluster1", 0, dumpedMessageNum);
    Assert.assertEquals(adminOperationInfos.size(), dumpedMessageNum);
  }

  private List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> prepareAdminPubSubMessageList(
      String storeName,
      PubSubTopicPartition pubSubTopicPartition,
      int messageNum) {
    List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> pubSubMessageList = new ArrayList<>();
    for (int i = 0; i < messageNum; i++) {
      String keyString = "test";
      byte[] serializedKey = TopicMessageFinder.serializeKey(keyString, SCHEMA_STRING);
      KafkaKey kafkaKey = new KafkaKey(PUT, serializedKey);
      KafkaMessageEnvelope messageEnvelope = new KafkaMessageEnvelope();
      messageEnvelope.producerMetadata = new ProducerMetadata();
      messageEnvelope.producerMetadata.messageTimestamp = 0;
      messageEnvelope.producerMetadata.messageSequenceNumber = 0;
      messageEnvelope.producerMetadata.segmentNumber = 0;
      messageEnvelope.producerMetadata.producerGUID = new GUID();
      Put put = new Put();
      put.schemaId = AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION;
      AdminOperationSerializer deserializer = new AdminOperationSerializer();
      StoreCreation storeCreation = (StoreCreation) AdminMessageType.STORE_CREATION.getNewInstance();
      storeCreation.clusterName = "clusterName";
      storeCreation.storeName = storeName;
      storeCreation.owner = "owner";
      storeCreation.keySchema = new SchemaMeta();
      storeCreation.keySchema.definition = SCHEMA_STRING;
      storeCreation.keySchema.schemaType = SchemaType.AVRO_1_4.getValue();
      storeCreation.valueSchema = new SchemaMeta();
      storeCreation.valueSchema.definition = SCHEMA_STRING;
      storeCreation.valueSchema.schemaType = SchemaType.AVRO_1_4.getValue();
      AdminOperation adminMessage = new AdminOperation();
      adminMessage.operationType = AdminMessageType.STORE_CREATION.getValue();
      adminMessage.payloadUnion = storeCreation;
      adminMessage.executionId = 1;
      deserializer.serialize(adminMessage);
      byte[] putValueBytes = deserializer.serialize(adminMessage);
      put.putValue = ByteBuffer.wrap(putValueBytes);
      put.replicationMetadataPayload = ByteBuffer.allocate(0);
      messageEnvelope.payloadUnion = put;
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> pubSubMessage =
          new ImmutablePubSubMessage<>(kafkaKey, messageEnvelope, pubSubTopicPartition, 0, 0, 20);
      pubSubMessageList.add(pubSubMessage);
    }
    return pubSubMessageList;
  }

  @Test
  public void testAdminToolConsumption() {
    ControllerClient controllerClient = mock(ControllerClient.class);
    SchemaResponse schemaResponse = mock(SchemaResponse.class);
    when(schemaResponse.getSchemaStr()).thenReturn(SCHEMA_STRING);
    when(controllerClient.getKeySchema(STORE_NAME)).thenReturn(schemaResponse);
    StoreResponse storeResponse = mock(StoreResponse.class);
    StoreInfo storeInfo = mock(StoreInfo.class, RETURNS_DEEP_STUBS);
    when(storeInfo.getPartitionCount()).thenReturn(2);
    when(controllerClient.getStore(STORE_NAME)).thenReturn(storeResponse);
    when(storeResponse.getStore()).thenReturn(storeInfo);
    when(storeInfo.getHybridStoreConfig().getRealTimeTopicName()).thenReturn(Utils.composeRealTimeTopic(STORE_NAME));
    String topic = storeInfo.getHybridStoreConfig().getRealTimeTopicName();

    int assignedPartition = 0;
    long startOffset = 0;
    long endOffset = 1;
    long progressInterval = 1;
    String keyString = "test";
    byte[] serializedKey = TopicMessageFinder.serializeKey(keyString, SCHEMA_STRING);
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
    KafkaKey kafkaControlMessageKey = new KafkaKey(MessageType.CONTROL_MESSAGE, new byte[0]);
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
    when(apacheKafkaConsumer.poll(anyLong())).thenReturn(messagesMap, Collections.EMPTY_MAP);
    long startTimestamp = 10;
    long endTimestamp = 20;
    when(apacheKafkaConsumer.offsetForTime(pubSubTopicPartition, startTimestamp)).thenReturn(startOffset);
    when(apacheKafkaConsumer.offsetForTime(pubSubTopicPartition, endTimestamp)).thenReturn(endOffset);
    long messageCount = TopicMessageFinder
        .find(controllerClient, apacheKafkaConsumer, topic, keyString, startTimestamp, endTimestamp, progressInterval);
    Assert.assertEquals(messageCount, endOffset - startOffset);

    when(apacheKafkaConsumer.poll(anyLong())).thenReturn(messagesMap, Collections.EMPTY_MAP);
    when(apacheKafkaConsumer.endOffset(pubSubTopicPartition)).thenReturn(endOffset);
    long messageCountNoEndOffset = TopicMessageFinder.find(
        controllerClient,
        apacheKafkaConsumer,
        topic,
        keyString,
        startTimestamp,
        Long.MAX_VALUE,
        progressInterval);
    Assert.assertEquals(messageCountNoEndOffset, endOffset - startOffset);

    when(apacheKafkaConsumer.poll(anyLong())).thenReturn(messagesMap, Collections.EMPTY_MAP);
    ControlMessageDumper controlMessageDumper =
        new ControlMessageDumper(apacheKafkaConsumer, topic, 0, 0, pubSubMessageList.size());
    Assert.assertEquals(controlMessageDumper.fetch().display(), 1);

    when(apacheKafkaConsumer.poll(anyLong())).thenReturn(messagesMap, Collections.EMPTY_MAP);
    int consumedMessageCount = pubSubMessageList.size() - 1;
    KafkaTopicDumper kafkaTopicDumper = new KafkaTopicDumper(
        controllerClient,
        apacheKafkaConsumer,
        pubSubTopicPartition,
        "",
        3,
        true,
        false,
        false,
        false);
    Assert.assertEquals(kafkaTopicDumper.fetchAndProcess(0, 1, 2), consumedMessageCount);
  }
}
