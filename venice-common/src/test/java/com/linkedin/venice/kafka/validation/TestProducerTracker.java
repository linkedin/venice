package com.linkedin.venice.kafka.validation;

import com.linkedin.venice.exceptions.validation.MissingDataException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.StartOfSegment;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.message.KafkaKey;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Optional;
import org.apache.avro.specific.FixedSize;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestProducerTracker {

  private ProducerTracker producerTracker;
  private GUID guid;
  private String topic;

  @BeforeMethod(alwaysRun = true)
  public void methodSetUp() {
    String testGuid = "test_guid_" + System.currentTimeMillis();
    this.guid = new GUID();
    guid.bytes(testGuid.getBytes());
    this.topic = "test_topic_" + System.currentTimeMillis();
    this.producerTracker = new ProducerTracker(guid, topic);
  }


  private KafkaMessageEnvelope getKafkaMessageEnvelope(MessageType messageType,
      GUID producerGUID, Segment currentSegment, Optional<Integer> sequenceNum,
      Object payload) {
    KafkaMessageEnvelope kafkaValue = new KafkaMessageEnvelope();
    kafkaValue.messageType = messageType.getValue();

    ProducerMetadata producerMetadata = new ProducerMetadata();
    producerMetadata.producerGUID = producerGUID;
    producerMetadata.segmentNumber = currentSegment.getSegmentNumber();
    if (sequenceNum.isPresent()) {
      producerMetadata.messageSequenceNumber = sequenceNum.get();
    } else {
      producerMetadata.messageSequenceNumber = currentSegment.getAndIncrementSequenceNumber();
    }
    producerMetadata.messageTimestamp = System.currentTimeMillis();
    kafkaValue.producerMetadata = producerMetadata;
    kafkaValue.payloadUnion = payload;

    return kafkaValue;
  }

  private static final int CONTROL_MESSAGE_KAFKA_KEY_LENGTH = GUID.class.getAnnotation(FixedSize.class).value() + Integer.SIZE * 2;
  private KafkaKey getControlMessageKey(KafkaMessageEnvelope kafkaValue) {
    return new KafkaKey(
        MessageType.CONTROL_MESSAGE,
        ByteBuffer.allocate(CONTROL_MESSAGE_KAFKA_KEY_LENGTH)
            .put(kafkaValue.producerMetadata.producerGUID.bytes())
            .putInt(kafkaValue.producerMetadata.segmentNumber)
            .putInt(kafkaValue.producerMetadata.messageSequenceNumber)
            .array());
  }

  private KafkaKey getPutMessageKey(byte[] bytes) {
    return new KafkaKey(MessageType.PUT, bytes);
  }

  private Put getPutMessage(byte[] bytes) {
    Put putPayload = new Put();
    putPayload.putValue = ByteBuffer.wrap(bytes);
    putPayload.schemaId = -1;

    return putPayload;
  }

  private ControlMessage getStartOfSegment() {
    ControlMessage controlMessage = new ControlMessage();
    controlMessage.controlMessageType = ControlMessageType.START_OF_SEGMENT.getValue();
    StartOfSegment startOfSegment = new StartOfSegment();
    startOfSegment.upcomingAggregates = new ArrayList<>();
    startOfSegment.upcomingAggregates.add("baz");
    startOfSegment.checksumType = CheckSumType.NONE.getValue();
    controlMessage.controlMessageUnion = startOfSegment;
    controlMessage.debugInfo = new HashMap<>();
    controlMessage.debugInfo.put("foo", "bar");

    return controlMessage;
  }

  @Test
  public void testSequenceNumber() {
    int partitionId = 0;
    Segment currentSegment = new Segment(partitionId, 0, CheckSumType.NONE);

    // Send Start_Of_Segment control msg.
    ControlMessage startOfSegment = getStartOfSegment();
    KafkaMessageEnvelope startOfSegmentMessage = getKafkaMessageEnvelope(MessageType.CONTROL_MESSAGE,
        guid, currentSegment, Optional.empty(), startOfSegment);
    long offset = 10;
    ConsumerRecord<KafkaKey, KafkaMessageEnvelope> controlMessageConsumerRecord = new ConsumerRecord<>(
        topic, partitionId, offset++, System.currentTimeMillis() + 1000, TimestampType.NO_TIMESTAMP_TYPE,
        ConsumerRecord.NULL_CHECKSUM, ConsumerRecord.NULL_SIZE, ConsumerRecord.NULL_SIZE,
        getControlMessageKey(startOfSegmentMessage), startOfSegmentMessage);
    producerTracker.validateMessageAndGetOffsetRecordTransformer(controlMessageConsumerRecord, false, false);

    Put firstPut = getPutMessage("first_message".getBytes());
    KafkaMessageEnvelope firstMessage = getKafkaMessageEnvelope(MessageType.PUT,
        guid, currentSegment, Optional.empty(), firstPut); // sequence number is 1
    KafkaKey firstMessageKey = getPutMessageKey("first_key".getBytes());
    ConsumerRecord<KafkaKey, KafkaMessageEnvelope> firstConsumerRecord = new ConsumerRecord<>(
        topic, partitionId, offset++, System.currentTimeMillis() + 1000, TimestampType.NO_TIMESTAMP_TYPE,
        ConsumerRecord.NULL_CHECKSUM, ConsumerRecord.NULL_SIZE, ConsumerRecord.NULL_SIZE, firstMessageKey, firstMessage);
    producerTracker.validateMessageAndGetOffsetRecordTransformer(firstConsumerRecord, false, false);

    // Message with gap
    Put secondPut = getPutMessage("second_message".getBytes());
    KafkaMessageEnvelope secondMessage = getKafkaMessageEnvelope(MessageType.PUT,
        guid, currentSegment, Optional.of(100), secondPut); // sequence number is 100
    KafkaKey secondMessageKey = getPutMessageKey("second_key".getBytes());
    ConsumerRecord<KafkaKey, KafkaMessageEnvelope> secondConsumerRecord = new ConsumerRecord<>(
        topic, partitionId, offset++, System.currentTimeMillis() + 1000, TimestampType.NO_TIMESTAMP_TYPE,
        ConsumerRecord.NULL_CHECKSUM, ConsumerRecord.NULL_SIZE, ConsumerRecord.NULL_SIZE, secondMessageKey, secondMessage);
    Assert.assertThrows(MissingDataException.class, () -> producerTracker.validateMessageAndGetOffsetRecordTransformer(secondConsumerRecord, false, false));

    // Message without gap
    Put thirdPut = getPutMessage("third_message".getBytes());
    KafkaMessageEnvelope thirdMessage = getKafkaMessageEnvelope(MessageType.PUT,
        guid, currentSegment, Optional.of(2), thirdPut); // sequence number is 2
    KafkaKey thirdMessageKey = getPutMessageKey("third_key".getBytes());
    ConsumerRecord<KafkaKey, KafkaMessageEnvelope> thirdConsumerRecord = new ConsumerRecord<>(
        topic, partitionId, offset++, System.currentTimeMillis() + 1000, TimestampType.NO_TIMESTAMP_TYPE,
        ConsumerRecord.NULL_CHECKSUM, ConsumerRecord.NULL_SIZE, ConsumerRecord.NULL_SIZE, thirdMessageKey, thirdMessage);
    // It doesn't matter whether EOP is true/false. The result is same.
    producerTracker.validateMessageAndGetOffsetRecordTransformer(thirdConsumerRecord, false, false);

    // Message with gap but tolerate messages is allowed
    Put fourthPut = getPutMessage("fourth_message".getBytes());
    KafkaMessageEnvelope fourthMessage = getKafkaMessageEnvelope(MessageType.PUT,
        guid, currentSegment, Optional.of(100), fourthPut); // sequence number is 100
    KafkaKey fourthMessageKey = getPutMessageKey("fourth_key".getBytes());
    ConsumerRecord<KafkaKey, KafkaMessageEnvelope> fourthConsumerRecord = new ConsumerRecord<>(
        topic, partitionId, offset++, System.currentTimeMillis() + 1000, TimestampType.NO_TIMESTAMP_TYPE,
        ConsumerRecord.NULL_CHECKSUM, ConsumerRecord.NULL_SIZE, ConsumerRecord.NULL_SIZE, fourthMessageKey, fourthMessage);
    producerTracker.validateMessageAndGetOffsetRecordTransformer(fourthConsumerRecord, false, true);
  }

  @Test
  public void testSegmentNumber() {
    int partitionId = 0;
    int firstSegmentNumber = 0;
    int skipSegmentNumber = 2;
    Segment firstSegment = new Segment(partitionId, firstSegmentNumber, CheckSumType.NONE);
    Segment secondSegment = new Segment(partitionId, skipSegmentNumber, CheckSumType.NONE);
    long offset = 10;

    // Send the first segment
    ControlMessage startOfSegment = getStartOfSegment();
    KafkaMessageEnvelope startOfSegmentMessage = getKafkaMessageEnvelope(MessageType.CONTROL_MESSAGE,
        guid, firstSegment, Optional.empty(), startOfSegment); // sequence number is zero
    ConsumerRecord<KafkaKey, KafkaMessageEnvelope> controlMessageConsumerRecord = new ConsumerRecord<>(
        topic, partitionId, offset++, System.currentTimeMillis() + 1000, TimestampType.NO_TIMESTAMP_TYPE,
        ConsumerRecord.NULL_CHECKSUM, ConsumerRecord.NULL_SIZE, ConsumerRecord.NULL_SIZE,
        getControlMessageKey(startOfSegmentMessage), startOfSegmentMessage);
    producerTracker.validateMessageAndGetOffsetRecordTransformer(controlMessageConsumerRecord, true, false);

    // Send the second segment. Notice this segment number has a gap than previous one
    Put firstPut = getPutMessage("message".getBytes());
    int skipSequenceNumber = 5;
    KafkaMessageEnvelope firstMessage = getKafkaMessageEnvelope(MessageType.PUT,
        guid, secondSegment, Optional.of(skipSequenceNumber), firstPut);
    KafkaKey firstMessageKey = getPutMessageKey("key".getBytes());
    ConsumerRecord<KafkaKey, KafkaMessageEnvelope> firstConsumerRecord = new ConsumerRecord<>(
        topic, partitionId, offset++, System.currentTimeMillis() + 1000, TimestampType.NO_TIMESTAMP_TYPE,
        ConsumerRecord.NULL_CHECKSUM, ConsumerRecord.NULL_SIZE, ConsumerRecord.NULL_SIZE, firstMessageKey, firstMessage);
    Assert.assertThrows(MissingDataException.class, () -> producerTracker.validateMessageAndGetOffsetRecordTransformer(firstConsumerRecord, true, false));
    producerTracker.validateMessageAndGetOffsetRecordTransformer(firstConsumerRecord, true, true);
    Assert.assertEquals(producerTracker.segments.get(partitionId).getSegmentNumber(), skipSegmentNumber);
    Assert.assertEquals(producerTracker.segments.get(partitionId).getSequenceNumber(), skipSequenceNumber);
  }
}
