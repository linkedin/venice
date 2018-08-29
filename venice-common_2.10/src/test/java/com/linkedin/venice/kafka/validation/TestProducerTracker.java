package com.linkedin.venice.kafka.validation;

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
import java.util.Optional;
import org.apache.avro.specific.FixedSize;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class TestProducerTracker {

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
    startOfSegment.checksumType = CheckSumType.NONE.getValue();
    startOfSegment.upcomingAggregates = new ArrayList<>();
    controlMessage.controlMessageUnion = startOfSegment;

    return controlMessage;
  }

  @Test
  public void testAddMessagesWithGap() {
    String testGuid = "test_guid";
    int partitionId = 0;
    GUID guid = new GUID();
    guid.bytes(testGuid.getBytes());
    ProducerTracker producerTracker = new ProducerTracker(guid, "test-topic-name");
    Segment currentSegment = new Segment(partitionId, 0, CheckSumType.NONE);

    ProducerTracker.DIVErrorMetricCallback mockCallback = mock(ProducerTracker.DIVErrorMetricCallback.class);

    ControlMessage startOfSegment = getStartOfSegment();

    KafkaMessageEnvelope startOfSegmentMessage = getKafkaMessageEnvelope(MessageType.CONTROL_MESSAGE,
        guid, currentSegment, Optional.empty(), startOfSegment);
    producerTracker.addMessage(partitionId, getControlMessageKey(startOfSegmentMessage), startOfSegmentMessage,
        true, Optional.of(mockCallback));
    verify(mockCallback, never()).execute(any());

    mockCallback = mock(ProducerTracker.DIVErrorMetricCallback.class);
    Put firstPut = getPutMessage("first_message".getBytes());
    KafkaMessageEnvelope firstMessage = getKafkaMessageEnvelope(MessageType.PUT,
        guid, currentSegment, Optional.empty(), firstPut);
    KafkaKey firstMessageKey = getPutMessageKey("first_key".getBytes());
    producerTracker.addMessage(partitionId, firstMessageKey, firstMessage, true, Optional.of(mockCallback));

    // Message with gap
    mockCallback = mock(ProducerTracker.DIVErrorMetricCallback.class);
    Put secondPut = getPutMessage("second_message".getBytes());
    KafkaMessageEnvelope secondMessage = getKafkaMessageEnvelope(MessageType.PUT,
        guid, currentSegment, Optional.of(100), secondPut);
    KafkaKey secondMessageKey = getPutMessageKey("second_key".getBytes());
    producerTracker.addMessage(partitionId, secondMessageKey, secondMessage, true, Optional.of(mockCallback));
    verify(mockCallback, times(1)).execute(any());

    // third message without gap
    mockCallback = mock(ProducerTracker.DIVErrorMetricCallback.class);
    Put thirdPut = getPutMessage("third_message".getBytes());
    KafkaMessageEnvelope thirdMessage = getKafkaMessageEnvelope(MessageType.PUT,
        guid, currentSegment, Optional.of(101), thirdPut);
    KafkaKey thirdMessageKey = getPutMessageKey("third_key".getBytes());
    producerTracker.addMessage(partitionId, thirdMessageKey, thirdMessage, true, Optional.of(mockCallback));
    verify(mockCallback, never()).execute(any());
  }
}
