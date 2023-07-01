package com.linkedin.davinci.validation;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.*;

import com.linkedin.venice.exceptions.validation.ImproperlyStartedSegmentException;
import com.linkedin.venice.exceptions.validation.MissingDataException;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.LeaderMetadata;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.StartOfSegment;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.utils.TestMockTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.writer.VeniceWriter;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;


public class KafkaDataIntegrityValidatorTest {
  private static final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @Test
  public void testClearExpiredState() throws InterruptedException {
    final long maxAgeInMs = 1000;
    Time time = new TestMockTime();
    String kafkaTopic = Utils.getUniqueString("TestStore") + "_v1";
    KafkaDataIntegrityValidator validator =
        new KafkaDataIntegrityValidator(kafkaTopic, KafkaDataIntegrityValidator.DISABLED, maxAgeInMs, time);
    PubSubTopicPartition topicPartition0 = new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(kafkaTopic), 0);
    PubSubTopicPartition topicPartition1 = new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(kafkaTopic), 1);
    long offsetForPartition0 = 0;
    long offsetForPartition1 = 0;
    int seqNumberForPartition0Guid1 = 1;
    int seqNumberForPartition1Guid1 = 1;
    GUID producerGuid1 = GuidUtils.getGUID(VeniceProperties.empty());
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record1p0 =
        buildSoSRecord(topicPartition0, offsetForPartition0++, producerGuid1, 0, time.getMilliseconds());
    validator.validateMessage(record1p0, false, Lazy.FALSE);

    time.sleep(10);

    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record2p0 = buildPutRecord(
        topicPartition0,
        offsetForPartition0++,
        producerGuid1,
        0,
        seqNumberForPartition0Guid1++,
        time.getMilliseconds());
    validator.validateMessage(record2p0, false, Lazy.FALSE);

    ProducerTracker guid1ProducerTracker = validator.registerProducer(producerGuid1);
    assertEquals(guid1ProducerTracker.getNumberOfTrackedPartitions(), 1);

    // Nothing should be cleared yet
    validator.clearExpiredState();
    assertEquals(guid1ProducerTracker.getNumberOfTrackedPartitions(), 1);

    // Even if we wait some more, the max age is not yet reached, so the state should still be retained
    time.sleep(maxAgeInMs / 2);
    validator.clearExpiredState();
    assertEquals(guid1ProducerTracker.getNumberOfTrackedPartitions(), 1);

    // Start writing into another partition
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record1p1 =
        buildSoSRecord(topicPartition1, offsetForPartition1++, producerGuid1, 0, time.getMilliseconds());
    validator.validateMessage(record1p1, false, Lazy.FALSE);
    assertEquals(guid1ProducerTracker.getNumberOfTrackedPartitions(), 2);

    // At exactly the max age, clearing should still not kick in
    time.sleep(maxAgeInMs / 2);
    validator.clearExpiredState();
    assertEquals(guid1ProducerTracker.getNumberOfTrackedPartitions(), 2);

    // Finally, after the max age is exceeded, it should clear partition 0 and thus be left with only 1 partition
    time.sleep(1);
    validator.clearExpiredState();
    assertEquals(guid1ProducerTracker.getNumberOfTrackedPartitions(), 1);

    // If, somehow, a message still came from this GUID in partition 0, after clearing the state, DIV should catch it
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record3p0 = buildPutRecord(
        topicPartition0,
        offsetForPartition0++,
        producerGuid1,
        0,
        seqNumberForPartition0Guid1,
        time.getMilliseconds());
    assertThrows(
        ImproperlyStartedSegmentException.class,
        () -> validator.validateMessage(record3p0, false, Lazy.FALSE));

    // After waiting more, partition 1 should also get cleared
    time.sleep(maxAgeInMs / 2);
    validator.clearExpiredState();
    assertEquals(guid1ProducerTracker.getNumberOfTrackedPartitions(), 0);

    // If, somehow, a message still came from this GUID in partition 1, after clearing the state, DIV should catch it
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record2p1 = buildPutRecord(
        topicPartition1,
        offsetForPartition1,
        producerGuid1,
        0,
        seqNumberForPartition1Guid1,
        time.getMilliseconds());
    assertThrows(
        ImproperlyStartedSegmentException.class,
        () -> validator.validateMessage(record2p1, false, Lazy.FALSE));

    // Another producer GUID starts producing
    GUID producerGuid2 = GuidUtils.getGUID(VeniceProperties.empty());
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record4p0 =
        buildSoSRecord(topicPartition0, offsetForPartition0, producerGuid2, 0, time.getMilliseconds());
    validator.validateMessage(record4p0, false, Lazy.FALSE);
    ProducerTracker guid2ProducerTracker = validator.registerProducer(producerGuid2);
    assertEquals(guid1ProducerTracker.getNumberOfTrackedPartitions(), 0);
    assertEquals(guid2ProducerTracker.getNumberOfTrackedPartitions(), 1);

    // State shouldn't change even after clearing
    validator.clearExpiredState();
    assertEquals(guid1ProducerTracker.getNumberOfTrackedPartitions(), 0);
    assertEquals(guid2ProducerTracker.getNumberOfTrackedPartitions(), 1);

    // Sleep beyond max age, see if the second GUID gets cleared
    time.sleep(maxAgeInMs + 1);
    validator.clearExpiredState();
    assertEquals(guid1ProducerTracker.getNumberOfTrackedPartitions(), 0);
    assertEquals(guid2ProducerTracker.getNumberOfTrackedPartitions(), 0);
  }

  @Test
  public void testStatelessDIV() {
    String kafkaTopic = Utils.getUniqueString("TestStore") + "_v1";
    long kafkaLogCompactionLagInMs = TimeUnit.HOURS.toMillis(24); // 24 hours
    KafkaDataIntegrityValidator stateLessDIVValidator =
        new KafkaDataIntegrityValidator(kafkaTopic, kafkaLogCompactionLagInMs);
    PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(kafkaTopic), 1);

    /**
     * Create a record that starts in the middle of a segment with sequence number 100 and the broken timestamp for this
     * record is 28 hours ago.
     */
    GUID producerGUID = GuidUtils.getGUID(VeniceProperties.empty());
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record = buildPutRecord(
        topicPartition,
        100,
        producerGUID,
        0,
        100,
        System.currentTimeMillis() - TimeUnit.HOURS.toMillis(28));

    // Stateless DIV will allow a segment starts in the middle
    stateLessDIVValidator.checkMissingMessage(record, Optional.empty());

    /**
     * Create a record with sequence number 101 in the same segment and the broken timestamp for this record is 27 hours
     * ago; no error should be thrown since sequence number is incrementing without gap.
     */
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record2 = buildPutRecord(
        topicPartition,
        101,
        producerGUID,
        0,
        101,
        System.currentTimeMillis() - TimeUnit.HOURS.toMillis(27));
    stateLessDIVValidator.checkMissingMessage(record2, Optional.empty());

    /**
     * Create a record with sequence number 103 in the same segment and the broken timestamp for this record is 20 hours
     * ago; there is a gap between sequence number 101 and 103; however, since the previous record is older than the
     * log compaction delay threshold (24 hours), missing message is allowed.
     */
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record3 = buildPutRecord(
        topicPartition,
        200,
        producerGUID,
        0,
        103,
        System.currentTimeMillis() - TimeUnit.HOURS.toMillis(20));
    stateLessDIVValidator.checkMissingMessage(record3, Optional.empty());

    /**
     * Create a record with sequence number 105 in the same segment and the broken timestamp for this record is 10 hours
     * ago; there is a gap between sequence number 103 and 105; MISSING_MESSAGE exception should be thrown this time
     * because the previous message for the same segment is fresh (20 hours ago), Kafka log compaction hasn't started
     * yet, so missing message is not expected
     */
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record4 = buildPutRecord(
        topicPartition,
        205,
        producerGUID,
        0,
        105,
        System.currentTimeMillis() - TimeUnit.HOURS.toMillis(10));
    Assert.assertThrows(
        MissingDataException.class,
        () -> stateLessDIVValidator.checkMissingMessage(record4, Optional.empty()));

    ProducerTracker.DIVErrorMetricCallback errorMetricCallback = mock(ProducerTracker.DIVErrorMetricCallback.class);

    /**
     * Create a record with a gap in segment number. MISSING_MESSAGE exception should be thrown
     */
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record5 = buildPutRecord(
        topicPartition,
        206,
        producerGUID,
        2,
        1,
        System.currentTimeMillis() - TimeUnit.HOURS.toMillis(10));
    Assert.assertThrows(
        MissingDataException.class,
        () -> stateLessDIVValidator.checkMissingMessage(record5, Optional.of(errorMetricCallback)));
    verify(errorMetricCallback, times(1)).execute(any());
  }

  private static PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> buildPutRecord(
      PubSubTopicPartition topicPartition,
      long offset,
      GUID producerGUID,
      int segmentNumber,
      int sequenceNumber,
      long brokerTimestamp) {
    Put putPayload = new Put();
    putPayload.putValue = ByteBuffer.wrap("value".getBytes());
    putPayload.schemaId = 0;
    putPayload.replicationMetadataVersionId = VeniceWriter.VENICE_DEFAULT_TIMESTAMP_METADATA_VERSION_ID;
    putPayload.replicationMetadataPayload = ByteBuffer.wrap(new byte[0]);
    return buildRecord(
        topicPartition,
        offset,
        producerGUID,
        segmentNumber,
        sequenceNumber,
        brokerTimestamp,
        MessageType.PUT,
        putPayload);
  }

  private static PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> buildSoSRecord(
      PubSubTopicPartition topicPartition,
      long offset,
      GUID producerGUID,
      int segmentNumber,
      long brokerTimestamp) {
    ControlMessage controlMessage = new ControlMessage();
    controlMessage.controlMessageType = ControlMessageType.START_OF_SEGMENT.getValue();
    StartOfSegment startOfSegment = new StartOfSegment();
    startOfSegment.checksumType = CheckSumType.MD5.getValue();
    startOfSegment.upcomingAggregates = Collections.emptyList();
    controlMessage.controlMessageUnion = startOfSegment;
    return buildRecord(
        topicPartition,
        offset,
        producerGUID,
        segmentNumber,
        0,
        brokerTimestamp,
        MessageType.CONTROL_MESSAGE,
        controlMessage);
  }

  private static PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> buildRecord(
      PubSubTopicPartition topicPartition,
      long offset,
      GUID producerGUID,
      int segmentNumber,
      int sequenceNumber,
      long brokerTimestamp,
      MessageType messageType,
      Object payload) {
    KafkaKeySerializer kafkaKeySerializer = new KafkaKeySerializer();
    KafkaKey kafkaKey = kafkaKeySerializer.deserialize(null, "key".getBytes());
    KafkaMessageEnvelope messageEnvelope = new KafkaMessageEnvelope();
    messageEnvelope.messageType = messageType.getValue();
    messageEnvelope.producerMetadata = new ProducerMetadata();
    messageEnvelope.producerMetadata.producerGUID = producerGUID;
    messageEnvelope.producerMetadata.segmentNumber = segmentNumber;
    messageEnvelope.producerMetadata.messageSequenceNumber = sequenceNumber;
    messageEnvelope.producerMetadata.messageTimestamp = brokerTimestamp;
    messageEnvelope.leaderMetadataFooter = new LeaderMetadata();
    messageEnvelope.leaderMetadataFooter.upstreamOffset = -1;
    messageEnvelope.payloadUnion = payload;

    return new ImmutablePubSubMessage<>(kafkaKey, messageEnvelope, topicPartition, offset, brokerTimestamp, 0);
  }
}
