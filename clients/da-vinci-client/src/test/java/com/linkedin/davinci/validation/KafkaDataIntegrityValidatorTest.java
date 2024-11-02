package com.linkedin.davinci.validation;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertThrows;

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
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.utils.DataProviderUtils;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;


public class KafkaDataIntegrityValidatorTest {
  private static final Logger LOGGER = LogManager.getLogger(KafkaDataIntegrityValidatorTest.class);
  private static final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @Test(dataProvider = "CheckpointingSupported-CheckSum-Types", dataProviderClass = DataProviderUtils.class)
  public void testClearExpiredState(CheckSumType checkSumType) throws InterruptedException {
    final long maxAgeInMs = 1000;
    Time time = new TestMockTime();
    String kafkaTopic = Utils.getUniqueString("TestStore") + "_v1";
    KafkaDataIntegrityValidator validator =
        new KafkaDataIntegrityValidator(kafkaTopic, KafkaDataIntegrityValidator.DISABLED, maxAgeInMs);
    PubSubTopicPartition topicPartition0 = new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(kafkaTopic), 0);
    PubSubTopicPartition topicPartition1 = new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(kafkaTopic), 1);
    long offsetForPartition0 = 0;
    long offsetForPartition1 = 0;
    int seqNumberForPartition0Guid1 = 1;
    GUID producerGuid0 = GuidUtils.getGUID(VeniceProperties.empty());
    GUID producerGuid1 = GuidUtils.getGUID(VeniceProperties.empty());
    OffsetRecord p0offsetRecord = mock(OffsetRecord.class);
    OffsetRecord p1offsetRecord = mock(OffsetRecord.class);
    if (producerGuid0.equals(producerGuid1)) {
      LOGGER.info("Got two equal producer GUIDs! Buy a lottery ticket!");
      // Extremely unlikely, but theoretically possible... let's just re-run the test
      testClearExpiredState(checkSumType);
      return;
    }
    assertNotEquals(producerGuid0, producerGuid1);
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> p0g0record0 = buildSoSRecord(
        topicPartition0,
        offsetForPartition0++,
        producerGuid0,
        time.getMilliseconds(),
        p0offsetRecord,
        checkSumType);
    validator.validateMessage(PartitionTracker.VERSION_TOPIC, p0g0record0, false, Lazy.FALSE);

    time.sleep(10);

    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> p0g0record1 = buildPutRecord(
        topicPartition0,
        offsetForPartition0++,
        producerGuid0,
        0,
        seqNumberForPartition0Guid1++,
        time.getMilliseconds(),
        p0offsetRecord);
    validator.validateMessage(PartitionTracker.VERSION_TOPIC, p0g0record1, false, Lazy.FALSE);

    assertEquals(validator.getNumberOfTrackedPartitions(), 1);
    assertEquals(validator.getNumberOfTrackedProducerGUIDs(), 1);

    // Nothing should be cleared yet
    validator.updateOffsetRecordForPartition(PartitionTracker.VERSION_TOPIC, 0, p0offsetRecord);
    assertEquals(validator.getNumberOfTrackedPartitions(), 1);
    assertEquals(validator.getNumberOfTrackedProducerGUIDs(), 1);

    // Even if we wait some more, the state should still be retained, since the wall-clock time does not matter, only
    // the last consumed time does.
    time.sleep(2 * maxAgeInMs);
    validator.updateOffsetRecordForPartition(PartitionTracker.VERSION_TOPIC, 0, p0offsetRecord);
    assertEquals(validator.getNumberOfTrackedPartitions(), 1);
    assertEquals(validator.getNumberOfTrackedProducerGUIDs(), 1);

    /**
     * Start writing into the same partition with another producer GUID. In effect, this will bump up the highest
     * timestamp of that partition, which will make the first producer eligible for getting cleared, but it will not
     * be cleared yet, since {@link KafkaDataIntegrityValidator#clearExpiredState(int, LongSupplier)} will not have
     * been called.
     */
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> p0g1record0 = buildSoSRecord(
        topicPartition0,
        offsetForPartition0++,
        producerGuid1,
        time.getMilliseconds(),
        p0offsetRecord,
        checkSumType);
    validator.validateMessage(PartitionTracker.VERSION_TOPIC, p0g1record0, false, Lazy.FALSE);
    assertEquals(validator.getNumberOfTrackedPartitions(), 1);
    assertEquals(validator.getNumberOfTrackedProducerGUIDs(), 2);

    // After calling clear, now we should see the update to the internal state...
    validator.updateOffsetRecordForPartition(PartitionTracker.VERSION_TOPIC, 0, p0offsetRecord);
    assertEquals(validator.getNumberOfTrackedPartitions(), 1);
    assertEquals(validator.getNumberOfTrackedProducerGUIDs(), 1);

    // Start writing into another partition
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> p1g0record0 = buildSoSRecord(
        topicPartition1,
        offsetForPartition1,
        producerGuid0,
        time.getMilliseconds(),
        p1offsetRecord,
        checkSumType);
    validator.validateMessage(PartitionTracker.VERSION_TOPIC, p1g0record0, false, Lazy.FALSE);
    assertEquals(validator.getNumberOfTrackedPartitions(), 2);
    assertEquals(validator.getNumberOfTrackedProducerGUIDs(), 2);

    // If, somehow, a message still came from this GUID in partition 0, after clearing the state, DIV should catch it
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> p0g0record2 = buildPutRecord(
        topicPartition0,
        offsetForPartition0,
        producerGuid0,
        0,
        seqNumberForPartition0Guid1,
        time.getMilliseconds(),
        p0offsetRecord);
    assertThrows(
        ImproperlyStartedSegmentException.class,
        () -> validator.validateMessage(PartitionTracker.VERSION_TOPIC, p0g0record2, false, Lazy.FALSE));
    assertEquals(validator.getNumberOfTrackedPartitions(), 2);
    assertEquals(validator.getNumberOfTrackedProducerGUIDs(), 2);

    // This is a stable state, so no changes are expected...
    validator.updateOffsetRecordForPartition(PartitionTracker.VERSION_TOPIC, 0, p0offsetRecord);
    validator.updateOffsetRecordForPartition(PartitionTracker.VERSION_TOPIC, 1, p1offsetRecord);
    assertEquals(validator.getNumberOfTrackedPartitions(), 2);
    assertEquals(validator.getNumberOfTrackedProducerGUIDs(), 2);
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

    PartitionTracker.DIVErrorMetricCallback errorMetricCallback = mock(PartitionTracker.DIVErrorMetricCallback.class);

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
    return buildPutRecord(topicPartition, offset, producerGUID, segmentNumber, sequenceNumber, brokerTimestamp, null);
  }

  private static PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> buildPutRecord(
      PubSubTopicPartition topicPartition,
      long offset,
      GUID producerGUID,
      int segmentNumber,
      int sequenceNumber,
      long brokerTimestamp,
      OffsetRecord offsetRecord) {
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
        putPayload,
        offsetRecord);
  }

  private static PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> buildSoSRecord(
      PubSubTopicPartition topicPartition,
      long offset,
      GUID producerGUID,
      long brokerTimestamp,
      OffsetRecord offsetRecord,
      CheckSumType type) {
    ControlMessage controlMessage = new ControlMessage();
    controlMessage.controlMessageType = ControlMessageType.START_OF_SEGMENT.getValue();
    StartOfSegment startOfSegment = new StartOfSegment();
    startOfSegment.checksumType = type.getValue();
    startOfSegment.upcomingAggregates = Collections.emptyList();
    controlMessage.controlMessageUnion = startOfSegment;
    return buildRecord(
        topicPartition,
        offset,
        producerGUID,
        0,
        0,
        brokerTimestamp,
        MessageType.CONTROL_MESSAGE,
        controlMessage,
        offsetRecord);
  }

  private static PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> buildRecord(
      PubSubTopicPartition topicPartition,
      long offset,
      GUID producerGUID,
      int segmentNumber,
      int sequenceNumber,
      long brokerTimestamp,
      MessageType messageType,
      Object payload,
      OffsetRecord offsetRecord) {
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

    if (offsetRecord != null && offsetRecord.getMaxMessageTimeInMs() < brokerTimestamp) {
      when(offsetRecord.getMaxMessageTimeInMs()).thenReturn(brokerTimestamp);
    }

    return new ImmutablePubSubMessage<>(kafkaKey, messageEnvelope, topicPartition, offset, brokerTimestamp, 0);
  }
}
