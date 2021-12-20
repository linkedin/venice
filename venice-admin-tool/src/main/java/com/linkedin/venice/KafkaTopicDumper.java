package com.linkedin.venice;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.etl.VeniceKafkaDecodedRecord;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.ByteUtils;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class KafkaTopicDumper {
  private static final Logger logger = LogManager.getLogger(KafkaTopicDumper.class);
  public static final String VENICE_ETL_KEY_FIELD = "key";
  public static final String VENICE_ETL_VALUE_FIELD = "value";
  public static final String VENICE_ETL_OFFSET_FIELD = "offset";
  public static final String VENICE_ETL_DELETED_TS_FIELD = "DELETED_TS";
  public static final String VENICE_ETL_METADATA_FIELD = "metadata";

  public static final String VENICE_ETL_BROKER_TIMESTAMP_FIELD = "brokerTimestamp";
  public static final String VENICE_ETL_PRODUCER_TIMESTAMP_FIELD = "producerTimestamp";
  public static final String VENICE_ETL_PARTITION_FIELD = "partition";

  private final String storeName;
  private final String topicName;
  private final int partition;
  private final String keySchemaStr;
  private final String latestValueSchemaStr;
  private final Schema[] allValueSchemas;
  private final String parentDirectory;
  private final List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>> kafkaRecordList;
  private final KafkaConsumer consumer;
  private final long messageCount;
  private final long endOffset;
  private final int maxConsumeAttempts;
  private final boolean logMetadataOnly;

  public KafkaTopicDumper(ControllerClient controllerClient, Properties consumerProps, String topic, int partitionNumber,  long startingOffset, int messageCount, String parentDir, int maxConsumeAttempts, boolean logMetadataOnly) {
    this.maxConsumeAttempts = maxConsumeAttempts;
    if (Version.isVersionTopic(topic)) {
      this.storeName = Version.parseStoreFromKafkaTopicName(topic);
    } else {
      this.storeName = Version.parseStoreFromRealTimeTopic(topic);
    }
    this.topicName = topic;
    this.partition = partitionNumber;
    this.parentDirectory = parentDir;
    this.logMetadataOnly = logMetadataOnly;
    if (logMetadataOnly) {
      this.keySchemaStr = null;
      this.latestValueSchemaStr = null;
      allValueSchemas = null;
    } else {
      this.keySchemaStr = controllerClient.getKeySchema(storeName).getSchemaStr();
      MultiSchemaResponse.Schema[] schemas = controllerClient.getAllValueSchema(storeName).getSchemas();
      logger.info("Found " + schemas.length + " value schemas for store " + storeName);
      this.latestValueSchemaStr = schemas[schemas.length - 1].getSchemaStr();
      allValueSchemas = new Schema[schemas.length];
      int i = 0;
      for (MultiSchemaResponse.Schema valueSchema : schemas) {
        this.allValueSchemas[i] = Schema.parse(valueSchema.getSchemaStr());
        i++;
      }
    }
    this.consumer = new KafkaConsumer(consumerProps);

    TopicPartition partition = new TopicPartition(topic, partitionNumber);
    List<TopicPartition> partitions = Collections.singletonList(partition);
    consumer.assign(partitions);
    Map<TopicPartition, Long> partitionToBeginningOffset = consumer.beginningOffsets(partitions);
    long computedStartingOffset = Math.max(partitionToBeginningOffset.get(partition), startingOffset);
    logger.info("Starting from offset: " + computedStartingOffset);
    consumer.seek(partition, computedStartingOffset);
    Map<TopicPartition, Long> partitionToEndOffset = consumer.endOffsets(partitions);
    this.endOffset = partitionToEndOffset.get(partition);
    logger.info("End offset for partition " + partition.partition() + " is " + this.endOffset);
    if (messageCount < 0) {
      this.messageCount = this.endOffset;
    } else {
      this.messageCount = messageCount;
    }
    this.kafkaRecordList = new ArrayList<>((int)this.messageCount);
  }



  /**
   * 1. Fetch up to {@link KafkaTopicDumper#messageCount} messages in this partition.
   * 2. Discard non-control messages.
   */
  public KafkaTopicDumper fetch() {
    int countdownBeforeStop = maxConsumeAttempts;
    int currentMessageCount = 0;

    int lastReportedConsumedCount = 0;
    ConsumerRecord<KafkaKey, KafkaMessageEnvelope> lastProcessRecord = null;
    do {
      ConsumerRecords records = consumer.poll(5000); // up to 5 seconds
      Iterator<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>> iter = records.iterator();
      while (iter.hasNext() && currentMessageCount < messageCount) {
        currentMessageCount++;
        ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record = iter.next();
        KafkaMessageEnvelope envelope = record.value();
        lastProcessRecord = record;
        if (logMetadataOnly || MessageType.valueOf(envelope) != MessageType.CONTROL_MESSAGE) {
          this.kafkaRecordList.add(record);
        }
      }

      if (currentMessageCount - lastReportedConsumedCount > 1000) {
        logger.info("Consumed " + currentMessageCount + " messages; last consumed message offset: " + lastProcessRecord.offset());
        lastReportedConsumedCount = currentMessageCount;
      }
      countdownBeforeStop = records.count() == 0 ? countdownBeforeStop - 1 : maxConsumeAttempts;
      if (lastProcessRecord.offset() >= (this.endOffset - 2)) {
        return this;
      }
    } while (currentMessageCount < messageCount && countdownBeforeStop > 0 && lastProcessRecord.offset() < (this.endOffset - 2));

    return this;
  }


  /**
   *  Display control messages from each producer
   */
  public void dumpToFile() {
    // build file
    File file = new File(this.parentDirectory + this.topicName + "_" + this.partition + ".avro");
    List<Schema.Field> outputSchemaFields = new ArrayList<>();
    for (Schema.Field field : VeniceKafkaDecodedRecord.SCHEMA$.getFields()) {
      if (field.name().equals(VENICE_ETL_KEY_FIELD)) {
        outputSchemaFields.add(AvroCompatibilityHelper.newField(field).setSchema(Schema.parse(this.keySchemaStr)).build());
      } else if (field.name().equals(VENICE_ETL_VALUE_FIELD)) {
        outputSchemaFields.add(AvroCompatibilityHelper.newField(field)
                                  .setSchema(Schema.createUnion(Arrays.asList(
                                           Schema.create(Schema.Type.NULL),
                                           Schema.parse(this.latestValueSchemaStr))))
                                  .build());
      } else {
        // any fields except key and value will be added using the original schemas, like the offset field and the DELETED_TS field
        outputSchemaFields.add(AvroCompatibilityHelper.newField(field).build());
      }
    }
    Schema outputSchema = Schema.createRecord("KafkaRecord", "", "none", false);
    outputSchema.setFields(outputSchemaFields);
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(outputSchema);
    try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
      try {
        dataFileWriter.create(outputSchema, file);
      } catch (IOException e) {
        throw new VeniceException("Failed on creating avro file", e);
      }

      // build key/value reader
      GenericDatumReader<Object> keyReader = null;
      GenericDatumReader<Object>[] valueReaders = null;
      Schema keySchema = Schema.parse(keySchemaStr);
      keyReader = new GenericDatumReader<>(keySchema, keySchema);

      int valueSchemaNum = allValueSchemas.length;
      valueReaders = new GenericDatumReader[valueSchemaNum];
      for (int schemaId = 0; schemaId < valueSchemaNum; schemaId++) {
        valueReaders[schemaId] = new GenericDatumReader<>(allValueSchemas[schemaId], allValueSchemas[valueSchemaNum - 1]);
      }

      DecoderFactory decoderFactory = new DecoderFactory();
      for (ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record : kafkaRecordList) {
        try {
          KafkaKey kafkaKey = record.key();
          KafkaMessageEnvelope kafkaMessageEnvelope = record.value();
          if (kafkaKey.isControlMessage()) {
            logger.info("Found a control message, continue");
            continue;
          }
          // build the record
          GenericRecord convertedRecord = new GenericData.Record(outputSchema);
          convertedRecord.put(VENICE_ETL_OFFSET_FIELD, record.offset());

          byte[] keyBytes = kafkaKey.getKey();
          Decoder keyDecoder = decoderFactory.createBinaryDecoder(keyBytes, null);
          Object keyRecord = keyReader.read(null, keyDecoder);
          convertedRecord.put(VENICE_ETL_KEY_FIELD, keyRecord);
          Map<CharSequence, CharSequence> metadataMap = new HashMap<>();

          metadataMap.put(VENICE_ETL_PARTITION_FIELD, String.valueOf(record.partition()));
          metadataMap.put(VENICE_ETL_PRODUCER_TIMESTAMP_FIELD, String.valueOf(kafkaMessageEnvelope.producerMetadata.messageTimestamp));
          metadataMap.put(VENICE_ETL_BROKER_TIMESTAMP_FIELD, String.valueOf(TimestampType.LOG_APPEND_TIME.equals(record.timestampType()) ? record.timestamp() : 0));

          convertedRecord.put(VENICE_ETL_METADATA_FIELD, metadataMap);

          switch (MessageType.valueOf(kafkaMessageEnvelope)) {
            case PUT:
              // put message
              Put put = (Put) kafkaMessageEnvelope.payloadUnion;
              ByteBuffer putValue = put.putValue;
              int schemaId = put.schemaId;
              Decoder valueDecoder = decoderFactory.createBinaryDecoder(ByteUtils.extractByteArray(putValue), null);
              Object valueRecord = valueReaders[schemaId - 1].read(null, valueDecoder);
              convertedRecord.put(VENICE_ETL_VALUE_FIELD, valueRecord);
              break;
            case DELETE:
              convertedRecord.put(VENICE_ETL_DELETED_TS_FIELD, record.offset());
              break;
            case UPDATE:
              logger.info("Found update message! continue");
              continue;
            default:
              throw new VeniceException("How come?");
          }
          dataFileWriter.append(convertedRecord);
        } catch (Exception e) {
          logger.error("Failed when building record for offset " + record.offset(), e);
        }
      }
    } catch (IOException e) {
      throw new VeniceException("IOException while trying to dump to file.", e);
    }
  }

  /**
   * Log the metadata for each kafka message.
   */
  public void logMetadata() {
    for (ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record : kafkaRecordList) {
      try {
        KafkaKey kafkaKey = record.key();
        KafkaMessageEnvelope kafkaMessageEnvelope = record.value();

        ProducerMetadata producerMetadata = kafkaMessageEnvelope.producerMetadata;

        String msg = kafkaKey.isControlMessage() ? ControlMessageType.valueOf((ControlMessage) kafkaMessageEnvelope.payloadUnion)
            .toString() : MessageType.valueOf(kafkaMessageEnvelope).toString();
        logger.info("KafkaOffset: " + record.offset() + " ControlMsg: " + (kafkaKey.isControlMessage() ? "Yes" : "No")
            + " msgType: " + msg + " ProducerMetadata:(" + "guid: " + producerMetadata.producerGUID.toString()
            + " seq: " + producerMetadata.messageSequenceNumber + " segment: " + producerMetadata.segmentNumber
            + " upstreamOffset: " + producerMetadata.upstreamOffset+ " messageTimestamp: " + producerMetadata.messageTimestamp
            + ") footer hostname: " + (kafkaMessageEnvelope.leaderMetadataFooter == null ? "" : kafkaMessageEnvelope.leaderMetadataFooter.hostName)
            + " footer offset: " + (kafkaMessageEnvelope.leaderMetadataFooter == null ? "" : kafkaMessageEnvelope.leaderMetadataFooter.upstreamOffset));
      } catch (Exception e) {
        logger.error("Failed when building record for offset " + record.offset(), e);
      }
    }
  }
}
