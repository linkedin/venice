package com.linkedin.venice;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.etl.VeniceKafkaDecodedRecord;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.GUID;
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
import java.util.LinkedList;
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
import org.apache.log4j.Logger;
import org.codehaus.jackson.node.JsonNodeFactory;


public class KafkaTopicDumper {
  private static final Logger logger = Logger.getLogger(KafkaTopicDumper.class);
  public static final String VENICE_ETL_KEY_FIELD = "key";
  public static final String VENICE_ETL_VALUE_FIELD = "value";
  public static final String VENICE_ETL_OFFSET_FIELD = "offset";
  public static final String VENICE_ETL_DELETED_TS_FIELD = "DELETED_TS";
  public static final String VENICE_ETL_METADATA_FIELD = "metadata";
  public static final String VENICE_ETL_SCHEMAID_FIELD = "schemaId";

  private ControllerClient controllerClient;
  private String storeName;
  private String topicName;
  private int partition;
  private String keySchemaStr;
  private String latestValueSchemaStr;
  private Schema[] allValueSchemas;
  private String parentDirectory;
  private List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>> kafkaRecordList;
  private KafkaConsumer consumer;
  private long messageCount;
  private long endOffset;
  private int max_consume_attempts;

  public KafkaTopicDumper(ControllerClient controllerClient, Properties consumerProps, String topic, int partitionNumber,  int startingOffset, int messageCount, String parentDir, int maxConsumeAttempts) {
    this.max_consume_attempts = maxConsumeAttempts;
    this.controllerClient = controllerClient;
    this.storeName = Version.parseStoreFromKafkaTopicName(topic);
    this.topicName = topic;
    this.partition = partitionNumber;
    this.parentDirectory = parentDir;
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
    this.consumer = new KafkaConsumer(consumerProps);

    TopicPartition partition = new TopicPartition(topic, partitionNumber);
    consumer.assign(Collections.singletonList(partition));
    consumer.seek(partition, startingOffset);
    Map<TopicPartition, Long> partitionToEndOffset = consumer.endOffsets(Collections.singletonList(partition));
    logger.info("End offset for partition " + partition.partition() + " is " + partitionToEndOffset.get(partition));
    this.endOffset = partitionToEndOffset.get(partition);
    if (messageCount < 0) {
      this.messageCount = partitionToEndOffset.get(partition);
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
    int countdownBeforeStop = max_consume_attempts;
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
        if (MessageType.valueOf(envelope) != MessageType.CONTROL_MESSAGE) {
          this.kafkaRecordList.add(record);
        }
      }

      if (currentMessageCount - lastReportedConsumedCount > 1000) {
        logger.info("Consumed " + currentMessageCount + " messages; last consumed message offset: " + lastProcessRecord.offset());
        lastReportedConsumedCount = currentMessageCount;
      }
      countdownBeforeStop = records.count() == 0 ? countdownBeforeStop - 1 : max_consume_attempts;
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
        outputSchemaFields.add(new Schema.Field(field.name(), Schema.parse(this.keySchemaStr), field.doc(), field.defaultValue(), field.order()));
      } else if (field.name().equals(VENICE_ETL_VALUE_FIELD)) {
        outputSchemaFields.add(new Schema.Field(field.name(), Schema.createUnion(
            Arrays.asList(Schema.create(Schema.Type.NULL), Schema.parse(this.latestValueSchemaStr))), field.doc(), field.defaultValue(), field.order()));
      } else if (!field.name().equals(VENICE_ETL_METADATA_FIELD)) {
        // any fields except key, value and metadata will be added using the original schemas, like the offset field and the DELETED_TS field
        outputSchemaFields.add(new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultValue(), field.order()));
      }
    }
    // add partition information
    outputSchemaFields.add(new Schema.Field("partitionId", Schema.create(Schema.Type.INT), "", JsonNodeFactory.instance.numberNode(-1)));
    Schema outputSchema = Schema.createRecord("KafkaRecord", "", "none", false);
    outputSchema.setFields(outputSchemaFields);
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(outputSchema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    try {
      dataFileWriter.create(outputSchema, file);
    } catch (Exception e) {
      logger.error("Failed on creating avro file");
    }

    // build key/value reader
    GenericDatumReader<Object> keyReader = null;
    GenericDatumReader<Object>[] valueReaders = null;
    keyReader = new GenericDatumReader<>(Schema.parse(this.keySchemaStr));

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
        Decoder keyDecoder = decoderFactory.createBinaryDecoder(kafkaKey.getKey(), null);
        Object keyRecord = keyReader.read(null, keyDecoder);
        convertedRecord.put(VENICE_ETL_KEY_FIELD, keyRecord);
        convertedRecord.put("partitionId", record.partition());
        switch (MessageType.valueOf(kafkaMessageEnvelope)) {
          case PUT:
            // put message
            Put put = (Put) kafkaMessageEnvelope.payloadUnion;
            ByteBuffer putValue = put.putValue;
            int schemaId = put.schemaId;
            Decoder valueDecoder = decoderFactory.createBinaryDecoder(ByteUtils.extractByteArray(put.putValue), null);
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
    try {
      dataFileWriter.flush();
      dataFileWriter.close();
    } catch (Exception e) {
      logger.error("Failed when flushing avro file");
    }
  }
}
