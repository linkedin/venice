package com.linkedin.venice;

import static com.linkedin.venice.chunking.ChunkKeyValueTransformer.KeyType.WITH_VALUE_CHUNK;
import static com.linkedin.venice.kafka.partitionoffset.PartitionOffsetFetcherImpl.DEFAULT_KAFKA_OFFSET_API_TIMEOUT;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.chunking.ChunkKeyValueTransformer;
import com.linkedin.venice.chunking.ChunkKeyValueTransformerImpl;
import com.linkedin.venice.chunking.RawKeyBytesAndChunkedKeySuffix;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.etl.VeniceKafkaDecodedRecord;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.LeaderMetadata;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.ChunkedValueManifestSerializer;
import com.linkedin.venice.serializer.AvroSpecificDeserializer;
import com.linkedin.venice.storage.protocol.ChunkId;
import com.linkedin.venice.storage.protocol.ChunkedKeySuffix;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class KafkaTopicDumper implements AutoCloseable {
  private static final Logger LOGGER = LogManager.getLogger(KafkaTopicDumper.class);
  private static final String VENICE_ETL_KEY_FIELD = "key";
  private static final String VENICE_ETL_VALUE_FIELD = "value";
  private static final String VENICE_ETL_OFFSET_FIELD = "offset";
  private static final String VENICE_ETL_DELETED_TS_FIELD = "DELETED_TS";
  private static final String VENICE_ETL_METADATA_FIELD = "metadata";

  private static final String VENICE_ETL_BROKER_TIMESTAMP_FIELD = "brokerTimestamp";
  private static final String VENICE_ETL_PRODUCER_TIMESTAMP_FIELD = "producerTimestamp";
  private static final String VENICE_ETL_PARTITION_FIELD = "partition";

  private final String storeName;
  private final String topicName;
  private final int partition;
  private final String keySchemaStr;
  private final String latestValueSchemaStr;
  private final Schema[] allValueSchemas;
  private final boolean isChunkingEnabled;
  private final String parentDirectory;
  private final PubSubConsumerAdapter consumer;
  private final long messageCount;
  private final long endOffset;
  private final int maxConsumeAttempts;
  private final boolean logMetadataOnly;

  private final ChunkKeyValueTransformer chunkKeyValueTransformer;
  private final SpecificDatumReader<ChunkedKeySuffix> specificDatumReader;
  private final AvroSpecificDeserializer<ChunkedKeySuffix> chunkedKeySuffixDeserializer;
  private final ChunkedValueManifestSerializer manifestSerializer;

  // helper objects for saving records to a file
  private DataFileWriter<GenericRecord> dataFileWriter;
  private GenericDatumReader<Object> keyReader;
  private GenericDatumReader<Object>[] valueReaders;
  private DecoderFactory decoderFactory;
  private Schema outputSchema;

  public KafkaTopicDumper(
      ControllerClient controllerClient,
      PubSubConsumerAdapter consumer,
      String topic,
      int partitionNumber,
      long startingOffset,
      int messageCount,
      String parentDir,
      int maxConsumeAttempts,
      boolean logMetadataOnly) {
    this.consumer = consumer;
    this.maxConsumeAttempts = maxConsumeAttempts;
    if (Version.isVersionTopic(topic)) {
      this.storeName = Version.parseStoreFromKafkaTopicName(topic);
      int version = Version.parseVersionFromVersionTopicName(topic);
      this.isChunkingEnabled =
          controllerClient.getStore(storeName).getStore().getVersion(version).get().isChunkingEnabled();
    } else {
      this.storeName = Version.parseStoreFromRealTimeTopic(topic);
      this.isChunkingEnabled = false;
    }
    this.keySchemaStr = controllerClient.getKeySchema(storeName).getSchemaStr();

    if (isChunkingEnabled) {
      chunkKeyValueTransformer = new ChunkKeyValueTransformerImpl(AvroCompatibilityHelper.parse(keySchemaStr));
      specificDatumReader = new SpecificDatumReader<>(ChunkedKeySuffix.class);
      chunkedKeySuffixDeserializer = new AvroSpecificDeserializer<>(specificDatumReader);
      manifestSerializer = new ChunkedValueManifestSerializer(true);
    } else {
      chunkKeyValueTransformer = null;
      specificDatumReader = null;
      chunkedKeySuffixDeserializer = null;
      manifestSerializer = null;
    }

    this.topicName = topic;
    this.partition = partitionNumber;
    this.parentDirectory = parentDir;
    this.logMetadataOnly = logMetadataOnly;
    if (logMetadataOnly) {
      this.latestValueSchemaStr = null;
      this.allValueSchemas = null;
    } else {
      MultiSchemaResponse.Schema[] schemas = controllerClient.getAllValueSchema(storeName).getSchemas();
      LOGGER.info("Found {} value schemas for store {}", schemas.length, storeName);
      this.latestValueSchemaStr = schemas[schemas.length - 1].getSchemaStr();
      this.allValueSchemas = new Schema[schemas.length];
      int i = 0;
      for (MultiSchemaResponse.Schema valueSchema: schemas) {
        this.allValueSchemas[i] = Schema.parse(valueSchema.getSchemaStr());
        i++;
      }
    }
    PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
    PubSubTopicPartition partition =
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(topicName), partitionNumber);

    Long partitionBeginningOffset = consumer.beginningOffset(partition, DEFAULT_KAFKA_OFFSET_API_TIMEOUT);
    long computedStartingOffset = Math.max(partitionBeginningOffset, startingOffset);
    LOGGER.info("Starting from offset: {}", computedStartingOffset);
    consumer.subscribe(partition, computedStartingOffset - 1);
    this.endOffset = consumer.endOffset(partition);
    LOGGER.info("End offset for partition {} is {}", partition, this.endOffset);
    if (messageCount < 0) {
      this.messageCount = this.endOffset;
    } else {
      this.messageCount = messageCount;
    }

    if (!logMetadataOnly) {
      setupDumpFile();
    }
  }

  /**
   * 1. Fetch up to {@link KafkaTopicDumper#messageCount} messages in this partition.
   * 2. Discard non-control messages.
   */
  public int fetchAndProcess() {
    int countdownBeforeStop = maxConsumeAttempts;
    int currentMessageCount = 0;

    int lastReportedConsumedCount = 0;
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> lastProcessRecord = null;
    do {
      Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> records =
          consumer.poll(5000); // up to 5 seconds
      Iterator<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> recordsIterator =
          Utils.iterateOnMapOfLists(records);
      while (recordsIterator.hasNext() && currentMessageCount < messageCount) {
        currentMessageCount++;
        PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record = recordsIterator.next();
        lastProcessRecord = record;
        processRecord(record);
      }

      if (currentMessageCount - lastReportedConsumedCount > 1000) {
        LOGGER.info(
            "Consumed {} messages; last consumed message offset:{}",
            currentMessageCount,
            lastProcessRecord.getOffset());
        lastReportedConsumedCount = currentMessageCount;
      }
      countdownBeforeStop = records.isEmpty() ? countdownBeforeStop - 1 : maxConsumeAttempts;
    } while ((lastProcessRecord != null && lastProcessRecord.getOffset() < (this.endOffset - 2))
        && currentMessageCount < messageCount && countdownBeforeStop > 0);
    return currentMessageCount;
  }

  private void setupDumpFile() {
    // build file
    File dataFile = new File(this.parentDirectory + this.topicName + "_" + this.partition + ".avro");
    List<Schema.Field> outputSchemaFields = new ArrayList<>();
    for (Schema.Field field: VeniceKafkaDecodedRecord.SCHEMA$.getFields()) {
      if (field.name().equals(VENICE_ETL_KEY_FIELD)) {
        outputSchemaFields
            .add(AvroCompatibilityHelper.newField(field).setSchema(Schema.parse(this.keySchemaStr)).build());
      } else if (field.name().equals(VENICE_ETL_VALUE_FIELD)) {
        outputSchemaFields.add(
            AvroCompatibilityHelper.newField(field)
                .setSchema(
                    Schema.createUnion(
                        Arrays.asList(Schema.create(Schema.Type.NULL), Schema.parse(this.latestValueSchemaStr))))
                .build());
      } else {
        // any fields except key and value will be added using the original schemas, like the offset field and the
        // DELETED_TS field
        outputSchemaFields.add(AvroCompatibilityHelper.newField(field).build());
      }
    }
    outputSchema = Schema.createRecord("KafkaRecord", "", "none", false);
    outputSchema.setFields(outputSchemaFields);
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(outputSchema);
    dataFileWriter = new DataFileWriter<>(datumWriter);
    try {
      dataFileWriter.create(outputSchema, dataFile);
    } catch (IOException e) {
      throw new VeniceException("Failed on creating avro file", e);
    }

    // build key/value reader
    Schema keySchema = Schema.parse(keySchemaStr);
    keyReader = new GenericDatumReader<>(keySchema, keySchema);

    int valueSchemaNum = allValueSchemas.length;
    valueReaders = new GenericDatumReader[valueSchemaNum];
    for (int schemaId = 0; schemaId < valueSchemaNum; schemaId++) {
      valueReaders[schemaId] = new GenericDatumReader<>(allValueSchemas[schemaId], allValueSchemas[valueSchemaNum - 1]);
    }
    decoderFactory = new DecoderFactory();
  }

  private static final String REGULAR_REC = "REG";
  private static final String CONTROL_REC = "CTRL";

  /**
   * Log the metadata for each kafka message.
   */
  private void logRecordMetadata(PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record) {
    try {
      KafkaKey kafkaKey = record.getKey();
      KafkaMessageEnvelope kafkaMessageEnvelope = record.getValue();
      ProducerMetadata producerMetadata = kafkaMessageEnvelope.producerMetadata;

      String msgType = kafkaKey.isControlMessage()
          ? ControlMessageType.valueOf((ControlMessage) kafkaMessageEnvelope.payloadUnion).toString()
          : MessageType.valueOf(kafkaMessageEnvelope).toString();

      LeaderMetadata leaderMetadata = kafkaMessageEnvelope.leaderMetadataFooter;

      final String chunkMetadata = getChunkMetadataLog(record);

      LOGGER.info(
          "{} {} Offset:{} ProducerMd=(guid:{},seg:{},seq:{},mts:{},lts:{}) LeaderMd=(host:{},uo:{},ukcId:{}){}",
          kafkaKey.isControlMessage() ? CONTROL_REC : REGULAR_REC,
          msgType,
          record.getOffset(),
          GuidUtils.getHexFromGuid(producerMetadata.producerGUID),
          producerMetadata.segmentNumber,
          producerMetadata.messageSequenceNumber,
          producerMetadata.messageTimestamp,
          producerMetadata.logicalTimestamp,
          leaderMetadata == null ? "-" : leaderMetadata.hostName,
          leaderMetadata == null ? "-" : leaderMetadata.upstreamOffset,
          leaderMetadata == null ? "-" : leaderMetadata.upstreamKafkaClusterId,
          chunkMetadata);
    } catch (Exception e) {
      LOGGER.error("Failed when building record for offset {}", record.getOffset(), e);
    }
  }

  private void processRecord(PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record) {
    if (logMetadataOnly) {
      logRecordMetadata(record);
      return;
    }
    writeToFile(record);
  }

  private void writeToFile(PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record) {
    try {
      KafkaKey kafkaKey = record.getKey();
      KafkaMessageEnvelope kafkaMessageEnvelope = record.getValue();
      if (kafkaKey.isControlMessage()) {
        LOGGER.info("Found a control message, continue");
        return;
      }
      // build the record
      GenericRecord convertedRecord = new GenericData.Record(outputSchema);
      convertedRecord.put(VENICE_ETL_OFFSET_FIELD, record.getOffset());

      byte[] keyBytes = kafkaKey.getKey();
      Decoder keyDecoder = decoderFactory.binaryDecoder(keyBytes, null);
      Object keyRecord = keyReader.read(null, keyDecoder);
      convertedRecord.put(VENICE_ETL_KEY_FIELD, keyRecord);
      Map<CharSequence, CharSequence> metadataMap = new HashMap<>();

      metadataMap.put(VENICE_ETL_PARTITION_FIELD, String.valueOf(record.getPartition()));
      metadataMap.put(
          VENICE_ETL_PRODUCER_TIMESTAMP_FIELD,
          String.valueOf(kafkaMessageEnvelope.producerMetadata.messageTimestamp));
      metadataMap.put(VENICE_ETL_BROKER_TIMESTAMP_FIELD, String.valueOf(record.getPubSubMessageTime()));

      convertedRecord.put(VENICE_ETL_METADATA_FIELD, metadataMap);

      switch (MessageType.valueOf(kafkaMessageEnvelope)) {
        case PUT:
          // put message
          Put put = (Put) kafkaMessageEnvelope.payloadUnion;
          ByteBuffer putValue = put.putValue;
          int schemaId = put.schemaId;
          Decoder valueDecoder = decoderFactory.binaryDecoder(ByteUtils.extractByteArray(putValue), null);
          Object valueRecord = valueReaders[schemaId - 1].read(null, valueDecoder);
          convertedRecord.put(VENICE_ETL_VALUE_FIELD, valueRecord);
          break;
        case DELETE:
          convertedRecord.put(VENICE_ETL_DELETED_TS_FIELD, record.getOffset());
          break;
        case UPDATE:
          LOGGER.info("Found update message! continue");
          break;
        default:
          throw new VeniceException("How come?");
      }
      dataFileWriter.append(convertedRecord);
    } catch (Exception e) {
      LOGGER.error("Failed when building record for offset {}", record.getOffset(), e);
    }
  }

  // Visible for testing
  String getChunkMetadataLog(PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record) throws IOException {
    KafkaKey kafkaKey = record.getKey();
    KafkaMessageEnvelope kafkaMessageEnvelope = record.getValue();
    if (this.isChunkingEnabled && !kafkaKey.isControlMessage()) {
      MessageType messageType = MessageType.valueOf(kafkaMessageEnvelope);
      int schemaId;
      switch (messageType) {
        case PUT:
          schemaId = ((Put) kafkaMessageEnvelope.payloadUnion).schemaId;
          break;
        case DELETE:
          schemaId = -1;
          break;
        default:
          throw new IOException(
              "Unexpected '" + messageType + "' message from Topic: " + record.getTopicName() + " Partition: "
                  + record.getPartition());
      }

      final ChunkKeyValueTransformer.KeyType keyType = ChunkKeyValueTransformer.getKeyType(messageType, schemaId);
      ChunkId firstChunkId = getFirstChunkId(keyType, kafkaKey, kafkaMessageEnvelope);

      if (firstChunkId == null) {
        String chunkMetadataFormatWithoutFirstChunkMd = " ChunkMd=(type:%s)";
        return String.format(chunkMetadataFormatWithoutFirstChunkMd, keyType);
      } else {
        String chunkMetadataFormatWithFirstChunkMd = " ChunkMd=(type:%s, FirstChunkMd=(guid:%s,seg:%d,seq:%d))";
        return String.format(
            chunkMetadataFormatWithFirstChunkMd,
            keyType,
            GuidUtils.getHexFromGuid(firstChunkId.producerGUID),
            firstChunkId.segmentNumber,
            firstChunkId.messageSequenceNumber);
      }
    } else {
      return "";
    }
  }

  private ChunkId getFirstChunkId(
      ChunkKeyValueTransformer.KeyType keyType,
      KafkaKey kafkaKey,
      KafkaMessageEnvelope kafkaMessageEnvelope) {
    final RawKeyBytesAndChunkedKeySuffix rawKeyBytesAndChunkedKeySuffix =
        chunkKeyValueTransformer.splitChunkedKey(kafkaKey.getKey(), keyType);

    final ByteBuffer chunkedKeySuffixBytes = rawKeyBytesAndChunkedKeySuffix.getChunkedKeySuffixBytes();
    final ChunkedKeySuffix chunkedKeySuffix = chunkedKeySuffixDeserializer.deserialize(chunkedKeySuffixBytes);

    switch (keyType) {
      case WITH_VALUE_CHUNK:
        return chunkedKeySuffix.chunkId;
      case WITH_CHUNK_MANIFEST:
        ChunkedValueManifest chunkedValueManifest = manifestSerializer.deserialize(
            ByteUtils.extractByteArray(((Put) kafkaMessageEnvelope.payloadUnion).putValue),
            AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion());

        ByteBuffer firstChunkKeyWithChunkIdSuffix = chunkedValueManifest.keysWithChunkIdSuffix.get(0);
        final RawKeyBytesAndChunkedKeySuffix firstChunkRawKeyBytesAndChunkedKeySuffix = chunkKeyValueTransformer
            .splitChunkedKey(ByteUtils.extractByteArray(firstChunkKeyWithChunkIdSuffix), WITH_VALUE_CHUNK);
        final ByteBuffer firstChunkKeySuffixBytes = firstChunkRawKeyBytesAndChunkedKeySuffix.getChunkedKeySuffixBytes();
        final ChunkedKeySuffix firstChunkedKeySuffix =
            chunkedKeySuffixDeserializer.deserialize(firstChunkKeySuffixBytes);

        return firstChunkedKeySuffix.chunkId;
      case WITH_FULL_VALUE:
        return null;
      default:
        throw new VeniceException("Unexpected key type: " + keyType);
    }
  }

  @Override
  public void close() throws Exception {
    if (dataFileWriter != null) {
      dataFileWriter.close();
    }
  }
}
