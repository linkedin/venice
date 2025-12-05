package com.linkedin.venice;

import static com.linkedin.venice.chunking.ChunkKeyValueTransformer.KeyType.WITH_VALUE_CHUNK;

import com.github.luben.zstd.Zstd;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.chunking.ChunkKeyValueTransformer;
import com.linkedin.venice.chunking.ChunkKeyValueTransformerImpl;
import com.linkedin.venice.chunking.RawKeyBytesAndChunkedKeySuffix;
import com.linkedin.venice.client.change.capture.protocol.RecordChangeEvent;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.etl.VeniceKafkaDecodedRecord;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.LeaderMetadata;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.TopicSwitch;
import com.linkedin.venice.kafka.protocol.Update;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubPositionDeserializer;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.PubSubUtil;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientException;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.serialization.avro.ChunkedValueManifestSerializer;
import com.linkedin.venice.serializer.AvroSpecificDeserializer;
import com.linkedin.venice.storage.protocol.ChunkedKeySuffix;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.DictionaryUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.views.ChangeCaptureView;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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


/**
 * This class contains logic to dump Venice Kafka topics.
 * It has several modes:
 * (1) Log metadata: Print out Kafka message metadata to console.
 * (2) Log data record: Print out data record's key/value and optionally RMD to console. For now it only supports version
 * topic and realtime topic.
 * (3) Save data record to file: If both (1)&(2) is not enabled, it will save all the data record value payload to local disk.
 */
public class KafkaTopicDumper implements AutoCloseable {
  private static final Logger LOGGER = LogManager.getLogger(KafkaTopicDumper.class);
  private static final PubSubTopicRepository TOPIC_REPOSITORY = new PubSubTopicRepository();
  private static final String VENICE_ETL_KEY_FIELD = "key";
  private static final String VENICE_ETL_VALUE_FIELD = "value";
  private static final String VENICE_ETL_POSITION_FIELD = "position";
  private static final String VENICE_ETL_DELETED_TS_FIELD = "DELETED_TS";
  private static final String VENICE_ETL_METADATA_FIELD = "metadata";

  private static final String VENICE_ETL_BROKER_TIMESTAMP_FIELD = "brokerTimestamp";
  private static final String VENICE_ETL_PRODUCER_TIMESTAMP_FIELD = "producerTimestamp";
  private static final String VENICE_ETL_PARTITION_FIELD = "partition";
  private static final String REGULAR_REC = "REG";
  private static final String CONTROL_REC = "CTRL";

  private final String keySchemaStr;
  private final Schema keySchema;
  private final String latestValueSchemaStr;
  private final Schema[] allValueSchemas;
  private final Map<Integer, ValueAndDerivedSchemaData> schemaDataMap = new VeniceConcurrentHashMap<>();
  private final boolean isChunkingEnabled;
  private final CompressorFactory compressorFactory;
  private final VeniceCompressor compressor;
  private final String parentDirectory;
  private final PubSubConsumerAdapter consumer;
  private final int maxConsumeAttempts;
  private final boolean logMetadata;
  private final boolean logDataRecord;
  private final boolean logRmdRecord;
  private final boolean logTsRecord;

  private final ChunkKeyValueTransformer chunkKeyValueTransformer;
  private final AvroSpecificDeserializer<ChunkedKeySuffix> chunkedKeySuffixDeserializer;
  private final ChunkedValueManifestSerializer manifestSerializer;

  // helper objects for saving records to a file
  private DataFileWriter<GenericRecord> dataFileWriter;
  private GenericDatumReader<Object> keyReader;
  private GenericDatumReader<Object>[] valueReaders;
  private DecoderFactory decoderFactory = new DecoderFactory();
  private Schema outputSchema;
  private PubSubTopicPartition topicPartition;
  private PubSubPositionDeserializer pubSubPositionDeserializer;

  public KafkaTopicDumper(
      ControllerClient controllerClient,
      PubSubConsumerAdapter consumer,
      PubSubTopicPartition topicPartition,
      String parentDir,
      int maxConsumeAttempts,
      boolean logMetadata,
      boolean logDataRecord,
      boolean logRmdRecord,
      boolean logTsRecord,
      PubSubPositionDeserializer pubSubPositionDeserializer) {
    this.topicPartition = topicPartition;
    this.consumer = consumer;
    this.maxConsumeAttempts = maxConsumeAttempts;
    this.pubSubPositionDeserializer = pubSubPositionDeserializer;

    String storeName = Version.parseStoreFromKafkaTopicName(topicPartition.getTopicName());
    StoreResponse storeResponse = controllerClient.getStore(storeName);
    if (storeResponse.isError()) {
      throw new VeniceException(
          "Failed to get store info for store: " + storeName + " with error: " + storeResponse.getError());
    }

    StoreInfo storeInfo = storeResponse.getStore();
    this.compressorFactory = new CompressorFactory();
    if (Version.isATopicThatIsVersioned(topicPartition.getTopicName())) {
      int version = Version.parseVersionFromKafkaTopicName(topicPartition.getTopicName());
      Optional<Version> optionalVersion = storeInfo.getVersion(version);
      if (!optionalVersion.isPresent()) {
        throw new VeniceException("Version: " + version + " does not exist for store: " + storeName);
      }
      this.isChunkingEnabled = optionalVersion.get().isChunkingEnabled();
      CompressionStrategy compressionStrategy = optionalVersion.get().getCompressionStrategy();
      if (compressionStrategy == CompressionStrategy.ZSTD_WITH_DICT) {
        ByteBuffer dictionary =
            DictionaryUtils.readDictionaryFromKafka(topicPartition.getTopicName(), consumer, TOPIC_REPOSITORY);
        compressor = compressorFactory
            .createCompressorWithDictionary(ByteUtils.extractByteArray(dictionary), Zstd.maxCompressionLevel());
      } else {
        compressor = compressorFactory.getCompressor(compressionStrategy);
      }
    } else {
      this.isChunkingEnabled = false;
      this.compressor = compressorFactory.getCompressor(CompressionStrategy.NO_OP);
    }
    this.keySchemaStr = controllerClient.getKeySchema(storeName).getSchemaStr();
    this.keySchema = AvroCompatibilityHelper.parse(keySchemaStr);
    this.keyReader = new GenericDatumReader<>(keySchema, keySchema);

    if (isChunkingEnabled) {
      chunkKeyValueTransformer = new ChunkKeyValueTransformerImpl(keySchema);
      chunkedKeySuffixDeserializer = new AvroSpecificDeserializer<>(new SpecificDatumReader<>(ChunkedKeySuffix.class));
      manifestSerializer = new ChunkedValueManifestSerializer(true);
    } else {
      chunkKeyValueTransformer = null;
      chunkedKeySuffixDeserializer = null;
      manifestSerializer = null;
    }
    this.parentDirectory = parentDir;
    this.logMetadata = logMetadata;
    this.logDataRecord = logDataRecord;
    this.logRmdRecord = logRmdRecord;
    this.logTsRecord = logTsRecord;

    if (logMetadata && !logDataRecord) {
      this.latestValueSchemaStr = null;
      this.allValueSchemas = null;
    } else if (topicPartition.getTopicName().contains(ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX)) {
      // For now dump data record to console mode does not support change capture topic.
      this.latestValueSchemaStr = RecordChangeEvent.getClassSchema().toString();
      this.allValueSchemas = new Schema[1];
      this.allValueSchemas[0] = RecordChangeEvent.getClassSchema();
    } else {
      MultiSchemaResponse.Schema[] schemas = controllerClient.getAllValueSchema(storeName).getSchemas();
      LOGGER.info("Found {} value schemas for store {}", schemas.length, storeName);
      this.latestValueSchemaStr = schemas[schemas.length - 1].getSchemaStr();
      this.allValueSchemas = new Schema[schemas.length];
      int i = 0;
      for (MultiSchemaResponse.Schema valueSchema: schemas) {
        this.allValueSchemas[i] = AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation(valueSchema.getSchemaStr());
        i++;
        this.schemaDataMap.put(valueSchema.getId(), new ValueAndDerivedSchemaData(valueSchema.getSchemaStr()));
      }
      if (storeInfo.isWriteComputationEnabled()) {
        for (MultiSchemaResponse.Schema schema: controllerClient.getAllValueAndDerivedSchema(storeName).getSchemas()) {
          if (!schema.isDerivedSchema()) {
            continue;
          }
          int valueSchemaId = schema.getId();
          int protocolId = schema.getDerivedSchemaId();
          this.schemaDataMap.get(valueSchemaId).setUpdateSchema(protocolId, schema.getSchemaStr());
        }
      }

      if (storeInfo.isActiveActiveReplicationEnabled()) {
        for (MultiSchemaResponse.Schema schema: controllerClient.getAllReplicationMetadataSchemas(storeName)
            .getSchemas()) {
          /**
           * This is intended, as {@link com.linkedin.venice.controller.server.SchemaRoutes} implementation is wrong
           * for RMD schema entry.
           */
          int valueSchemaId = schema.getRmdValueSchemaId();
          int protocolId = schema.getId();
          this.schemaDataMap.get(valueSchemaId).setRmdSchema(protocolId, schema.getSchemaStr());
        }
      }
    }
    if (!(logMetadata || logDataRecord)) {
      setupDumpFile();
    }
  }

  // Constructor for testing purpose only
  KafkaTopicDumper(PubSubConsumerAdapter consumer, PubSubTopicPartition topicPartition, int maxConsumeAttempts) {
    this.topicPartition = topicPartition;
    this.consumer = consumer;
    this.maxConsumeAttempts = maxConsumeAttempts;
    this.keySchemaStr = null;
    this.keySchema = null;
    this.keyReader = null;
    this.latestValueSchemaStr = null;
    this.allValueSchemas = null;
    this.parentDirectory = null;
    this.logMetadata = false;
    this.logDataRecord = false;
    this.logRmdRecord = false;
    this.logTsRecord = false;
    this.isChunkingEnabled = false;
    this.compressorFactory = new CompressorFactory();
    this.compressor = compressorFactory.getCompressor(CompressionStrategy.NO_OP);
    this.chunkKeyValueTransformer = null;
    this.chunkedKeySuffixDeserializer = null;
    this.manifestSerializer = null;
  }

  /**
   * Calculates the starting offset for consuming messages from a PubSub topic partition.
   *
   * <p>This method determines the appropriate starting offset based on the provided starting
   * offset or timestamp. If a specific {@code startingTimestamp} is provided, it attempts to find
   * the offset corresponding to that timestamp. If no offset is found for the timestamp, it throws
   * a {@link PubSubClientException}. The calculated starting offset will always be greater than
   * or equal to the partition's beginning offset to ensure it is within the valid range.
   *
   * @param consumer the {@link PubSubConsumerAdapter} used to fetch position
   * @param partition the {@link PubSubTopicPartition} identifying the topic and partition
   * @param startingPosition the default starting position to use if no timestamp is provided
   * @param startingTimestamp the starting timestamp (epoch in milliseconds) to locate an position.
   *                          If set to -1, the method uses the provided {@code startingPosition}.
   * @return the calculated starting position for the consumption
   * @throws PubSubClientException if no position is found for the provided timestamp
   */
  static PubSubPosition calculateStartingPosition(
      PubSubConsumerAdapter consumer,
      PubSubTopicPartition partition,
      PubSubPosition startingPosition,
      long startingTimestamp) {
    if (startingTimestamp != -1) {
      LOGGER.info("Searching for position for timestamp: {} in topic-partition: {}", partition, startingTimestamp);
      PubSubPosition position = consumer.getPositionByTimestamp(partition, startingTimestamp);
      if (position == null) {
        LOGGER.error(
            "No position found for the requested timestamp: {} in topic-partition: {}. "
                + "This indicates that there are no messages in the topic-partition with a timestamp "
                + "greater than or equal to the provided timestamp. Verify if the topic-partition has data "
                + "in the specified time range.",
            startingTimestamp,
            partition);
        throw new PubSubClientException(
            "Failed to find an position for the requested timestamp: " + startingTimestamp + " in topic-partition: "
                + partition + ". Ensure that messages exist in the specified time range.");
      }
      LOGGER
          .info("Found position: {} for timestamp: {} in topic-partition: {}", position, startingTimestamp, partition);
      startingPosition = position;
    }
    PubSubPosition beginningPosition = consumer.beginningPosition(partition);
    long diff = consumer.positionDifference(partition, startingPosition, beginningPosition);
    if (diff <= 0) {
      LOGGER.info(
          "The calculated starting position: {} is earlier than the beginning position: {}. "
              + "Adjusting to use the beginning position.",
          startingPosition,
          beginningPosition);
      return beginningPosition;
    }
    return startingPosition;
  }

  /**
   * Calculates the ending position for consuming messages from a PubSub topic partition.
   *
   * <p>This method determines the appropriate ending position based on the provided timestamp.
   * If the {@code endTimestamp} is -1, it directly returns the end position of the partition.
   * If a specific {@code endTimestamp} is provided, it attempts to find the position corresponding
   * to the timestamp. If no position is found for the given timestamp, it falls back to the partition's
   * end position and logs a warning.
   *
   * @param consumer the {@link PubSubConsumerAdapter} used to fetch position
   * @param partition the {@link PubSubTopicPartition} identifying the topic and partition
   * @param endTimestamp the ending timestamp (epoch in milliseconds). If set to -1, the method uses the end position.
   * @return the calculated ending position for the consumption
   */
  static PubSubPosition calculateEndingPosition(
      PubSubConsumerAdapter consumer,
      PubSubTopicPartition partition,
      long endTimestamp) {
    PubSubPosition endPosition = consumer.endPosition(partition);
    if (endTimestamp == -1) {
      return endPosition;
    }
    PubSubPosition positionForTime = consumer.getPositionByTimestamp(partition, endTimestamp);
    if (positionForTime != null) {
      return positionForTime;
    }
    // if getPositionByTimestamp returns null, it means there is no message with timestamp >= endTimestamp;
    // In this case we will use the endPosition
    LOGGER.warn(
        "No position found for the requested timestamp: {} in topic-partition: {}. "
            + "This indicates that there are no messages in the topic-partition with a timestamp "
            + "greater than or equal to the provided timestamp. Returning the end position: {}",
        endTimestamp,
        partition,
        endPosition);
    return endPosition;
  }

  /**
   * Fetches and processes messages from a given PubSub topic partition between specified positions.
   *
   * <p>The method polls messages from the specified `startPosition` to `endPosition` and processes up to
   * `messageCount` messages. It uses the consumer to fetch records in batches, processes them via
   * {@code processRecord}, and stops under the following conditions:
   * <ul>
   *   <li>The number of processed messages reaches {@code messageCount}.</li>
   *   <li>The position of the last processed message is greater than or equal to {@code endPosition}.</li>
   *   <li>No new records are fetched within the allowed number of attempts.</li>
   * </ul>
   *
   * @param startPosition the starting position (inclusive) to begin processing messages
   * @param endPosition the ending position (exclusive) to stop processing messages
   * @param messageCount the maximum number of messages to process
   * @return the total number of messages processed
   * @throws IllegalArgumentException if {@code messageCount} is less than or equal to zero, or if
   *                                  {@code startPosition} is greater than {@code endPosition}
   */
  public int fetchAndProcess(PubSubPosition startPosition, PubSubPosition endPosition, long messageCount) {
    if (messageCount <= 0) {
      throw new IllegalArgumentException("Invalid message count: " + messageCount);
    }
    if (consumer.positionDifference(topicPartition, startPosition, endPosition) >= 0) {
      throw new IllegalArgumentException(
          "Start position: " + startPosition + " is greater than or equal to end position: " + endPosition);
    }

    int remainingAttempts = maxConsumeAttempts;
    int processedMessageCount = 0;
    int lastReportedCount = 0;

    consumer.subscribe(topicPartition, startPosition, true);

    try {
      DefaultPubSubMessage lastProcessedRecord = null;
      while (remainingAttempts > 0 && processedMessageCount < messageCount) {
        // Poll for records
        Map<PubSubTopicPartition, List<DefaultPubSubMessage>> records = consumer.poll(5000); // Poll for up to 5 seconds
        Iterator<DefaultPubSubMessage> recordIterator = Utils.iterateOnMapOfLists(records);
        boolean hasProcessedRecords = false;
        while (recordIterator.hasNext() && processedMessageCount < messageCount) {
          DefaultPubSubMessage record = recordIterator.next();
          // Only process and count the record if we haven't reached the end
          processedMessageCount++;
          lastProcessedRecord = record;
          hasProcessedRecords = true;
          processRecord(record);
          // Check if we've reached endPosition BEFORE processing the record
          // endPosition is exclusive, so stop when record.getPosition() >= endPosition
          long positionDelta = consumer.positionDifference(topicPartition, endPosition, record.getPosition());
          if (positionDelta <= 1) {
            LOGGER.info(
                "Reached endPosition: {}. Total messages processed: {}. Position delta: {}",
                endPosition,
                processedMessageCount,
                positionDelta);
            return processedMessageCount;
          }
        }

        // Log progress if sufficient messages have been processed since the last report
        if (processedMessageCount - lastReportedCount >= 1000) {
          LOGGER.info(
              "Consumed {} messages; last consumed message position: {}",
              processedMessageCount,
              lastProcessedRecord.getPosition());
          lastReportedCount = processedMessageCount;
        }

        // Adjust remaining attempts if no records were processed
        remainingAttempts = hasProcessedRecords ? maxConsumeAttempts : remainingAttempts - 1;
      }

      // Return the total number of processed messages
      return processedMessageCount;

    } finally {
      // Ensure unsubscription to free resources
      consumer.unSubscribe(topicPartition);
    }
  }

  private void setupDumpFile() {
    // build file
    File dataFile = new File(
        this.parentDirectory + this.topicPartition.getTopicName() + "_" + this.topicPartition.getPartitionNumber()
            + ".avro");
    List<Schema.Field> outputSchemaFields = new ArrayList<>();
    for (Schema.Field field: VeniceKafkaDecodedRecord.SCHEMA$.getFields()) {
      if (field.name().equals(VENICE_ETL_KEY_FIELD)) {
        outputSchemaFields.add(
            AvroCompatibilityHelper.newField(field)
                .setSchema(AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation(this.keySchemaStr))
                .build());
      } else if (field.name().equals(VENICE_ETL_VALUE_FIELD)) {
        outputSchemaFields.add(
            AvroCompatibilityHelper.newField(field)
                .setSchema(
                    Schema.createUnion(
                        Arrays.asList(
                            Schema.create(Schema.Type.NULL),
                            AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation(this.latestValueSchemaStr))))
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

    int valueSchemaNum = allValueSchemas.length;
    valueReaders = new GenericDatumReader[valueSchemaNum];
    for (int schemaId = 0; schemaId < valueSchemaNum; schemaId++) {
      valueReaders[schemaId] = new GenericDatumReader<>(allValueSchemas[schemaId], allValueSchemas[valueSchemaNum - 1]);
    }
  }

  /**
   * Log the metadata for each kafka message.
   */
  private void logRecordMetadata(DefaultPubSubMessage record) {
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
          "Position:{}; {}; {}; ProducerMd=(guid:{},seg:{},seq:{},mts:{},lts:{}); LeaderMd=(host:{},uo:{},ukcId:{}){}",
          record.getPosition(),
          kafkaKey.isControlMessage() ? CONTROL_REC : REGULAR_REC,
          msgType,
          GuidUtils.getHexFromGuid(producerMetadata.producerGUID),
          producerMetadata.segmentNumber,
          producerMetadata.messageSequenceNumber,
          producerMetadata.messageTimestamp,
          producerMetadata.logicalTimestamp,
          leaderMetadata == null ? "-" : leaderMetadata.hostName,
          leaderMetadata == null
              ? "-"
              : PubSubUtil.deserializePositionWithOffsetFallback(
                  leaderMetadata.upstreamPubSubPosition,
                  leaderMetadata.upstreamOffset,
                  pubSubPositionDeserializer),
          leaderMetadata == null ? "-" : leaderMetadata.upstreamKafkaClusterId,
          chunkMetadata);
    } catch (Exception e) {
      LOGGER.error("Encounter exception when processing record for position {}", record.getPosition(), e);
    }
  }

  void logDataRecord(DefaultPubSubMessage record, boolean logRecordMetadata, boolean logReplicationMetadata) {
    KafkaKey kafkaKey = record.getKey();
    if (kafkaKey.isControlMessage()) {
      return;
    }
    KafkaMessageEnvelope kafkaMessageEnvelope = record.getValue();
    MessageType msgType = MessageType.valueOf(kafkaMessageEnvelope);
    LOGGER.info(
        "[Record Data] Position:{}; {}; {}",
        record.getPosition(),
        msgType.toString(),
        buildDataRecordLog(record, logReplicationMetadata));

    // Potentially print the record metadata for data record.
    if (logRecordMetadata) {
      logRecordMetadata(record);
    }
  }

  String buildDataRecordLog(DefaultPubSubMessage record, boolean logReplicationMetadata) {
    KafkaKey kafkaKey = record.getKey();
    KafkaMessageEnvelope kafkaMessageEnvelope = record.getValue();
    Object keyRecord = null;
    Object valueRecord = null;
    Object rmdRecord = null;
    String valuePayloadSchemaId = "";
    try {
      byte[] keyBytes = kafkaKey.getKey();
      Decoder keyDecoder = decoderFactory.binaryDecoder(keyBytes, null);
      keyRecord = keyReader.read(null, keyDecoder);
      switch (MessageType.valueOf(kafkaMessageEnvelope)) {
        case PUT:
          Put put = (Put) kafkaMessageEnvelope.payloadUnion;
          Decoder valueDecoder =
              decoderFactory.binaryDecoder(ByteUtils.extractByteArray(compressor.decompress(put.putValue)), null);
          valueRecord = schemaDataMap.get(put.schemaId).getValueRecordReader().read(null, valueDecoder);
          valuePayloadSchemaId = String.valueOf(put.schemaId);
          if (logReplicationMetadata && put.replicationMetadataPayload != null
              && put.replicationMetadataPayload.remaining() > 0) {
            Decoder rmdDecoder =
                decoderFactory.binaryDecoder(ByteUtils.extractByteArray(put.replicationMetadataPayload), null);
            LOGGER.info(
                "{} {} {} {}",
                schemaDataMap.get(put.schemaId).getRmdRecordReader(put.replicationMetadataVersionId),
                schemaDataMap.get(put.schemaId),
                put.schemaId,
                put.replicationMetadataVersionId);
            rmdRecord = schemaDataMap.get(put.schemaId)
                .getRmdRecordReader(put.replicationMetadataVersionId)
                .read(null, rmdDecoder);
          }
          break;
        case DELETE:
          Delete delete = (Delete) kafkaMessageEnvelope.payloadUnion;
          valuePayloadSchemaId = String.valueOf(delete.schemaId);
          if (logReplicationMetadata && delete.replicationMetadataPayload != null
              && delete.replicationMetadataPayload.remaining() > 0) {
            Decoder rmdDecoder =
                decoderFactory.binaryDecoder(ByteUtils.extractByteArray(delete.replicationMetadataPayload), null);
            rmdRecord = schemaDataMap.get(delete.schemaId)
                .getRmdRecordReader(delete.replicationMetadataVersionId)
                .read(null, rmdDecoder);
          }
          break;
        case UPDATE:
          Update update = (Update) kafkaMessageEnvelope.payloadUnion;
          valuePayloadSchemaId = String.format("%d-%d", update.schemaId, update.updateSchemaId);
          Decoder updateDecoder = decoderFactory.binaryDecoder(ByteUtils.extractByteArray(update.updateValue), null);
          valueRecord =
              schemaDataMap.get(update.schemaId).getUpdateRecordReader(update.updateSchemaId).read(null, updateDecoder);
          break;
        default:
          throw new VeniceException("Unknown data type.");
      }
    } catch (Exception e) {
      LOGGER.error("Encounter exception when processing record for position: {}", record.getPosition(), e);
    }
    return logReplicationMetadata
        ? String
            .format("Key: %s; Value: %s; Schema: %s; RMD: %s", keyRecord, valueRecord, valuePayloadSchemaId, rmdRecord)
        : String.format("Key: %s; Value: %s; Schema: %s", keyRecord, valueRecord, valuePayloadSchemaId);
  }

  void processRecord(DefaultPubSubMessage record) {
    if (logTsRecord) {
      logIfTopicSwitchMessage(record, pubSubPositionDeserializer);
    } else if (logDataRecord) {
      logDataRecord(record, logMetadata, logRmdRecord);
    } else if (logMetadata) {
      logRecordMetadata(record);
    } else {
      // If no console logging is enabled, we will save data records into local file.
      writeToFile(record);
    }
  }

  static void logIfTopicSwitchMessage(
      DefaultPubSubMessage record,
      PubSubPositionDeserializer pubSubPositionDeserializer) {
    KafkaKey kafkaKey = record.getKey();
    if (!kafkaKey.isControlMessage()) {
      // TS message is a control message, so we only care about control messages.
      return;
    }

    ControlMessage controlMessage = (ControlMessage) record.getValue().payloadUnion;
    if (controlMessage.controlMessageType != ControlMessageType.TOPIC_SWITCH.getValue()) {
      return;
    }

    String logMessage = constructTopicSwitchLog(record, pubSubPositionDeserializer);
    LOGGER.info(logMessage);
  }

  /**
   * Constructs the log message for a TopicSwitch message.
   *
   * @param record The PubSubMessage containing the TopicSwitch message.
   * @return A formatted string representing the log message.
   */
  static String constructTopicSwitchLog(
      DefaultPubSubMessage record,
      PubSubPositionDeserializer pubSubPositionDeserializer) {
    KafkaMessageEnvelope kafkaMessageEnvelope = record.getValue();
    ProducerMetadata producerMetadata = kafkaMessageEnvelope.producerMetadata;
    LeaderMetadata leaderMetadata = kafkaMessageEnvelope.leaderMetadataFooter;
    ControlMessage controlMessage = (ControlMessage) kafkaMessageEnvelope.payloadUnion;
    TopicSwitch topicSwitch = (TopicSwitch) controlMessage.controlMessageUnion;

    return String.format(
        "Position:%s; %s; SourceKafkaServers: %s; SourceTopicName: %s; RewindStartTimestamp: %s; "
            + "ProducerMd=(guid:%s,seg:%s,seq:%s,mts:%s,lts:%s); LeaderMd=(host:%s,uo:%s,ukcId:%s)",
        record.getPosition(),
        ControlMessageType.TOPIC_SWITCH.name(),
        topicSwitch.sourceKafkaServers,
        topicSwitch.sourceTopicName,
        topicSwitch.rewindStartTimestamp,
        GuidUtils.getHexFromGuid(producerMetadata.producerGUID),
        producerMetadata.segmentNumber,
        producerMetadata.messageSequenceNumber,
        producerMetadata.messageTimestamp,
        producerMetadata.logicalTimestamp,
        leaderMetadata == null ? "-" : leaderMetadata.hostName,
        leaderMetadata == null
            ? "-"
            : PubSubUtil.deserializePositionWithOffsetFallback(
                leaderMetadata.upstreamPubSubPosition,
                leaderMetadata.upstreamOffset,
                pubSubPositionDeserializer),
        leaderMetadata == null ? "-" : leaderMetadata.upstreamKafkaClusterId);
  }

  private void writeToFile(DefaultPubSubMessage record) {
    try {
      KafkaKey kafkaKey = record.getKey();
      KafkaMessageEnvelope kafkaMessageEnvelope = record.getValue();
      if (kafkaKey.isControlMessage()) {
        LOGGER.info("Found a control message, continue");
        return;
      }
      // build the record
      GenericRecord convertedRecord = new GenericData.Record(outputSchema);
      convertedRecord.put(VENICE_ETL_POSITION_FIELD, record.getPosition().toString());

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
          convertedRecord.put(VENICE_ETL_DELETED_TS_FIELD, record.getPosition().getNumericOffset());
          break;
        case UPDATE:
          LOGGER.info("Found update message! continue");
          break;
        default:
          throw new VeniceException("How come?");
      }
      dataFileWriter.append(convertedRecord);
    } catch (Exception e) {
      LOGGER.error("Failed when building record for position {}", record.getPosition(), e);
    }
  }

  // Visible for testing
  String getChunkMetadataLog(DefaultPubSubMessage record) throws IOException {
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
          schemaId = ((Delete) kafkaMessageEnvelope.payloadUnion).schemaId;
          break;
        default:
          throw new IOException(
              "Unexpected '" + messageType + "' message from Topic: " + record.getTopicName() + " Partition: "
                  + record.getPartition());
      }

      final ChunkKeyValueTransformer.KeyType keyType = ChunkKeyValueTransformer.getKeyType(messageType, schemaId);
      switch (keyType) {
        case WITH_FULL_VALUE:
          return String.format(" ChunkMd=(type:%s)", keyType);
        case WITH_VALUE_CHUNK:
          final RawKeyBytesAndChunkedKeySuffix rawKeyBytesAndChunkedKeySuffix =
              chunkKeyValueTransformer.splitChunkedKey(kafkaKey.getKey(), keyType);
          final ByteBuffer chunkedKeySuffixBytes = rawKeyBytesAndChunkedKeySuffix.getChunkedKeySuffixBytes();
          final ChunkedKeySuffix chunkedKeySuffix = chunkedKeySuffixDeserializer.deserialize(chunkedKeySuffixBytes);
          return String.format(
              " ChunkMd=(type:%s, ChunkIndex: %d, FirstChunkMd=(guid:%s,seg:%d,seq:%d))",
              keyType,
              chunkedKeySuffix.chunkId.chunkIndex,
              GuidUtils.getHexFromGuid(chunkedKeySuffix.chunkId.producerGUID),
              chunkedKeySuffix.chunkId.segmentNumber,
              chunkedKeySuffix.chunkId.messageSequenceNumber);
        case WITH_CHUNK_MANIFEST:
          Put putMessage = (Put) kafkaMessageEnvelope.payloadUnion;
          ChunkedValueManifest chunkedValueManifest =
              manifestSerializer.deserialize(ByteUtils.extractByteArray(putMessage.putValue), putMessage.schemaId);

          ByteBuffer firstChunkKeyWithChunkIdSuffix = chunkedValueManifest.keysWithChunkIdSuffix.get(0);
          final RawKeyBytesAndChunkedKeySuffix firstChunkRawKeyBytesAndChunkedKeySuffix = chunkKeyValueTransformer
              .splitChunkedKey(ByteUtils.extractByteArray(firstChunkKeyWithChunkIdSuffix), WITH_VALUE_CHUNK);
          final ByteBuffer firstChunkKeySuffixBytes =
              firstChunkRawKeyBytesAndChunkedKeySuffix.getChunkedKeySuffixBytes();
          final ChunkedKeySuffix firstChunkedKeySuffix =
              chunkedKeySuffixDeserializer.deserialize(firstChunkKeySuffixBytes);

          return String.format(
              " ChunkMd=(type:%s, FirstChunkMd=(guid:%s,seg:%d,seq:%d))",
              keyType,
              GuidUtils.getHexFromGuid(firstChunkedKeySuffix.chunkId.producerGUID),
              firstChunkedKeySuffix.chunkId.segmentNumber,
              firstChunkedKeySuffix.chunkId.messageSequenceNumber);
        default:
          throw new VeniceException("Unexpected key type: " + keyType);
      }
    } else {
      return "";
    }
  }

  @Override
  public void close() throws Exception {
    if (dataFileWriter != null) {
      dataFileWriter.close();
    }
    Utils.closeQuietlyWithErrorLogged(compressor);
    Utils.closeQuietlyWithErrorLogged(compressorFactory);
  }
}
