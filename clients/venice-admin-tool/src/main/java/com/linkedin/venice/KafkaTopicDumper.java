package com.linkedin.venice;

import static com.linkedin.venice.chunking.ChunkKeyValueTransformer.KeyType.WITH_VALUE_CHUNK;
import static com.linkedin.venice.pubsub.PubSubConstants.getPubsubOffsetApiTimeoutDurationDefaultValue;

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
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
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
  private static final String VENICE_ETL_KEY_FIELD = "key";
  private static final String VENICE_ETL_VALUE_FIELD = "value";
  private static final String VENICE_ETL_OFFSET_FIELD = "offset";
  private static final String VENICE_ETL_DELETED_TS_FIELD = "DELETED_TS";
  private static final String VENICE_ETL_METADATA_FIELD = "metadata";

  private static final String VENICE_ETL_BROKER_TIMESTAMP_FIELD = "brokerTimestamp";
  private static final String VENICE_ETL_PRODUCER_TIMESTAMP_FIELD = "producerTimestamp";
  private static final String VENICE_ETL_PARTITION_FIELD = "partition";
  private static final String REGULAR_REC = "REG";
  private static final String CONTROL_REC = "CTRL";

  private final String topicName;
  private final int partition;
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
  private final long messageCount;
  private final long endOffset;
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

  public KafkaTopicDumper(
      ControllerClient controllerClient,
      PubSubConsumerAdapter consumer,
      String topic,
      int partitionNumber,
      long startingOffset,
      long startingTimestamp,
      int messageCount,
      String parentDir,
      int maxConsumeAttempts,
      boolean logMetadata,
      boolean logDataRecord,
      boolean logRmdRecord,
      boolean logTsRecord) {
    this.consumer = consumer;
    this.maxConsumeAttempts = maxConsumeAttempts;

    PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

    String storeName = Version.parseStoreFromKafkaTopicName(topic);
    StoreResponse storeResponse = controllerClient.getStore(storeName);
    if (storeResponse.isError()) {
      throw new VeniceException(
          "Failed to get store info for store: " + storeName + " with error: " + storeResponse.getError());
    }

    StoreInfo storeInfo = storeResponse.getStore();
    this.compressorFactory = new CompressorFactory();
    if (Version.isATopicThatIsVersioned(topic)) {
      int version = Version.parseVersionFromKafkaTopicName(topic);
      Optional<Version> optionalVersion = storeInfo.getVersion(version);
      if (!optionalVersion.isPresent()) {
        throw new VeniceException("Version: " + version + " does not exist for store: " + storeName);
      }
      this.isChunkingEnabled = optionalVersion.get().isChunkingEnabled();
      CompressionStrategy compressionStrategy = optionalVersion.get().getCompressionStrategy();
      if (compressionStrategy == CompressionStrategy.ZSTD_WITH_DICT) {
        ByteBuffer dictionary = DictionaryUtils.readDictionaryFromKafka(topic, consumer, pubSubTopicRepository);
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

    this.topicName = topic;
    this.partition = partitionNumber;
    this.parentDirectory = parentDir;
    this.logMetadata = logMetadata;
    this.logDataRecord = logDataRecord;
    this.logRmdRecord = logRmdRecord;
    this.logTsRecord = logTsRecord;

    if (logMetadata && !logDataRecord) {
      this.latestValueSchemaStr = null;
      this.allValueSchemas = null;
    } else if (topicName.contains(ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX)) {
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

    PubSubTopicPartition partition =
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(topicName), partitionNumber);
    this.endOffset = consumer.endOffset(partition);
    LOGGER.info("End offset for partition {} is {}", partition, this.endOffset);
    if (messageCount < 0) {
      this.messageCount = this.endOffset;
    } else {
      this.messageCount = messageCount;
    }
    long computedStartingOffset = getOffsetToConsumerFrom(consumer, partition, startingOffset, startingTimestamp);
    consumer.subscribe(partition, computedStartingOffset - 1);
    if (!(logMetadata || logDataRecord)) {
      setupDumpFile();
    }
  }

  static long getOffsetToConsumerFrom(
      PubSubConsumerAdapter consumer,
      PubSubTopicPartition partition,
      long startingOffset,
      long startingTimestamp) {
    if (startingTimestamp != -1) {
      LOGGER.info(
          "Requested to dump topic-partition: {} with starting timestamp: {}. Will try to find the offset for the timestamp.",
          partition,
          startingTimestamp);
      Long offsetForTime = consumer.offsetForTime(partition, startingTimestamp);
      if (offsetForTime == null) {
        LOGGER.error(
            "No offset found for timestamp: {} in topic-partition: {}. There might be no message in the topic-partition "
                + " with timestamp greater than or equal to the requested timestamp.",
            startingTimestamp,
            partition);
        throw new PubSubClientException(
            "No offset found for timestamp: " + startingTimestamp + " in topic-partition: " + partition);
      }
      LOGGER.info(
          "Found offset: {} for timestamp: {} in topic-partition: {}",
          offsetForTime,
          startingTimestamp,
          partition);
      startingOffset = offsetForTime;
    }

    Long partitionBeginningOffset =
        consumer.beginningOffset(partition, getPubsubOffsetApiTimeoutDurationDefaultValue());
    long computedStartingOffset = Math.max(partitionBeginningOffset, startingOffset);
    LOGGER.info(
        "Consumer will start consuming from offset: {} in topic-partition: {}",
        computedStartingOffset,
        partition);
    return computedStartingOffset;
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
          "Offset:{}; {}; {}; ProducerMd=(guid:{},seg:{},seq:{},mts:{},lts:{}); LeaderMd=(host:{},uo:{},ukcId:{}){}",
          record.getOffset(),
          kafkaKey.isControlMessage() ? CONTROL_REC : REGULAR_REC,
          msgType,
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
      LOGGER.error("Encounter exception when processing record for offset {}", record.getOffset(), e);
    }
  }

  void logDataRecord(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record,
      boolean logRecordMetadata,
      boolean logReplicationMetadata) {
    KafkaKey kafkaKey = record.getKey();
    if (kafkaKey.isControlMessage()) {
      return;
    }
    KafkaMessageEnvelope kafkaMessageEnvelope = record.getValue();
    MessageType msgType = MessageType.valueOf(kafkaMessageEnvelope);
    LOGGER.info(
        "[Record Data] Offset:{}; {}; {}",
        record.getOffset(),
        msgType.toString(),
        buildDataRecordLog(record, logReplicationMetadata));

    // Potentially print the record metadata for data record.
    if (logRecordMetadata) {
      logRecordMetadata(record);
    }
  }

  String buildDataRecordLog(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record,
      boolean logReplicationMetadata) {
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
      LOGGER.error("Encounter exception when processing record for offset: {}", record.getOffset(), e);
    }
    return logReplicationMetadata
        ? String
            .format("Key: %s; Value: %s; Schema: %s; RMD: %s", keyRecord, valueRecord, valuePayloadSchemaId, rmdRecord)
        : String.format("Key: %s; Value: %s; Schema: %s", keyRecord, valueRecord, valuePayloadSchemaId);
  }

  private void processRecord(PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record) {
    if (logTsRecord) {
      logIfTopicSwitchMessage(record);
    } else if (logDataRecord) {
      logDataRecord(record, logMetadata, logRmdRecord);
    } else if (logMetadata) {
      logRecordMetadata(record);
    } else {
      // If no console logging is enabled, we will save data records into local file.
      writeToFile(record);
    }
  }

  static void logIfTopicSwitchMessage(PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record) {
    KafkaKey kafkaKey = record.getKey();
    if (!kafkaKey.isControlMessage()) {
      // TS message is a control message, so we only care about control messages.
      return;
    }

    ControlMessage controlMessage = (ControlMessage) record.getValue().payloadUnion;
    if (controlMessage.controlMessageType != ControlMessageType.TOPIC_SWITCH.getValue()) {
      return;
    }

    String logMessage = constructTopicSwitchLog(record);
    LOGGER.info(logMessage);
  }

  /**
   * Constructs the log message for a TopicSwitch message.
   *
   * @param record The PubSubMessage containing the TopicSwitch message.
   * @return A formatted string representing the log message.
   */
  static String constructTopicSwitchLog(PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record) {
    KafkaMessageEnvelope kafkaMessageEnvelope = record.getValue();
    ProducerMetadata producerMetadata = kafkaMessageEnvelope.producerMetadata;
    LeaderMetadata leaderMetadata = kafkaMessageEnvelope.leaderMetadataFooter;
    ControlMessage controlMessage = (ControlMessage) kafkaMessageEnvelope.payloadUnion;
    TopicSwitch topicSwitch = (TopicSwitch) controlMessage.controlMessageUnion;

    return String.format(
        "Offset:%s; %s; SourceKafkaServers: %s; SourceTopicName: %s; RewindStartTimestamp: %s; "
            + "ProducerMd=(guid:%s,seg:%s,seq:%s,mts:%s,lts:%s); LeaderMd=(host:%s,uo:%s,ukcId:%s)",
        record.getOffset(),
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
        leaderMetadata == null ? "-" : leaderMetadata.upstreamOffset,
        leaderMetadata == null ? "-" : leaderMetadata.upstreamKafkaClusterId);
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
