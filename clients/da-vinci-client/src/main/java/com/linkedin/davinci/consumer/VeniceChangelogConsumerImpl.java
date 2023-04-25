package com.linkedin.davinci.consumer;

import static com.linkedin.venice.schema.rmd.RmdConstants.*;

import com.linkedin.davinci.repository.ThinClientMetaStoreBasedRepository;
import com.linkedin.davinci.storage.chunking.AbstractAvroChunkingAdapter;
import com.linkedin.davinci.storage.chunking.GenericChunkingAdapter;
import com.linkedin.davinci.storage.chunking.SpecificRecordChunkingAdapter;
import com.linkedin.davinci.store.memory.InMemoryStorageEngine;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.venice.client.change.capture.protocol.RecordChangeEvent;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.NoopCompressor;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.StartOfPush;
import com.linkedin.venice.kafka.protocol.VersionSwap;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.views.ChangeCaptureView;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceChangelogConsumerImpl<K, V> implements VeniceChangelogConsumer<K, V> {
  private static final Logger LOGGER = LogManager.getLogger(VeniceChangelogConsumerImpl.class);
  private final int partitionCount;

  protected static final VeniceCompressor NO_OP_COMPRESSOR = new NoopCompressor();

  protected VeniceCompressor currentCompressor = NO_OP_COMPRESSOR;

  protected final CompressorFactory compressorFactory = new CompressorFactory();

  protected ThinClientMetaStoreBasedRepository readOnlySchemaRepository;

  protected final ReadOnlySchemaRepository recordChangeEventSchemaRepository;

  protected final AbstractAvroChunkingAdapter<RecordChangeEvent> recordChangeEventChunkingAdapter =
      new SpecificRecordChunkingAdapter<>(RecordChangeEvent.class);

  protected final AbstractAvroChunkingAdapter<V> userEventChunkingAdapter;

  protected final SchemaReader schemaReader;
  private final String viewClassName;
  private final Set<Integer> subscribedPartitions;
  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  protected final RecordDeserializer<K> keyDeserializer;
  private final D2ControllerClient d2ControllerClient;
  private final RecordDeserializer<RecordChangeEvent> recordChangeDeserializer =
      FastSerializerDeserializerFactory.getFastAvroSpecificDeserializer(
          AvroProtocolDefinition.RECORD_CHANGE_EVENT.getCurrentProtocolVersionSchema(),
          RecordChangeEvent.class);
  protected final ReplicationMetadataSchemaRepository replicationMetadataSchemaRepository;

  protected final String storeName;
  protected final int storeCurrentVersion;

  protected final boolean storeChunkingEnabled;

  // This storage engine serves as a buffer for records which are chunked and have to be buffered before they can
  // be returned to the client. We leverage the storageEngine interface here in order to take better advantage
  // of the chunking and decompressing adapters that we've already built (which today are built around this interface)
  // as chunked records are assembled we will eagerly evict all keys from the storage engine in order to keep the memory
  // footprint as small as we can. We could use the object cache storage engine here in order to get LRU behavior
  // but then that runs the risk of a parallel subscription having record chunks getting evicted before we have a chance
  // to assemble them. So we rely on the simpler and concrete implementation as opposed to the abstraction in order
  // to control and guarantee the behavior we're expecting.
  protected final InMemoryStorageEngine inMemoryStorageEngine;
  protected final Consumer<KafkaKey, KafkaMessageEnvelope> kafkaConsumer;
  protected String currentTopic;
  protected Map<Integer, List<Long>> currentVersionTempHighWatermarks = new HashMap<>();
  protected final Map<Integer, List<Long>> currentVersionHighWatermarks = new HashMap<>();
  protected final int[] currentValuePayloadSize; // This is for recording current value payload.

  public VeniceChangelogConsumerImpl(
      ChangelogClientConfig changelogClientConfig,
      Consumer<KafkaKey, KafkaMessageEnvelope> kafkaConsumer) {
    this.kafkaConsumer = kafkaConsumer;
    this.storeName = changelogClientConfig.getStoreName();
    this.d2ControllerClient = changelogClientConfig.getD2ControllerClient();
    StoreResponse storeResponse = changelogClientConfig.getD2ControllerClient().getStore(storeName);
    if (storeResponse.isError()) {
      throw new VeniceException(
          "Failed to get store info for store: " + storeName + " with error: " + storeResponse.getError());
    }
    StoreInfo store = storeResponse.getStore();
    this.storeCurrentVersion = store.getCurrentVersion();
    this.partitionCount = store.getPartitionCount();
    this.storeChunkingEnabled = store.isChunkingEnabled();
    this.currentValuePayloadSize = new int[partitionCount];
    this.viewClassName = changelogClientConfig.getViewName();
    this.currentTopic = Version.composeKafkaTopic(storeName, storeCurrentVersion);// +
                                                                                  // ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX;
    this.replicationMetadataSchemaRepository = new ReplicationMetadataSchemaRepository(d2ControllerClient);
    this.schemaReader = changelogClientConfig.getSchemaReader();
    this.subscribedPartitions = new HashSet<>();
    Schema keySchema = schemaReader.getKeySchema();
    this.keyDeserializer = FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(keySchema, keySchema);
    // The in memory storage engine only relies on the name of store and nothing else. We use an unversioned store name
    // here in order to reduce confusion (as this storage engine can be used across version topics).
    this.inMemoryStorageEngine = new InMemoryStorageEngine(storeName);
    readOnlySchemaRepository = new ThinClientMetaStoreBasedRepository(
        changelogClientConfig.getInnerClientConfig(),
        new VeniceProperties(),
        null);
    recordChangeEventSchemaRepository = new RecordChangeEventReadOnlySchemaRepository(readOnlySchemaRepository);
    Class<V> valueClass = changelogClientConfig.getInnerClientConfig().getSpecificValueClass();
    if (valueClass != null) {
      // If a value class is supplied, we'll use a Specific record adapter
      userEventChunkingAdapter = new SpecificRecordChunkingAdapter(valueClass);
    } else {
      userEventChunkingAdapter = GenericChunkingAdapter.INSTANCE;
    }
    LOGGER.info(
        "Start a change log consumer client for store: {}, current version: {}, with partition count: {} and view class: {} ",
        storeName,
        storeCurrentVersion,
        partitionCount,
        viewClassName);
  }

  @Override
  public CompletableFuture<Void> subscribe(Set<Integer> partitions) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        readOnlySchemaRepository.start();
        readOnlySchemaRepository.subscribe(storeName);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      readOnlySchemaRepository.refresh();
      Set<TopicPartition> topicPartitionSet = kafkaConsumer.assignment();
      List<TopicPartition> topicPartitionList = getPartitionListToSubscribe(partitions, topicPartitionSet);
      kafkaConsumer.assign(topicPartitionList);
      kafkaConsumer.seekToBeginning(topicPartitionList);
      subscribedPartitions.addAll(partitions);
      return null;
    });
  }

  @Override
  public CompletableFuture<Void> subscribeAll() {
    Set<Integer> allPartitions = new HashSet<>();
    for (int partition = 0; partition < partitionCount; partition++) {
      allPartitions.add(partition);
    }
    return this.subscribe(allPartitions);
  }

  private List<TopicPartition> getPartitionListToSubscribe(
      Set<Integer> partitions,
      Set<TopicPartition> topicPartitionSet) {
    List<TopicPartition> topicPartitionList = new ArrayList<>(topicPartitionSet.size());
    for (Integer partition: partitions) {
      TopicPartition topicPartition = new TopicPartition(currentTopic, partition);
      if (!topicPartitionSet.contains(topicPartition)) {
        topicPartitionList.add(topicPartition);
      }
    }
    return topicPartitionList;
  }

  @Override
  public void unsubscribe(Set<Integer> partitions) {
    for (Integer partition: partitions) {
      TopicPartition topicPartition = new TopicPartition(currentTopic, partition);
      Set<TopicPartition> topicPartitionSet = kafkaConsumer.assignment();
      if (topicPartitionSet.contains(topicPartition)) {
        List<TopicPartition> topicPartitionList = new ArrayList<>(topicPartitionSet);
        if (topicPartitionList.remove(topicPartition)) {
          kafkaConsumer.assign(topicPartitionList);
        }
      }
      subscribedPartitions.remove(topicPartition.partition());
    }
  }

  @Override
  public void unsubscribeAll() {
    Set<Integer> allPartitions = new HashSet<>();
    for (int partition = 0; partition < partitionCount; partition++) {
      allPartitions.add(partition);
    }
    this.unsubscribe(allPartitions);
  }

  @Override
  public Collection<PubSubMessage<K, ChangeEvent<V>, Long>> poll(long timeoutInMs) {
    return internalPoll(timeoutInMs, ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX);
  }

  protected Collection<PubSubMessage<K, ChangeEvent<V>, Long>> internalPoll(long timeoutInMs, String topicSuffix) {
    List<PubSubMessage<K, ChangeEvent<V>, Long>> pubSubMessages = new ArrayList<>();
    ConsumerRecords<KafkaKey, KafkaMessageEnvelope> consumerRecords = kafkaConsumer.poll(timeoutInMs);
    for (ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord: consumerRecords) {
      PubSubTopicPartition pubSubTopicPartition = getPubSubTopicPartitionFromConsumerRecord(consumerRecord);
      if (consumerRecord.key().isControlMessage()) {
        ControlMessage controlMessage = (ControlMessage) consumerRecord.value().payloadUnion;
        if (handleControlMessage(controlMessage, pubSubTopicPartition, topicSuffix)) {
          return pubSubMessages;
        }
      } else {
        Optional<PubSubMessage<K, ChangeEvent<V>, Long>> pubSubMessage =
            convertConsumerRecordToPubSubChangeEventMessage(consumerRecord, pubSubTopicPartition);
        pubSubMessage.ifPresent(pubSubMessages::add);
      }
    }
    return pubSubMessages;
  }

  /**
   * Handle control message from the given topic. Returns true if a topic switch should occur and records should be returned
   *
   * @param controlMessage the encountered control message
   * @param pubSubTopicPartition the topic that this control message was concerned
   * @return true if a version switch happened, false if otherwise
   */
  protected boolean handleControlMessage(
      ControlMessage controlMessage,
      PubSubTopicPartition pubSubTopicPartition,
      String topicSuffix) {
    ControlMessageType controlMessageType = ControlMessageType.valueOf(controlMessage);
    // TODO: Find a better way to avoid data gap between version topic and change capture topic due to log compaction.
    if (controlMessageType.equals(ControlMessageType.START_OF_PUSH)) {
      StartOfPush startOfPush = (StartOfPush) controlMessage.controlMessageUnion;
      byte[] dictionary = null;
      if (startOfPush.compressionDictionary != null) {
        dictionary = startOfPush.compressionDictionary.array();
      }
      // TODO: This relies on consuming the beginning of the version topic. This is what some libraries do anyway under
      // the hood, but it seems clumsy here. Should refactor
      // TODO: This factory maintains a cache of ZSTD compression strategies. We'll need to clean this out as we remove
      // subscriptions. Will add this in the checkpointing feature.
      currentCompressor = compressorFactory.createVersionSpecificCompressorIfNotExist(
          CompressionStrategy.valueOf(startOfPush.compressionStrategy),
          pubSubTopicPartition.getPubSubTopic().getName(),
          dictionary);
    }
    if (controlMessageType.equals(ControlMessageType.END_OF_PUSH)) {
      int partitionId = pubSubTopicPartition.getPartitionNumber();
      LOGGER.info(
          "Obtain End of Push message and current local high watermarks: {}",
          currentVersionTempHighWatermarks.get(partitionId));
      if (currentVersionTempHighWatermarks.containsKey(partitionId)) {
        currentVersionHighWatermarks.put(partitionId, currentVersionTempHighWatermarks.get(partitionId));
      }
      // Jump to next topic
      // TODO: Today we don't publish the version swap message to the version topic. This necessitates relying on the
      // change capture topic in order to navigate version pushes. We should pass the topicSuffix argument here once
      // that
      // support lands.
      switchToNewTopic(currentTopic, ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX);
      return true;
    }
    if (controlMessageType.equals(ControlMessageType.VERSION_SWAP)) {
      // TODO: Today we don't publish the version swap message to the version topic. This necessitates relying on the
      // change capture topic in order to navigate version pushes. We should pass the topicSuffix argument here once
      // that
      // support lands.
      return handleVersionSwapControlMessage(
          controlMessage,
          pubSubTopicPartition,
          ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX);
    }
    return false;
  }

  protected <T> T bufferAndAssembleRecordChangeEvent(
      PubSubTopicPartition pubSubTopicPartition,
      int schemaId,
      byte[] keyBytes,
      ByteBuffer valueBytes,
      long recordOffset,
      AbstractAvroChunkingAdapter<T> chunkingAdapter,
      Lazy<RecordDeserializer<T>> recordDeserializer,
      int readerSchemaId,
      ReadOnlySchemaRepository schemaRepository) {
    T assembledRecord = null;
    // Select compressor. We'll only construct compressors for version topics so this will return null for
    // events from change capture. This is fine as today they are not compressed.
    VeniceCompressor compressor;
    if (pubSubTopicPartition.getPubSubTopic().isVersionTopic()) {
      compressor = currentCompressor;
    } else {
      compressor = NO_OP_COMPRESSOR;
    }

    // If chunking is enabled prepare an in memory storage engine for buffering record chunks
    if (storeChunkingEnabled) {
      if (!inMemoryStorageEngine.containsPartition(pubSubTopicPartition.getPartitionNumber())) {
        inMemoryStorageEngine.addStoragePartition(pubSubTopicPartition.getPartitionNumber());
      }
      // If this is a record chunk, store the chunk and return null for processing this record
      if (schemaId == AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion()) {
        inMemoryStorageEngine.put(
            pubSubTopicPartition.getPartitionNumber(),
            keyBytes,
            ValueRecord.create(schemaId, valueBytes.array()).serialize());
        return null;
      } else if (schemaId == AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion()) {
        // This is the last value. Store it, and now read it from the in memory store as a fully assembled value
        inMemoryStorageEngine.put(
            pubSubTopicPartition.getPartitionNumber(),
            keyBytes,
            ValueRecord.create(schemaId, valueBytes.array()).serialize());
        try {
          assembledRecord = chunkingAdapter.get(
              inMemoryStorageEngine,
              readerSchemaId,
              pubSubTopicPartition.getPartitionNumber(),
              ByteBuffer.wrap(keyBytes),
              false,
              null,
              null,
              null,
              compressor.getCompressionStrategy(),
              true,
              schemaRepository,
              storeName,
              compressor);
        } catch (Exception ex) {
          // We might get an exception if we haven't persisted all the chunks for a given key. This
          // can actually happen if the client seeks to the middle of a chunked record either by
          // only tailing the records or through direct offset management. This is ok, we just won't
          // return this record since this is a course grained approach we can drop it.
          LOGGER.warn(
              "Encountered error assembling chunked record, this can happen when seeking between chunked records. Skipping offset {} on topic {}",
              recordOffset,
              pubSubTopicPartition.getPubSubTopic().getName());
        }
      } else {
        // this is a fully specified record, no need to buffer and assemble it, just decompress and deserialize it
        try {
          assembledRecord = recordDeserializer.get().deserialize(compressor.decompress(valueBytes));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      // We only buffer one record at a time for a given partition. If we've made it this far
      // we either just finished assembling a large record, or, didn't specify anything. So we'll clear
      // the cache. Kafka might give duplicate delivery but it won't give out of order delivery, so
      // this is safe to do in all such contexts.
      inMemoryStorageEngine.dropPartition(pubSubTopicPartition.getPartitionNumber());
    } else {
      try {
        assembledRecord = recordDeserializer.get().deserialize(compressor.decompress(valueBytes));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return assembledRecord;
  }

  protected Optional<PubSubMessage<K, ChangeEvent<V>, Long>> convertConsumerRecordToPubSubChangeEventMessage(
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord,
      PubSubTopicPartition pubSubTopicPartition) {
    Optional<PubSubMessage<K, ChangeEvent<V>, Long>> pubSubMessage = Optional.empty();
    byte[] keyBytes = consumerRecord.key().getKey();
    MessageType messageType = MessageType.valueOf(consumerRecord.value());
    RecordChangeEvent recordChangeEvent;
    // Internal store ingestion tasks only persist PUT messages to either VT or view topics
    if (messageType.equals(MessageType.PUT)) {
      Put put = (Put) consumerRecord.value().payloadUnion;
      // Select appropriate deserializers
      Lazy deserializerProvider;
      Object assembledObject = null;
      AbstractAvroChunkingAdapter chunkingAdapter;
      int readerSchemaId;
      ReadOnlySchemaRepository schemaRepo;
      if (pubSubTopicPartition.getPubSubTopic().isVersionTopic()) {
        Schema valueSchema = schemaReader.getValueSchema(put.schemaId);
        deserializerProvider =
            Lazy.of(() -> FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(valueSchema, valueSchema));
        chunkingAdapter = userEventChunkingAdapter;
        readerSchemaId = AvroProtocolDefinition.RECORD_CHANGE_EVENT.getCurrentProtocolVersion();
        schemaRepo = readOnlySchemaRepository;
      } else {
        deserializerProvider = Lazy.of(() -> recordChangeDeserializer);
        chunkingAdapter = recordChangeEventChunkingAdapter;
        readerSchemaId = this.schemaReader.getLatestValueSchemaId();
        schemaRepo = recordChangeEventSchemaRepository;
      }
      assembledObject = bufferAndAssembleRecordChangeEvent(
          pubSubTopicPartition,
          put.getSchemaId(),
          keyBytes,
          put.getPutValue(),
          consumerRecord.offset(),
          chunkingAdapter,
          deserializerProvider,
          readerSchemaId,
          schemaRepo);
      if (assembledObject == null) {
        // bufferAndAssembleRecordChangeEvent may have only buffered records and not returned anything yet because
        // it's waiting for more input. In this case, just return an empty optional for now.
        return Optional.empty();
      }

      // Now that we've assembled the object, we need to extract the replication vector depending on if it's from VT
      // or from the record change event. Records from VT 'typically' don't have an offset vector, but they will in
      // repush scenarios (which we want to be opaque to the user and filter accordingly).
      List<Long> replicationCheckpoint;
      int payloadSize = consumerRecord.serializedKeySize() + consumerRecord.serializedValueSize();
      if (assembledObject instanceof RecordChangeEvent) {
        recordChangeEvent = (RecordChangeEvent) assembledObject;
        replicationCheckpoint = recordChangeEvent.replicationCheckpointVector;
        pubSubMessage = Optional.of(
            convertChangeEventToPubSubMessage(
                recordChangeEvent,
                keyDeserializer.deserialize(keyBytes),
                pubSubTopicPartition,
                consumerRecord.offset(),
                consumerRecord.timestamp(),
                payloadSize));
      } else {
        replicationCheckpoint =
            extractOffsetVectorFromMessage(put.getReplicationMetadataVersionId(), put.getReplicationMetadataPayload());
        ChangeEvent<V> changeEvent = new ChangeEvent<>(null, (V) assembledObject);
        pubSubMessage = Optional.of(
            new ImmutablePubSubMessage<>(
                keyDeserializer.deserialize(keyBytes),
                changeEvent,
                pubSubTopicPartition,
                consumerRecord.offset(),
                consumerRecord.timestamp(),
                payloadSize));
      }

      // Determine if the event should be filtered or not
      if (filterRecordByVersionSwapHighWatermarks(replicationCheckpoint, pubSubTopicPartition)) {
        pubSubMessage = Optional.empty();
      }
    }
    return pubSubMessage;
  }

  protected List<Long> extractOffsetVectorFromMessage(
      int replicationMetadataVersionId,
      ByteBuffer replicationMetadataPayload) {
    if (replicationMetadataVersionId > 0) {
      MultiSchemaResponse.Schema replicationMetadataSchema =
          replicationMetadataSchemaRepository.getReplicationMetadataSchemaById(storeName, replicationMetadataVersionId);
      RecordDeserializer<GenericRecord> deserializer = SerializerDeserializerFactory
          .getAvroGenericDeserializer(Schema.parse(replicationMetadataSchema.getSchemaStr()));
      GenericRecord replicationMetadataRecord = deserializer.deserialize(replicationMetadataPayload);
      GenericData.Array replicationCheckpointVector =
          (GenericData.Array) replicationMetadataRecord.get(REPLICATION_CHECKPOINT_VECTOR_FIELD);
      List<Long> offsetVector = new ArrayList<>();
      for (Object o: replicationCheckpointVector) {
        offsetVector.add((Long) o);
      }
      return offsetVector;
    }
    return new ArrayList<>();
  }

  protected boolean handleVersionSwapControlMessage(
      ControlMessage controlMessage,
      PubSubTopicPartition pubSubTopicPartition,
      String topicSuffix) {
    ControlMessageType controlMessageType = ControlMessageType.valueOf(controlMessage);
    if (controlMessageType.equals(ControlMessageType.VERSION_SWAP)) {
      VersionSwap versionSwap = (VersionSwap) controlMessage.controlMessageUnion;
      LOGGER.info(
          "Obtain version swap message: {} and versions swap high watermarks: {}",
          versionSwap,
          versionSwap.getLocalHighWatermarks());
      String newServingVersionTopic = versionSwap.newServingVersionTopic.toString();
      currentVersionHighWatermarks.put(pubSubTopicPartition.getPartitionNumber(), versionSwap.getLocalHighWatermarks());
      switchToNewTopic(newServingVersionTopic, topicSuffix);
      inMemoryStorageEngine.drop();
      return true;
    }
    return false;
  }

  private PubSubMessage<K, ChangeEvent<V>, Long> convertChangeEventToPubSubMessage(
      RecordChangeEvent recordChangeEvent,
      K currentKey,
      PubSubTopicPartition pubSubTopicPartition,
      Long offset,
      Long timestamp,
      int payloadSize) {
    V currentValue = null;
    if (recordChangeEvent.currentValue != null && recordChangeEvent.currentValue.getSchemaId() > 0) {
      currentValuePayloadSize[pubSubTopicPartition.getPartitionNumber()] =
          recordChangeEvent.currentValue.getValue().array().length;
      currentValue = deserializeValueFromBytes(
          recordChangeEvent.currentValue.getValue(),
          recordChangeEvent.currentValue.getSchemaId());
    }
    V previousValue = null;
    if (recordChangeEvent.previousValue != null && recordChangeEvent.previousValue.getSchemaId() > 0) {
      previousValue = deserializeValueFromBytes(
          recordChangeEvent.previousValue.getValue(),
          recordChangeEvent.previousValue.getSchemaId());
    }
    ChangeEvent<V> changeEvent = new ChangeEvent<>(previousValue, currentValue);
    return new ImmutablePubSubMessage<>(currentKey, changeEvent, pubSubTopicPartition, offset, timestamp, payloadSize);
  }

  private V deserializeValueFromBytes(ByteBuffer byteBuffer, int valueSchemaId) {
    Schema currentValueSchema = schemaReader.getValueSchema(valueSchemaId);
    RecordDeserializer<V> valueDeserializer =
        FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(currentValueSchema, currentValueSchema);
    if (byteBuffer != null) {
      return valueDeserializer.deserialize(byteBuffer);
    }
    return null;
  }

  private boolean filterRecordByVersionSwapHighWatermarks(
      List<Long> recordCheckpointVector,
      PubSubTopicPartition pubSubTopicPartition) {
    int partitionId = pubSubTopicPartition.getPartitionNumber();
    if (recordCheckpointVector != null && currentVersionHighWatermarks.containsKey(partitionId)) {
      List<Long> partitionCurrentVersionHighWatermarks = currentVersionHighWatermarks.get(partitionId);
      if (recordCheckpointVector.size() > partitionCurrentVersionHighWatermarks.size()) {
        return false;
      }
      // Only filter the record when all regions fall behind.
      for (int i = 0; i < recordCheckpointVector.size(); i++) {
        if (recordCheckpointVector.get(i) > partitionCurrentVersionHighWatermarks.get(i)) {
          return false;
        }
      }
      return true;
    }
    // Has not met version swap message after client initialization.
    return false;
  }

  protected void switchToNewTopic(String newTopic, String topicSuffix) {
    String mergedTopicName = newTopic + topicSuffix;
    Set<Integer> partitions = new HashSet<>(subscribedPartitions);
    unsubscribe(subscribedPartitions);
    currentTopic = mergedTopicName;
    try {
      subscribe(partitions).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new VeniceException("Subscribe to new topic:" + mergedTopicName + " is not successful, error: " + e);
    }
  }

  protected PubSubTopicPartition getPubSubTopicPartitionFromConsumerRecord(
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord) {
    return new PubSubTopicPartitionImpl(
        pubSubTopicRepository.getTopic(consumerRecord.topic()),
        consumerRecord.partition());
  }

  @Override
  public void close() {
    this.unsubscribeAll();
    kafkaConsumer.close();
  }

  protected void setReadOnlySchemaRepository(ThinClientMetaStoreBasedRepository repository) {
    this.readOnlySchemaRepository = repository;
  }
}
