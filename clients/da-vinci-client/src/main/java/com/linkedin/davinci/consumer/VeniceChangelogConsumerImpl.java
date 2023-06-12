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
import com.linkedin.venice.kafka.protocol.VersionSwap;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.schema.rmd.RmdUtils;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.DictionaryUtils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.views.ChangeCaptureView;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceChangelogConsumerImpl<K, V> implements VeniceChangelogConsumer<K, V> {
  private static final Logger LOGGER = LogManager.getLogger(VeniceChangelogConsumerImpl.class);
  private final int partitionCount;

  protected static final VeniceCompressor NO_OP_COMPRESSOR = new NoopCompressor();

  protected final CompressorFactory compressorFactory = new CompressorFactory();

  protected final HashMap<Integer, VeniceCompressor> compressorMap = new HashMap<>();

  protected ThinClientMetaStoreBasedRepository storeRepository;

  protected final ReadOnlySchemaRepository recordChangeEventSchemaRepository;

  protected final AbstractAvroChunkingAdapter<RecordChangeEvent> recordChangeEventChunkingAdapter =
      new SpecificRecordChunkingAdapter<>(RecordChangeEvent.class);

  protected final AbstractAvroChunkingAdapter<V> userEventChunkingAdapter;

  protected final SchemaReader schemaReader;
  private final String viewClassName;
  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  protected final RecordDeserializer<K> keyDeserializer;
  private final D2ControllerClient d2ControllerClient;
  private final RecordDeserializer<RecordChangeEvent> recordChangeDeserializer =
      FastSerializerDeserializerFactory.getFastAvroSpecificDeserializer(
          AvroProtocolDefinition.RECORD_CHANGE_EVENT.getCurrentProtocolVersionSchema(),
          RecordChangeEvent.class);
  protected final ReplicationMetadataSchemaRepository replicationMetadataSchemaRepository;

  protected final String storeName;

  // This storage engine serves as a buffer for records which are chunked and have to be buffered before they can
  // be returned to the client. We leverage the storageEngine interface here in order to take better advantage
  // of the chunking and decompressing adapters that we've already built (which today are built around this interface)
  // as chunked records are assembled we will eagerly evict all keys from the storage engine in order to keep the memory
  // footprint as small as we can. We could use the object cache storage engine here in order to get LRU behavior
  // but then that runs the risk of a parallel subscription having record chunks getting evicted before we have a chance
  // to assemble them. So we rely on the simpler and concrete implementation as opposed to the abstraction in order
  // to control and guarantee the behavior we're expecting.
  protected final InMemoryStorageEngine inMemoryStorageEngine;
  protected final PubSubConsumerAdapter pubSubConsumer;
  protected final Map<Integer, List<Long>> currentVersionHighWatermarks = new HashMap<>();
  protected final int[] currentValuePayloadSize;

  protected final ChangelogClientConfig changelogClientConfig;

  public VeniceChangelogConsumerImpl(
      ChangelogClientConfig changelogClientConfig,
      PubSubConsumerAdapter pubSubConsumer) {
    this.pubSubConsumer = pubSubConsumer;
    this.storeName = changelogClientConfig.getStoreName();
    this.d2ControllerClient = changelogClientConfig.getD2ControllerClient();
    StoreResponse storeResponse = changelogClientConfig.getD2ControllerClient().getStore(storeName);
    if (storeResponse.isError()) {
      throw new VeniceException(
          "Failed to get store info for store: " + storeName + " with error: " + storeResponse.getError());
    }
    StoreInfo store = storeResponse.getStore();
    this.changelogClientConfig = ChangelogClientConfig.cloneConfig(changelogClientConfig);
    this.partitionCount = store.getPartitionCount();
    this.currentValuePayloadSize = new int[partitionCount];
    this.viewClassName = changelogClientConfig.getViewName();
    this.replicationMetadataSchemaRepository = new ReplicationMetadataSchemaRepository(d2ControllerClient);
    this.schemaReader = changelogClientConfig.getSchemaReader();
    Schema keySchema = schemaReader.getKeySchema();
    this.keyDeserializer = FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(keySchema, keySchema);
    // The in memory storage engine only relies on the name of store and nothing else. We use an unversioned store name
    // here in order to reduce confusion (as this storage engine can be used across version topics).
    this.inMemoryStorageEngine = new InMemoryStorageEngine(storeName);
    this.storeRepository = new ThinClientMetaStoreBasedRepository(
        changelogClientConfig.getInnerClientConfig(),
        VeniceProperties.empty(),
        null);
    recordChangeEventSchemaRepository = new RecordChangeEventReadOnlySchemaRepository(this.storeRepository);
    Class<V> valueClass = changelogClientConfig.getInnerClientConfig().getSpecificValueClass();
    if (valueClass != null) {
      // If a value class is supplied, we'll use a Specific record adapter
      userEventChunkingAdapter = new SpecificRecordChunkingAdapter(valueClass);
    } else {
      userEventChunkingAdapter = GenericChunkingAdapter.INSTANCE;
    }
    LOGGER.info(
        "Start a change log consumer client for store: {}, with partition count: {} and view class: {} ",
        storeName,
        partitionCount,
        viewClassName);
  }

  @Override
  public CompletableFuture<Void> subscribe(Set<Integer> partitions) {
    return internalSubscribe(partitions, null);
  }

  public CompletableFuture<Void> internalSubscribe(Set<Integer> partitions, PubSubTopic topic) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        storeRepository.start();
        storeRepository.subscribe(storeName);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      storeRepository.refresh();

      PubSubTopic topicToSubscribe;
      if (topic == null) {
        topicToSubscribe = getCurrentServingVersionTopic();
      } else {
        topicToSubscribe = topic;
      }

      synchronized (pubSubConsumer) {
        Set<PubSubTopicPartition> topicPartitionSet = new HashSet<>(pubSubConsumer.getAssignment());
        for (PubSubTopicPartition topicPartition: pubSubConsumer.getAssignment()) {
          if (partitions.contains(topicPartition.getPartitionNumber())) {
            pubSubConsumer.unSubscribe(topicPartition);
          }
        }

        List<PubSubTopicPartition> topicPartitionList =
            getPartitionListToSubscribe(partitions, topicPartitionSet, topicToSubscribe);
        List<PubSubTopicPartition> topicPartitionListToSeek =
            getPartitionListToSubscribe(partitions, Collections.EMPTY_SET, topicToSubscribe);

        for (PubSubTopicPartition topicPartition: topicPartitionList) {
          if (!topicPartition.getPubSubTopic().getName().endsWith(ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX)) {
            compressorMap.put(topicPartition.getPartitionNumber(), getVersionCompressor(topicPartition));
          }
        }
        for (PubSubTopicPartition topicPartition: topicPartitionListToSeek) {
          pubSubConsumer.subscribe(topicPartition, OffsetRecord.LOWEST_OFFSET);
        }
        return null;
      }
    });
  }

  protected VeniceCompressor getVersionCompressor(PubSubTopicPartition topicPartition) {
    Store store = storeRepository.getStore(storeName);
    String topicName = topicPartition.getPubSubTopic().getName();
    Version version = store.getVersion(Version.parseVersionFromVersionTopicName(topicName)).get();
    VeniceCompressor compressor;
    if (CompressionStrategy.ZSTD_WITH_DICT.equals(version.getCompressionStrategy())) {
      compressor = compressorFactory.getVersionSpecificCompressor(topicName);
      if (compressor == null) {
        // we need to retrieve the dictionary from the kafka topic
        ByteBuffer dictionary = DictionaryUtils
            .readDictionaryFromKafka(topicName, new VeniceProperties(changelogClientConfig.getConsumerProperties()));
        compressor = compressorFactory
            .createVersionSpecificCompressorIfNotExist(version.getCompressionStrategy(), topicName, dictionary.array());
      }
    } else {
      compressor = compressorFactory.getCompressor(version.getCompressionStrategy());
    }
    return compressor;
  }

  @Override
  public CompletableFuture<Void> seekToBeginningOfPush(Set<Integer> partitions) {
    // Get latest version topic
    PubSubTopic topic = getCurrentServingVersionTopic();
    return internalSeek(partitions, topic, p -> pubSubConsumer.subscribe(p, OffsetRecord.LOWEST_OFFSET));
  }

  @Override
  public CompletableFuture<Void> seekToBeginningOfPush() {
    return seekToBeginningOfPush(
        pubSubConsumer.getAssignment()
            .stream()
            .map(topicPartition -> topicPartition.getPartitionNumber())
            .collect(Collectors.toSet()));
  }

  @Override
  public CompletableFuture<Void> seekToEndOfPush(Set<Integer> partitions) {
    // Get the latest change capture topic
    storeRepository.refresh();
    Store store = storeRepository.getStore(storeName);
    int currentVersion = store.getCurrentVersion();
    PubSubTopic topic = pubSubTopicRepository
        .getTopic(Version.composeKafkaTopic(storeName, currentVersion) + ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX);
    return internalSeek(partitions, topic, p -> pubSubConsumer.subscribe(p, OffsetRecord.LOWEST_OFFSET));
  }

  @Override
  public void pause() {
    this.pause(
        pubSubConsumer.getAssignment()
            .stream()
            .map(PubSubTopicPartition::getPartitionNumber)
            .collect(Collectors.toSet()));
  }

  @Override
  public void resume(Set<Integer> partitions) {
    synchronized (pubSubConsumer) {
      Set<PubSubTopicPartition> currentSubscriptions = pubSubConsumer.getAssignment();
      for (PubSubTopicPartition partition: currentSubscriptions) {
        if (partitions.contains(partition.getPartitionNumber())) {
          pubSubConsumer.resume(partition);
        }
      }
    }
  }

  @Override
  public void resume() {
    this.resume(
        pubSubConsumer.getAssignment()
            .stream()
            .map(PubSubTopicPartition::getPartitionNumber)
            .collect(Collectors.toSet()));
  }

  @Override
  public void pause(Set<Integer> partitions) {
    synchronized (pubSubConsumer) {
      Set<PubSubTopicPartition> currentSubscriptions = pubSubConsumer.getAssignment();
      for (PubSubTopicPartition partition: currentSubscriptions) {
        if (partitions.contains(partition.getPartitionNumber())) {
          pubSubConsumer.pause(partition);
        }
      }
    }
  }

  @Override
  public CompletableFuture<Void> seekToEndOfPush() {
    return seekToEndOfPush(
        pubSubConsumer.getAssignment()
            .stream()
            .map(topicPartition -> topicPartition.getPartitionNumber())
            .collect(Collectors.toSet()));
  }

  @Override
  public CompletableFuture<Void> seekToTail(Set<Integer> partitions) {
    // Get the latest change capture topic
    PubSubTopic topic =
        pubSubTopicRepository.getTopic(getCurrentServingVersionTopic() + ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX);
    return internalSeek(partitions, topic, p -> {
      Long partitionEndOffset = pubSubConsumer.endOffset(p);
      pubSubConsumerSeek(p, partitionEndOffset);
    });
  }

  private PubSubTopic getCurrentServingVersionTopic() {
    storeRepository.refresh();
    Store store = storeRepository.getStore(storeName);
    int currentVersion = store.getCurrentVersion();
    return pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName, currentVersion));
  }

  @Override
  public CompletableFuture<Void> seekToTail() {
    return seekToTail(
        pubSubConsumer.getAssignment()
            .stream()
            .map(topicPartition -> topicPartition.getPartitionNumber())
            .collect(Collectors.toSet()));
  }

  @Override
  public CompletableFuture<Void> seekToCheckpoint(Set<VeniceChangeCoordinate> checkpoints) {
    return CompletableFuture.supplyAsync(() -> {
      for (VeniceChangeCoordinate coordinate: checkpoints) {
        PubSubTopic topic = pubSubTopicRepository.getTopic(coordinate.getTopic());
        PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(topic, coordinate.getPartition());
        internalSeek(Collections.singleton(coordinate.getPartition()), topic, foo -> {
          Long topicOffset = ((ApacheKafkaOffsetPosition) coordinate.getPosition()).getOffset();
          pubSubConsumerSeek(pubSubTopicPartition, topicOffset);
        }).join();
      }
      return null;
    });
  }

  private void pubSubConsumerSeek(PubSubTopicPartition topicPartition, Long offset) {
    // Offset the seek to next operation inside venice pub sub consumer adapter subscription logic.
    if (offset == OffsetRecord.LOWEST_OFFSET) {
      pubSubConsumer.subscribe(topicPartition, OffsetRecord.LOWEST_OFFSET);
    } else {
      pubSubConsumer.subscribe(topicPartition, offset - 1);
    }
  }

  @Override
  public CompletableFuture<Void> subscribeAll() {
    Set<Integer> allPartitions = new HashSet<>();
    for (int partition = 0; partition < partitionCount; partition++) {
      allPartitions.add(partition);
    }
    return this.subscribe(allPartitions);
  }

  @Override
  public CompletableFuture<Void> seekToTimestamps(Map<Integer, Long> timestamps) {
    // Get the latest change capture topic
    storeRepository.refresh();
    Store store = storeRepository.getStore(storeName);
    int currentVersion = store.getCurrentVersion();
    String topicName =
        Version.composeKafkaTopic(storeName, currentVersion) + ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX;
    PubSubTopic topic = pubSubTopicRepository.getTopic(topicName);
    Map<PubSubTopicPartition, Long> topicPartitionLongMap = new HashMap<>();
    for (Map.Entry<Integer, Long> timestampPair: timestamps.entrySet()) {
      PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(topic, timestampPair.getKey());
      topicPartitionLongMap.put(topicPartition, timestampPair.getValue());
    }
    return internalSeek(timestamps.keySet(), topic, partition -> {
      Long offset = pubSubConsumer.offsetForTime(partition, topicPartitionLongMap.get(partition));
      // As the offset for this timestamp does not exist, we need to seek to the very end of the topic partition.
      if (offset == null) {
        offset = pubSubConsumer.endOffset(partition);
      }
      pubSubConsumerSeek(partition, offset);
    });
  }

  @Override
  public CompletableFuture<Void> seekToTimestamp(Long timestamp) {
    Set<PubSubTopicPartition> topicPartitionSet = new HashSet<>(pubSubConsumer.getAssignment());
    Map<Integer, Long> partitionsToSeek = new HashMap<>();
    for (PubSubTopicPartition partition: topicPartitionSet) {
      partitionsToSeek.put(partition.getPartitionNumber(), timestamp);
    }
    return this.seekToTimestamps(partitionsToSeek);
  }

  public CompletableFuture<Void> internalSeek(
      Set<Integer> partitions,
      PubSubTopic targetTopic,
      SeekFunction seekAction) {
    return CompletableFuture.supplyAsync(() -> {
      synchronized (pubSubConsumer) {
        // Prune out current subscriptions
        for (PubSubTopicPartition topicPartition: pubSubConsumer.getAssignment()) {
          currentVersionHighWatermarks.remove(topicPartition.getPartitionNumber());
          if (partitions.contains(topicPartition.getPartitionNumber())) {
            pubSubConsumer.unSubscribe(topicPartition);
          }
        }

        List<PubSubTopicPartition> topicPartitionListToSeek =
            getPartitionListToSubscribe(partitions, Collections.EMPTY_SET, targetTopic);
        for (PubSubTopicPartition topicPartition: topicPartitionListToSeek) {
          if (!topicPartition.getPubSubTopic().getName().endsWith(ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX)) {
            compressorMap.put(topicPartition.getPartitionNumber(), getVersionCompressor(topicPartition));
          }
          seekAction.apply(topicPartition);
        }

      }
      return null;
    });
  }

  @FunctionalInterface
  interface SeekFunction {
    void apply(PubSubTopicPartition partitionToSeek);
  }

  private List<PubSubTopicPartition> getPartitionListToSubscribe(
      Set<Integer> partitions,
      Set<PubSubTopicPartition> topicPartitionSet,
      PubSubTopic topic) {
    List<PubSubTopicPartition> topicPartitionList = new ArrayList<>();
    for (Integer partition: partitions) {
      PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(topic, partition);
      if (!topicPartitionSet.contains(topicPartition)) {
        topicPartitionList.add(topicPartition);
      }
    }
    topicPartitionList.addAll(topicPartitionSet);
    return topicPartitionList;
  }

  @Override
  public void unsubscribe(Set<Integer> partitions) {
    synchronized (pubSubConsumer) {
      Set<PubSubTopicPartition> topicPartitionSet = new HashSet<>(pubSubConsumer.getAssignment());
      Set<PubSubTopicPartition> topicPartitionsToUnsub = new HashSet<>();
      for (PubSubTopicPartition topicPartition: topicPartitionSet) {
        if (partitions.contains(topicPartition.getPartitionNumber())) {
          topicPartitionsToUnsub.add(topicPartition);
        }
      }
      pubSubConsumer.batchUnsubscribe(topicPartitionsToUnsub);
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
  public Collection<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> poll(long timeoutInMs) {
    return internalPoll(timeoutInMs, ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX);
  }

  protected Collection<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> internalPoll(
      long timeoutInMs,
      String topicSuffix) {
    List<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> pubSubMessages = new ArrayList<>();
    Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> messagesMap;
    synchronized (pubSubConsumer) {
      messagesMap = pubSubConsumer.poll(timeoutInMs);
    }
    for (Map.Entry<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> entry: messagesMap
        .entrySet()) {
      PubSubTopicPartition pubSubTopicPartition = entry.getKey();
      List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> messageList = entry.getValue();
      for (PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> message: messageList) {
        if (message.getKey().isControlMessage()) {
          ControlMessage controlMessage = (ControlMessage) message.getValue().getPayloadUnion();
          if (handleControlMessage(controlMessage, pubSubTopicPartition, topicSuffix)) {
            break;
          }
        } else {
          Optional<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> pubSubMessage =
              convertPubSubMessageToPubSubChangeEventMessage(message, pubSubTopicPartition);
          pubSubMessage.ifPresent(pubSubMessages::add);
        }
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
    if (controlMessageType.equals(ControlMessageType.END_OF_PUSH)) {
      LOGGER.info(
          "End of Push message received for version {} for store {}",
          Version.parseVersionFromKafkaTopicName(pubSubTopicPartition.getPubSubTopic().getName()),
          storeName);
      // Jump to next topic
      // TODO: Today we don't publish the version swap message to the version topic. This necessitates relying on the
      // change capture topic in order to navigate version pushes. We should pass the topicSuffix argument here once
      // that
      // support lands.
      switchToNewTopic(
          pubSubTopicPartition.getPubSubTopic(),
          ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX,
          pubSubTopicPartition.getPartitionNumber());
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
      compressor = compressorMap.get(pubSubTopicPartition.getPartitionNumber());
    } else {
      compressor = NO_OP_COMPRESSOR;
    }

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
    // the cache. Kafka might give duplicate delivery, but it won't give out of order delivery, so
    // this is safe to do in all such contexts.
    inMemoryStorageEngine.dropPartition(pubSubTopicPartition.getPartitionNumber());
    return assembledRecord;
  }

  protected Optional<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> convertPubSubMessageToPubSubChangeEventMessage(
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> message,
      PubSubTopicPartition pubSubTopicPartition) {
    Optional<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> pubSubChangeEventMessage = Optional.empty();
    byte[] keyBytes = message.getKey().getKey();
    MessageType messageType = MessageType.valueOf(message.getValue());
    RecordChangeEvent recordChangeEvent;
    // Internal store ingestion tasks only persist PUT messages to either VT or view topics
    if (messageType.equals(MessageType.PUT)) {
      Put put = (Put) message.getValue().payloadUnion;
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
        schemaRepo = storeRepository;
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
          message.getOffset(),
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
      int payloadSize = message.getPayloadSize();
      if (assembledObject instanceof RecordChangeEvent) {
        recordChangeEvent = (RecordChangeEvent) assembledObject;
        replicationCheckpoint = recordChangeEvent.replicationCheckpointVector;
        pubSubChangeEventMessage = Optional.of(
            convertChangeEventToPubSubMessage(
                recordChangeEvent,
                keyDeserializer.deserialize(keyBytes),
                pubSubTopicPartition,
                message.getOffset(),
                message.getPubSubMessageTime(),
                payloadSize));
      } else {
        replicationCheckpoint =
            extractOffsetVectorFromMessage(put.getReplicationMetadataVersionId(), put.getReplicationMetadataPayload());
        ChangeEvent<V> changeEvent = new ChangeEvent<>(null, (V) assembledObject);
        pubSubChangeEventMessage = Optional.of(
            new ImmutableChangeCapturePubSubMessage<>(
                keyDeserializer.deserialize(keyBytes),
                changeEvent,
                pubSubTopicPartition,
                message.getOffset(),
                message.getPubSubMessageTime(),
                payloadSize));
      }

      // Determine if the event should be filtered or not
      if (filterRecordByVersionSwapHighWatermarks(replicationCheckpoint, pubSubTopicPartition)) {
        pubSubChangeEventMessage = Optional.empty();
      }
    }
    return pubSubChangeEventMessage;
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
      PubSubTopic newServingVersionTopic =
          pubSubTopicRepository.getTopic(versionSwap.newServingVersionTopic.toString());

      // TODO: There seems to exist a condition in the server where highwatermark offsets may regress when transmitting
      // the version swap message
      // it seems like this can potentially happen if a repush occurs and no data is consumed on that previous version.
      // To make the client
      // handle this gracefully, we instate the below condition that says the hwm in the client should never go
      // backwards.
      if (RmdUtils.hasOffsetAdvanced(
          currentVersionHighWatermarks.getOrDefault(pubSubTopicPartition.getPartitionNumber(), Collections.EMPTY_LIST),
          versionSwap.getLocalHighWatermarks())) {
        currentVersionHighWatermarks
            .put(pubSubTopicPartition.getPartitionNumber(), versionSwap.getLocalHighWatermarks());
      }
      switchToNewTopic(newServingVersionTopic, topicSuffix, pubSubTopicPartition.getPartitionNumber());
      inMemoryStorageEngine.drop();
      return true;
    }
    return false;
  }

  private PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate> convertChangeEventToPubSubMessage(
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
    return new ImmutableChangeCapturePubSubMessage<>(
        currentKey,
        changeEvent,
        pubSubTopicPartition,
        offset,
        timestamp,
        payloadSize);
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
      List<Long> partitionCurrentVersionHighWatermarks =
          currentVersionHighWatermarks.getOrDefault(partitionId, Collections.EMPTY_LIST);
      return !RmdUtils.hasOffsetAdvanced(partitionCurrentVersionHighWatermarks, recordCheckpointVector);
    }
    // Has not met version swap message after client initialization.
    return false;
  }

  protected void switchToNewTopic(PubSubTopic newTopic, String topicSuffix, Integer partition) {
    PubSubTopic mergedTopicName = pubSubTopicRepository.getTopic(newTopic.getName() + topicSuffix);
    Set<Integer> partitions = Collections.singleton(partition);
    unsubscribe(partitions);
    try {
      internalSubscribe(partitions, mergedTopicName).get();
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
    pubSubConsumer.close();
  }

  protected void setStoreRepository(ThinClientMetaStoreBasedRepository repository) {
    this.storeRepository = repository;
  }
}
