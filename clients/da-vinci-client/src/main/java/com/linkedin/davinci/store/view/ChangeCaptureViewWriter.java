package com.linkedin.davinci.store.view;

import static com.linkedin.venice.writer.VeniceWriter.DEFAULT_LEADER_METADATA_WRAPPER;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType;
import com.linkedin.davinci.kafka.consumer.PartitionConsumptionState;
import com.linkedin.venice.client.change.capture.protocol.RecordChangeEvent;
import com.linkedin.venice.client.change.capture.protocol.ValueBytes;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.VersionSwap;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapterFactory;
import com.linkedin.venice.schema.rmd.RmdUtils;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.views.ChangeCaptureView;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ChangeCaptureViewWriter extends VeniceViewWriter {
  private static final Logger LOGGER = LogManager.getLogger(ChangeCaptureViewWriter.class);
  final private ChangeCaptureView internalView;
  private VeniceWriter veniceWriter;
  private final Object2IntMap<String> kafkaClusterUrlToIdMap;
  private final int maxColoIdValue;

  private final PubSubProducerAdapterFactory pubSubProducerAdapterFactory;

  public ChangeCaptureViewWriter(
      VeniceConfigLoader props,
      Store store,
      Schema keySchema,
      Map<String, String> extraViewParameters) {
    super(props, store, keySchema, extraViewParameters);
    internalView = new ChangeCaptureView(props.getCombinedProperties().toProperties(), store, extraViewParameters);
    kafkaClusterUrlToIdMap = props.getVeniceServerConfig().getKafkaClusterUrlToIdMap();
    pubSubProducerAdapterFactory = props.getVeniceServerConfig().getPubSubClientsFactory().getProducerAdapterFactory();
    maxColoIdValue = kafkaClusterUrlToIdMap.values().stream().max(Integer::compareTo).orElse(-1);

  }

  @Override
  public void processRecord(
      ByteBuffer newValue,
      ByteBuffer oldValue,
      byte[] key,
      int version,
      int newValueSchemaId,
      int oldValueSchemaId,
      GenericRecord replicationMetadataRecord) {
    // TODO: not sold about having currentValue in the interface but it VASTLY simplifies a lot of things with regards
    // to dealing with compression/chunking/etc. in the storage layer.

    RecordChangeEvent recordChangeEvent = new RecordChangeEvent();
    recordChangeEvent.currentValue = constructValueBytes(newValue, newValueSchemaId);
    recordChangeEvent.previousValue = constructValueBytes(oldValue, oldValueSchemaId);
    recordChangeEvent.key = ByteBuffer.wrap(key);
    recordChangeEvent.replicationCheckpointVector = RmdUtils.extractOffsetVectorFromRmd(replicationMetadataRecord);

    if (veniceWriter == null) {
      initializeVeniceWriter(version);
    }
    // TODO: RecordChangeEvent isn't versioned today.
    // TODO: Chunking?
    // updatedKeyBytes = ChunkingUtils.KEY_WITH_CHUNKING_SUFFIX_SERIALIZER.serializeNonChunkedKey(key); (line 604
    // A/AIngestionTask?)
    try {
      veniceWriter.put(key, recordChangeEvent, 1).get();
    } catch (InterruptedException | ExecutionException e) {
      LOGGER
          .error("Failed to produce to Change Capture view topic for store: {} version: {}", store.getName(), version);
      throw new VeniceException(e);
    }
  }

  @Override
  public void processControlMessage(
      ControlMessage controlMessage,
      int partition,
      PartitionConsumptionState partitionConsumptionState,
      int version) {

    // We only care (for now) about version swap control Messages
    if (!(controlMessage.getControlMessageUnion() instanceof VersionSwap)) {
      return;
    }

    // Only leaders should produce to Change Capture topics
    if (partitionConsumptionState.getLeaderFollowerState() != LeaderFollowerStateType.LEADER) {
      return;
    }

    // Parse VersionSwap
    VersionSwap versionSwapMessage = (VersionSwap) controlMessage.getControlMessageUnion();

    // Only the version we're transiting FROM needs to populate the topic switch message into the change capture topic
    if (Version.parseVersionFromVersionTopicName(versionSwapMessage.oldServingVersionTopic.toString()) != version) {
      return;
    }

    Map<String, Long> sortedWaterMarkOffsets = partitionConsumptionState.getLatestProcessedUpstreamRTOffsetMap();

    List<Long> highWaterMarkOffsets;
    if (maxColoIdValue > -1) {
      highWaterMarkOffsets = new ArrayList<>(Collections.nCopies(maxColoIdValue + 1, 0L));
      for (String url: sortedWaterMarkOffsets.keySet()) {
        highWaterMarkOffsets.set(
            kafkaClusterUrlToIdMap.getInt(url),
            partitionConsumptionState.getLatestProcessedUpstreamRTOffsetMap().get(url));
      }
    } else {
      highWaterMarkOffsets = Collections.emptyList();
    }

    // Write the message on veniceWriter to the change capture topic
    if (veniceWriter == null) {
      initializeVeniceWriter(version);
    }

    veniceWriter.sendControlMessage(
        constructVersionSwapControlMessage(versionSwapMessage, highWaterMarkOffsets),
        partitionConsumptionState.getPartition(),
        Collections.emptyMap(),
        null,
        DEFAULT_LEADER_METADATA_WRAPPER);
  }

  @Override
  public Map<String, VeniceProperties> getTopicNamesAndConfigsForVersion(int version) {
    return internalView.getTopicNamesAndConfigsForVersion(version);
  }

  @Override
  public String getWriterClassName() {
    return internalView.getWriterClassName();
  }

  @Override
  public void close() {
    internalView.close();
    if (veniceWriter != null) {
      veniceWriter.close();
    }
  }

  // package private, for testing only
  void setVeniceWriter(VeniceWriter veniceWriter) {
    this.veniceWriter = veniceWriter;
  }

  VeniceWriterOptions buildWriterOptions(int version) {
    String changeCaptureTopicName = this.getTopicNamesAndConfigsForVersion(version).keySet().stream().findAny().get();

    // Build key/value Serializers for the kafka producer
    VeniceWriterOptions.Builder configBuilder = new VeniceWriterOptions.Builder(changeCaptureTopicName);
    VeniceKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(RecordChangeEvent.getClassSchema());
    configBuilder.setValueSerializer(valueSerializer);

    // Set writer properties based on the store version config
    Version storeVersionConfig = store.getVersion(version).get();
    PartitionerConfig partitionerConfig = storeVersionConfig.getPartitionerConfig();

    if (partitionerConfig != null) {
      // TODO: It would make sense to give the option to set a different partitioner for this view. Might
      // want to consider adding it as a param available to this view type.
      VenicePartitioner venicePartitioner = PartitionUtils.getVenicePartitioner(partitionerConfig);
      configBuilder.setPartitioner(venicePartitioner);
    }

    configBuilder.setChunkingEnabled(storeVersionConfig.isChunkingEnabled());
    return configBuilder.build();
  }

  synchronized private void initializeVeniceWriter(int version) {
    if (veniceWriter != null) {
      return;
    }
    veniceWriter = new VeniceWriterFactory(props, pubSubProducerAdapterFactory, null)
        .createVeniceWriter(buildWriterOptions(version));
  }

  private ValueBytes constructValueBytes(ByteBuffer value, int schemaId) {
    if (value == null) {
      return null;
    }
    ValueBytes valueBytes = new ValueBytes();
    valueBytes.schemaId = schemaId;
    valueBytes.value = value;
    return valueBytes;
  }

  private ControlMessage constructVersionSwapControlMessage(
      VersionSwap versionSwapMessage,
      List<Long> localHighWatermarks) {
    ControlMessage controlMessageToBroadcast = new ControlMessage();
    controlMessageToBroadcast.controlMessageType = ControlMessageType.VERSION_SWAP.getValue();
    controlMessageToBroadcast.controlMessageUnion = ControlMessageType.VERSION_SWAP.getNewInstance();
    VersionSwap versionSwapToBroadcast = new VersionSwap();
    versionSwapToBroadcast.oldServingVersionTopic = versionSwapMessage.oldServingVersionTopic;
    versionSwapToBroadcast.newServingVersionTopic = versionSwapMessage.newServingVersionTopic;
    versionSwapToBroadcast.localHighWatermarks = localHighWatermarks;
    controlMessageToBroadcast.controlMessageUnion = versionSwapToBroadcast;
    return controlMessageToBroadcast;
  }
}
