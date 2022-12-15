package com.linkedin.davinci.store.view;

import static com.linkedin.venice.writer.VeniceWriter.*;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType;
import com.linkedin.davinci.kafka.consumer.PartitionConsumptionState;
import com.linkedin.venice.client.change.capture.protocol.RecordChangeEvent;
import com.linkedin.venice.client.change.capture.protocol.ValueBytes;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.VersionSwap;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.VenicePartitioner;
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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


public class ChangeCaptureViewWriter extends VeniceViewWriter {
  final private ChangeCaptureView internalView;
  private VeniceWriter veniceWriter;
  private final Object2IntMap<String> kafkaClusterUrlToIdMap;

  public ChangeCaptureViewWriter(
      VeniceConfigLoader props,
      Store store,
      Schema keySchema,
      Map<String, String> extraViewParameters) {
    super(props, store, keySchema, extraViewParameters);
    internalView = new ChangeCaptureView(props.getCombinedProperties().toProperties(), store, extraViewParameters);
    kafkaClusterUrlToIdMap = props.getVeniceServerConfig().getKafkaClusterUrlToIdMap();
  }

  @Override
  public void processRecord(
      ByteBuffer newValue,
      ByteBuffer oldValue,
      ByteBuffer key,
      int version,
      int valueSchemaId,
      GenericRecord replicationMetadataRecord) {
    // TODO: not sold about having currentValue in the interface but it VASTLY simplifies a lot of things with regards
    // to dealing with compression/chunking/etc. in the storage layer.

    RecordChangeEvent recordChangeEvent = new RecordChangeEvent();
    recordChangeEvent.currentValue = constructValueBytes(newValue, valueSchemaId); // TODO: Verify this is
                                                                                   // the right schemaID, the
                                                                                   // record might also
                                                                                   // contain RMD data
    recordChangeEvent.previousValue = constructValueBytes(oldValue, valueSchemaId);
    recordChangeEvent.key = key;
    recordChangeEvent.replicationCheckpointVector = RmdUtils.extractOffsetVectorFromRmd(replicationMetadataRecord);

    if (veniceWriter == null) {
      initializeVeniceWriter(version);
    }
    // TODO: RecordChangeEvent isn't versioned today.
    veniceWriter.put(key, recordChangeEvent, 1);
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
    List<Long> highWaterMarkOffsets = new ArrayList<>();
    for (String url: sortedWaterMarkOffsets.keySet()) {
      highWaterMarkOffsets.add(
          kafkaClusterUrlToIdMap.getInt(url),
          partitionConsumptionState.getLatestProcessedUpstreamRTOffsetMap().get(url));
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
    veniceWriter.close();
  }

  synchronized private void initializeVeniceWriter(int version) {
    if (veniceWriter != null) {
      return;
    }
    String changeCaptureTopicName = this.getTopicNamesAndConfigsForVersion(version).keySet().stream().findAny().get();

    // Build key/value Serializers for the kafka producer
    VeniceWriterOptions.Builder configBuilder = new VeniceWriterOptions.Builder(changeCaptureTopicName);
    VeniceKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(keySchema);
    VeniceKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(RecordChangeEvent.getClassSchema());
    configBuilder.setKeySerializer(keySerializer);
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
    veniceWriter = new VeniceWriterFactory(props).createVeniceWriter(configBuilder.build());
  }

  private ValueBytes constructValueBytes(ByteBuffer value, int schemaId) {
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
