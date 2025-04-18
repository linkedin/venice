package com.linkedin.davinci.store.view;

import static com.linkedin.venice.writer.VeniceWriter.DEFAULT_LEADER_METADATA_WRAPPER;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType;
import com.linkedin.davinci.kafka.consumer.PartitionConsumptionState;
import com.linkedin.venice.client.change.capture.protocol.RecordChangeEvent;
import com.linkedin.venice.client.change.capture.protocol.ValueBytes;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.VersionSwap;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.schema.rmd.RmdUtils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


public class ChangeCaptureViewWriter extends VeniceViewWriter {
  final private ChangeCaptureView internalView;
  private VeniceWriter veniceWriter;
  private final Object2IntMap<String> kafkaClusterUrlToIdMap;
  private final int maxColoIdValue;
  private final String changeCaptureTopicName;

  public ChangeCaptureViewWriter(
      VeniceConfigLoader props,
      Version version,
      Schema keySchema,
      Map<String, String> extraViewParameters,
      VeniceWriterFactory veniceWriterFactory) {
    super(props, version, keySchema, extraViewParameters, veniceWriterFactory);
    internalView = new ChangeCaptureView(
        props.getCombinedProperties().toProperties(),
        version.getStoreName(),
        extraViewParameters);
    kafkaClusterUrlToIdMap = props.getVeniceServerConfig().getKafkaClusterUrlToIdMap();
    maxColoIdValue = kafkaClusterUrlToIdMap.values().stream().max(Integer::compareTo).orElse(-1);
    changeCaptureTopicName =
        this.getTopicNamesAndConfigsForVersion(version.getNumber()).keySet().stream().findAny().get();
  }

  @Override
  public CompletableFuture<Void> processRecord(
      ByteBuffer newValue,
      ByteBuffer oldValue,
      byte[] key,
      int newValueSchemaId,
      int oldValueSchemaId,
      GenericRecord replicationMetadataRecord,
      Lazy<GenericRecord> valueProvider) {
    // TODO: not sold about having currentValue in the interface but it VASTLY simplifies a lot of things with regards
    // to dealing with compression/chunking/etc. in the storage layer.

    RecordChangeEvent recordChangeEvent = new RecordChangeEvent();
    recordChangeEvent.currentValue = constructValueBytes(newValue, newValueSchemaId);
    recordChangeEvent.previousValue = constructValueBytes(oldValue, oldValueSchemaId);
    recordChangeEvent.key = ByteBuffer.wrap(key);
    recordChangeEvent.replicationCheckpointVector = RmdUtils.extractOffsetVectorFromRmd(replicationMetadataRecord);

    if (veniceWriter == null) {
      initializeVeniceWriter();
    }
    // TODO: RecordChangeEvent isn't versioned today.
    return veniceWriter.put(key, recordChangeEvent, 1);
  }

  @Override
  public CompletableFuture<Void> processRecord(
      ByteBuffer newValue,
      byte[] key,
      int newValueSchemaId,
      Set<Integer> viewPartitionSet,
      Lazy<GenericRecord> newValueProvider) {
    // No op
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public ViewWriterType getViewWriterType() {
    return ViewWriterType.CHANGE_CAPTURE_VIEW;
  }

  @Override
  public void processControlMessage(
      KafkaKey kafkaKey,
      KafkaMessageEnvelope kafkaMessageEnvelope,
      ControlMessage controlMessage,
      int partition,
      PartitionConsumptionState partitionConsumptionState) {

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
    if (Version
        .parseVersionFromVersionTopicName(versionSwapMessage.oldServingVersionTopic.toString()) != versionNumber) {
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
      initializeVeniceWriter();
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
  public String composeTopicName(int version) {
    return internalView.composeTopicName(version);
  }

  @Override
  public String getWriterClassName() {
    return internalView.getWriterClassName();
  }

  @Override
  public void close(boolean gracefulClose) {
    internalView.close(gracefulClose);
    if (veniceWriter != null) {
      veniceWriter.close(gracefulClose);
    }
  }

  // package private, for testing only
  void setVeniceWriter(VeniceWriter veniceWriter) {
    this.veniceWriter = veniceWriter;
  }

  VeniceWriterOptions buildWriterOptions() {
    return internalView.getWriterOptionsBuilder(changeCaptureTopicName, version).build();
  }

  synchronized private void initializeVeniceWriter() {
    if (veniceWriter != null) {
      return;
    }
    veniceWriter = veniceWriterFactory.createVeniceWriter(buildWriterOptions());
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
