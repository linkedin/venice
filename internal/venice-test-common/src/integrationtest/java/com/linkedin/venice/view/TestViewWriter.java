package com.linkedin.venice.view;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType;
import com.linkedin.davinci.kafka.consumer.PartitionConsumptionState;
import com.linkedin.davinci.store.view.VeniceViewWriter;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.VersionSwap;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.VeniceProperties;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


public class TestViewWriter extends VeniceViewWriter {
  final private TestView internalView;

  public TestViewWriter(
      VeniceConfigLoader props,
      Store store,
      Schema keySchema,
      Map<String, String> extraViewParameters) {
    super(props, store, keySchema, extraViewParameters);
    internalView = new TestView(props.getCombinedProperties().toProperties(), store, extraViewParameters);
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
    internalView.incrementRecordCount(store.getName());
  }

  @Override
  public void processControlMessage(
      ControlMessage controlMessage,
      int partition,
      PartitionConsumptionState partitionConsumptionState,
      int version) {

    // TODO: The below logic only operates on VersionSwap. We might want to augment this
    // logic to handle other control messages.

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

    // Optionally act on Control Message
    internalView.incrementVersionSwapMessageCountForStore(store.getName());
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
  }

}
