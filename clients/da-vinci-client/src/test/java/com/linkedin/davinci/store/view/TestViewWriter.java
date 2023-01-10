package com.linkedin.davinci.store.view;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.kafka.consumer.PartitionConsumptionState;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.meta.Store;
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
  }

  @Override
  public void processControlMessage(
      ControlMessage controlMessage,
      int partition,
      PartitionConsumptionState partitionConsumptionState,
      int version) {
    // Optionally act on Control Message
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
