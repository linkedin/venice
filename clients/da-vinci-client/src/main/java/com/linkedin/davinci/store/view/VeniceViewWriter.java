package com.linkedin.davinci.store.view;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.kafka.consumer.PartitionConsumptionState;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.views.VeniceView;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


public abstract class VeniceViewWriter extends VeniceView {
  protected final Schema keySchema;

  public VeniceViewWriter(
      VeniceConfigLoader props,
      Store store,
      Schema keySchema,
      Map<String, String> extraViewParameters) {
    super(props.getCombinedProperties().toProperties(), store, extraViewParameters);
    this.keySchema = keySchema;
  }

  public void processRecord(
      ByteBuffer newValue,
      ByteBuffer oldValue,
      ByteBuffer key,
      int version,
      int valueSchemaId,
      GenericRecord replicationMetadataRecord) {
  }

  public void processControlMessage(
      ControlMessage controlMessage,
      int partition,
      PartitionConsumptionState partitionConsumptionState,
      int version) {
    // Optionally act on Control Message
  }
}
