package com.linkedin.davinci.store.view;

import com.linkedin.davinci.kafka.consumer.PartitionConsumptionState;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.views.VeniceView;
import java.nio.ByteBuffer;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


public abstract class VeniceViewWriter extends VeniceView {
  protected final Schema keySchema;

  public VeniceViewWriter(Properties props, Store store, Schema keySchema) {
    super(props, store);
    this.keySchema = keySchema;
  }

  public void processRecord(
      Put record,
      ByteBuffer currentValue,
      ByteBuffer key,
      int version,
      Schema valueSchema,
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
