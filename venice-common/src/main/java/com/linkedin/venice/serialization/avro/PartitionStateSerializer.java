package com.linkedin.venice.serialization.avro;

import com.linkedin.venice.kafka.protocol.state.PartitionState;


/**
 * Serializer for the Avro-based offset recording protocol defined in:
 *
 * {@link PartitionState}
 */
public class PartitionStateSerializer extends InternalAvroSpecificSerializer<PartitionState> {
  public PartitionStateSerializer() {
    super(AvroProtocolDefinition.PARTITION_STATE);
  }
}
