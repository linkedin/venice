package com.linkedin.davinci.store.view;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.kafka.consumer.PartitionConsumptionState;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.views.MaterializedView;
import com.linkedin.venice.writer.ComplexVeniceWriter;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


/**
 * Materialized view writer is responsible for processing input records from the version topic and write them to the
 * materialized view topic based on parameters defined in {@link com.linkedin.venice.meta.MaterializedViewParameters}.
 * This writer has its own {@link VeniceWriter}.
 */
public class MaterializedViewWriter extends VeniceViewWriter {
  private final MaterializedView internalView;
  private final String materializedViewTopicName;
  private Lazy<ComplexVeniceWriter> veniceWriter;

  public MaterializedViewWriter(
      VeniceConfigLoader props,
      Version version,
      Schema keySchema,
      Map<String, String> extraViewParameters,
      VeniceWriterFactory veniceWriterFactory) {
    super(props, version, keySchema, extraViewParameters, veniceWriterFactory);
    internalView =
        new MaterializedView(props.getCombinedProperties().toProperties(), version.getStoreName(), extraViewParameters);
    materializedViewTopicName =
        internalView.getTopicNamesAndConfigsForVersion(version.getNumber()).keySet().stream().findAny().get();
    this.veniceWriter = Lazy.of(() -> veniceWriterFactory.createComplexVeniceWriter(buildWriterOptions()));
  }

  /**
   * package private for testing purpose
   */
  void setVeniceWriter(ComplexVeniceWriter veniceWriter) {
    this.veniceWriter = Lazy.of(() -> veniceWriter);
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
    return processRecord(newValue, key, newValueSchemaId, null, valueProvider);
  }

  /**
   * During NR pass-through viewPartitionSet is going to be provided. This way we can forward record or chunks of a
   * record to the appropriate view partition without the need to assemble or repartition.
   */
  @Override
  public CompletableFuture<Void> processRecord(
      ByteBuffer newValue,
      byte[] key,
      int newValueSchemaId,
      Set<Integer> viewPartitionSet,
      Lazy<GenericRecord> newValueProvider) {
    byte[] newValueBytes = newValue == null ? null : ByteUtils.extractByteArray(newValue);
    if (viewPartitionSet != null) {
      if (newValue == null) {
        // This is unexpected because we only attach view partition map for PUT records.
        throw new VeniceException(
            "Encountered a null PUT record while having view partition map in the message header");
      }
      // Forward the record to corresponding view partition without any processing (NR pass-through mode).
      return veniceWriter.get().forwardPut(key, newValueBytes, newValueSchemaId, viewPartitionSet);
    }
    if (newValue == null) {
      // This is a delete operation. newValueProvider will contain the old value in a best effort manner. The old value
      // might not be available if we are deleting a non-existing key.
      return veniceWriter.get().complexDelete(key, newValueProvider);
    }
    return veniceWriter.get().complexPut(key, newValueBytes, newValueSchemaId, newValueProvider);
  }

  @Override
  public ViewWriterType getViewWriterType() {
    return ViewWriterType.MATERIALIZED_VIEW;
  }

  @Override
  public void processControlMessage(
      KafkaKey kafkaKey,
      KafkaMessageEnvelope kafkaMessageEnvelope,
      ControlMessage controlMessage,
      int partition,
      PartitionConsumptionState partitionConsumptionState) {
    // Ignore all control messages for materialized view for now. Will revisit on the client side time lag monitoring.
    // TODO we need to handle new version CM for CC for materialized view.
  }

  @Override
  public String getWriterClassName() {
    return internalView.getWriterClassName();
  }

  @Override
  public void close(boolean gracefulClose) {
    internalView.close(gracefulClose);
    if (veniceWriter.isPresent()) {
      veniceWriter.get().close(gracefulClose);
    }
  }

  // Package private for testing
  VeniceWriterOptions buildWriterOptions() {
    return setProducerOptimizations(internalView.getWriterOptionsBuilder(materializedViewTopicName, version)).build();
  }

  public boolean isComplexVenicePartitioner() {
    return internalView.getViewPartitioner().getPartitionerType() == VenicePartitioner.VenicePartitionerType.COMPLEX;
  }

  public String getViewName() {
    return internalView.getViewName();
  }
}
