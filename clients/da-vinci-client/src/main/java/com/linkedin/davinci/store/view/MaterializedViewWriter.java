package com.linkedin.davinci.store.view;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.kafka.consumer.PartitionConsumptionState;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.ComplexVenicePartitioner;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.serialization.KeyWithChunkingSuffixSerializer;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.views.MaterializedView;
import com.linkedin.venice.writer.ComplexVeniceWriter;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


/**
 * Materialized view writer is responsible for processing input records from the version topic and write them to the
 * materialized view topic based on parameters defined in {@link com.linkedin.venice.meta.MaterializedViewParameters}.
 * This writer has its own {@link VeniceWriter}.
 */
public class MaterializedViewWriter extends VeniceViewWriter {
  private final PubSubProducerAdapterFactory pubSubProducerAdapterFactory;
  private final MaterializedView internalView;
  private final String materializedViewTopicName;
  private Lazy<ComplexVeniceWriter> veniceWriter;
  private final KeyWithChunkingSuffixSerializer keyWithChunkingSuffixSerializer = new KeyWithChunkingSuffixSerializer();

  public MaterializedViewWriter(
      VeniceConfigLoader props,
      Version version,
      Schema keySchema,
      Map<String, String> extraViewParameters) {
    super(props, version, keySchema, extraViewParameters);
    pubSubProducerAdapterFactory = props.getVeniceServerConfig().getPubSubClientsFactory().getProducerAdapterFactory();
    internalView =
        new MaterializedView(props.getCombinedProperties().toProperties(), version.getStoreName(), extraViewParameters);
    materializedViewTopicName =
        internalView.getTopicNamesAndConfigsForVersion(version.getNumber()).keySet().stream().findAny().get();
    this.veniceWriter = Lazy.of(
        () -> new VeniceWriterFactory(props.getCombinedProperties().toProperties(), pubSubProducerAdapterFactory, null)
            .createComplexVeniceWriter(buildWriterOptions()));
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
    return processRecord(newValue, key, newValueSchemaId, false, valueProvider);
  }

  /**
   * Before we have proper chunking support for view writers we assume that even when chunking is enabled the actual
   * k/v will not be chunked. This way we don't need to worry about how to ensure all the chunks are forwarded to the
   * same view partition during NR's pass-through mode. Proper chunking support can leverage message footer populated
   * by the CompositeVeniceWriter in VPJ (write to views first and then VT) to figure out which view partition to
   * forward the chunks to. Another alternative is trigger the view writer write upon receiving the manifest, and we
   * will assemble and re-chunk.
   */
  @Override
  public CompletableFuture<Void> processRecord(
      ByteBuffer newValue,
      byte[] key,
      int newValueSchemaId,
      boolean isChunkedKey,
      Lazy<GenericRecord> newValueProvider) {
    byte[] viewTopicKey = key;
    if (isChunkedKey) {
      viewTopicKey = keyWithChunkingSuffixSerializer.getKeyFromChunkedKey(key);
    }
    byte[] newValueBytes = newValue == null ? null : ByteUtils.extractByteArray(newValue);
    if (newValue == null) {
      // this is a delete operation
      return veniceWriter.get().complexDelete(viewTopicKey, newValueProvider);
    }
    return veniceWriter.get().complexPut(viewTopicKey, newValueBytes, newValueSchemaId, newValueProvider);
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

  // Package private for testing
  VeniceWriterOptions buildWriterOptions() {
    return setProducerOptimizations(internalView.getWriterOptionsBuilder(materializedViewTopicName, version)).build();
  }

  public boolean isComplexVenicePartitioner() {
    return internalView.getViewPartitioner() instanceof ComplexVenicePartitioner;
  }
}
