package com.linkedin.davinci.store.view;

import static com.linkedin.venice.views.ViewUtils.NEARLINE_PRODUCER_COMPRESSION_ENABLED;
import static com.linkedin.venice.views.ViewUtils.NEARLINE_PRODUCER_COUNT_PER_WRITER;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.kafka.consumer.PartitionConsumptionState;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.views.VeniceView;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


/**
 * This class is kept separate from the VeniceView class to not leak certain concepts that
 * currently exclusively reside in the server to other components. This class is created per
 * {@link com.linkedin.davinci.kafka.consumer.LeaderFollowerStoreIngestionTask} per view and is
 * invoked to processRecords in the {@link com.linkedin.davinci.kafka.consumer.ActiveActiveStoreIngestionTask}
 * as it's consumed from Kafka and after MergeConflict resolution has run. It's then invoked prior
 * to committing to local persistent storage or local VT production in order to keep integrity of
 * the data in various failure scenarios. This implies that repeat calls on the same data for
 * processRecords can happen and there should be some protection from that being an issue in different
 * view implementations.
 */
public abstract class VeniceViewWriter extends VeniceView {
  public enum ViewWriterType {
    MATERIALIZED_VIEW, CHANGE_CAPTURE_VIEW
  }

  protected final Version version;
  protected final int versionNumber;
  protected Optional<Boolean> isNearlineProducerCompressionEnabled = Optional.empty();
  protected Optional<Integer> nearlineProducerCountPerWriter = Optional.empty();
  protected final VeniceWriterFactory veniceWriterFactory;

  public VeniceViewWriter(
      VeniceConfigLoader props,
      Version version,
      Schema keySchema,
      Map<String, String> extraViewParameters,
      VeniceWriterFactory veniceWriterFactory) {
    super(props.getCombinedProperties().toProperties(), version.getStoreName(), extraViewParameters);
    this.version = version;
    this.versionNumber = version.getNumber();
    if (extraViewParameters.containsKey(NEARLINE_PRODUCER_COMPRESSION_ENABLED)) {
      isNearlineProducerCompressionEnabled =
          Optional.of(Boolean.valueOf(extraViewParameters.get(NEARLINE_PRODUCER_COMPRESSION_ENABLED)));
    }
    if (extraViewParameters.containsKey(NEARLINE_PRODUCER_COUNT_PER_WRITER)) {
      nearlineProducerCountPerWriter =
          Optional.of(Integer.valueOf(extraViewParameters.get(NEARLINE_PRODUCER_COUNT_PER_WRITER)));
    }
    this.veniceWriterFactory = veniceWriterFactory;
  }

  /**
   * To be called as a given ingestion task consumes each record. This is called prior to writing to a
   * VT or to persistent storage.
   *
   * @param newValue the incoming fully specified value which hasn't yet been committed to Venice
   * @param oldValue the previous value which has already been locally committed to Venice for the given key
   * @param key the key of the record that designates newValue and oldValue
   * @param newValueSchemaId the schemaId of the incoming record
   * @param oldValueSchemaId the schemaId of the old record
   * @param replicationMetadataRecord the associated RMD for the incoming record.
   * @param valueProvider to provide the corresponding deserialized newValue for PUT and UPDATE or the old value for the
   *                      given key for DELETE.
   */
  public abstract CompletableFuture<Void> processRecord(
      ByteBuffer newValue,
      ByteBuffer oldValue,
      byte[] key,
      int newValueSchemaId,
      int oldValueSchemaId,
      GenericRecord replicationMetadataRecord,
      Lazy<GenericRecord> valueProvider);

  /**
   * To be called as a given ingestion task consumes each record. This is called prior to writing to a
   * VT or to persistent storage.
   *
   * @param newValue the incoming fully specified value which hasn't yet been committed to Venice
   * @param key the key of the record that designates newValue and oldValue
   * @param newValueSchemaId the schemaId of the incoming record
   * @param viewPartitionSet set of view partitions this record should be processed to. This is used in NR
   *                                 pass-through when remote region leaders can forward record or chunks of a record
   *                                 to the correct view partitions without the need to perform chunk assembly or
   *                                 repartitioning.
   * @param newValueProvider to provide the deserialized new value
   */
  public abstract CompletableFuture<Void> processRecord(
      ByteBuffer newValue,
      byte[] key,
      int newValueSchemaId,
      Set<Integer> viewPartitionSet,
      Lazy<GenericRecord> newValueProvider);

  public abstract ViewWriterType getViewWriterType();

  /**
   * Called when the server encounters a control message. There isn't (today) a strict ordering
   * on if the rest of the server alters it's state completely or not based on the incoming control message
   * relative to the given view.
   *
   * Different view types may be interested in different control messages and act differently. The corresponding
   * view writer should implement this method accordingly.
   *
   * @param kafkaKey the corresponding kafka key of this control message
   * @param kafkaMessageEnvelope the corresponding kafka message envelope of this control message
   * @param controlMessage the control message we're processing
   * @param partition the partition this control message was delivered to
   * @param partitionConsumptionState the pcs of the consuming node
   */
  public void processControlMessage(
      KafkaKey kafkaKey,
      KafkaMessageEnvelope kafkaMessageEnvelope,
      ControlMessage controlMessage,
      int partition,
      PartitionConsumptionState partitionConsumptionState) {
    // Optionally act on Control Message
  }

  /**
   * A store could have many views and to reduce the impact to write throughput we want to check and enable producer
   * optimizations that can be configured at the store level. To change the producer optimization configs the ingestion
   * task needs to be re-initialized. Meaning either a new version push or server restart after the store level config
   * change and this is by design.
   * @param configBuilder to be configured with the producer optimizations
   * @return
   */
  protected VeniceWriterOptions.Builder setProducerOptimizations(VeniceWriterOptions.Builder configBuilder) {
    isNearlineProducerCompressionEnabled.ifPresent(configBuilder::setProducerCompressionEnabled);
    nearlineProducerCountPerWriter.ifPresent(configBuilder::setProducerCount);
    return configBuilder;
  }
}
