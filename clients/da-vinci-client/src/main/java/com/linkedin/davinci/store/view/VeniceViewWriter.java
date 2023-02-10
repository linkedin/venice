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
  public VeniceViewWriter(
      VeniceConfigLoader props,
      Store store,
      Schema keySchema,
      Map<String, String> extraViewParameters) {
    super(props.getCombinedProperties().toProperties(), store, extraViewParameters);
  }

  /**
   * To be called as a given ingestion task consumes each record. This is called prior to writing to a
   * VT or to persistent storage.
   *
   * @param newValue the incoming fully specified value which hasn't yet been committed to Venice
   * @param oldValue the previous value which has already been locally committed to Venice for the given key
   * @param key the key of the record that designates newValue and oldValue
   * @param version the version of the store taking this record
   * @param newValueSchemaId the schemaId of the incoming record
   * @param oldValueSchemaId the schemaId of the old record
   * @param replicationMetadataRecord the associated RMD for the incoming record.
   */
  public void processRecord(
      ByteBuffer newValue,
      ByteBuffer oldValue,
      byte[] key,
      int version,
      int newValueSchemaId,
      int oldValueSchemaId,
      GenericRecord replicationMetadataRecord) {
  }

  /**
   * Called when the server encounters a control message. There isn't (today) a strict ordering
   * on if the rest of the server alters it's state completely or not based on the incoming control message
   * relative to the given view.
   *
   * TODO: Today this is only invoked for VERSION_SWAP control message, but we
   * may in the future call this method for all control messages so that certain
   * view types can act accordingly.
   *
   * @param controlMessage the control message we're processing
   * @param partition the partition this control message was delivered to
   * @param partitionConsumptionState the pcs of the consuming node
   * @param version the store version that received this message
   */
  public void processControlMessage(
      ControlMessage controlMessage,
      int partition,
      PartitionConsumptionState partitionConsumptionState,
      int version) {
    // Optionally act on Control Message
  }
}
