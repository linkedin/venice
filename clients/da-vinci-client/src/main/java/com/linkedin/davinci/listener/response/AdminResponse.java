package com.linkedin.davinci.listener.response;

import com.linkedin.davinci.kafka.consumer.PartitionConsumptionState;
import com.linkedin.venice.admin.protocol.response.AdminResponseRecord;
import com.linkedin.venice.admin.protocol.response.ConsumptionStateSnapshot;
import com.linkedin.venice.admin.protocol.response.ServerConfigSnapshot;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.ByteBufferToHexFormatJsonEncoder;
import org.apache.avro.io.Encoder;


/**
 * This class stores all the information required for answering a server admin request.
 */
public class AdminResponse {
  private boolean isError;
  private String message;
  private final AdminResponseRecord responseRecord;

  public AdminResponse() {
    this.isError = false;
    this.responseRecord = new AdminResponseRecord();
  }

  /**
   * Add a partition consumption state to the admin response record
   */
  public void addPartitionConsumptionState(PartitionConsumptionState state) {
    if (responseRecord.partitionConsumptionStates == null) {
      responseRecord.partitionConsumptionStates = new ArrayList<>();
    }
    ConsumptionStateSnapshot snapshot = new ConsumptionStateSnapshot();
    snapshot.partitionId = state.getPartition();
    snapshot.hybrid = state.isHybrid();
    snapshot.offsetRecord = state.getOffsetRecord().toJsonString();
    snapshot.deferredWrite = state.isDeferredWrite();
    snapshot.errorReported = state.isErrorReported();
    snapshot.lagCaughtUp = !state.isWaitingForReplicationLag();
    snapshot.completionReported = state.isCompletionReported();
    snapshot.leaderState = state.getLeaderFollowerState().toString().toUpperCase();
    snapshot.isLatchReleased = state.isLatchReleased();
    snapshot.processedRecordSizeSinceLastSync = state.getProcessedRecordSizeSinceLastSync();
    snapshot.consumeRemotely = state.consumeRemotely();
    snapshot.latestMessageConsumptionTimestampInMs = state.getLatestMessageConsumptionTimestampInMs();
    responseRecord.partitionConsumptionStates.add(snapshot);
  }

  /**
   * Add store version state metadata into the admin response record
   */
  public void addStoreVersionState(StoreVersionState storeVersionState) {
    try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
      GenericDatumWriter<Object> avroDatumWriter = new GenericDatumWriter<>(StoreVersionState.SCHEMA$);
      Encoder byteToHexJsonEncoder = new ByteBufferToHexFormatJsonEncoder(StoreVersionState.SCHEMA$, output);
      avroDatumWriter.write(storeVersionState, byteToHexJsonEncoder);
      byteToHexJsonEncoder.flush();
      output.flush();
      responseRecord.storeVersionState = new String(output.toByteArray());
    } catch (IOException exception) {
      throw new VeniceException(exception);
    }
  }

  /**
   * Load all server configs into admin response record
   */
  public void addServerConfigs(Properties properties) {
    if (responseRecord.serverConfigs == null) {
      responseRecord.serverConfigs = new ServerConfigSnapshot();
    }
    Map<CharSequence, CharSequence> configMap = new HashMap<>();
    for (Map.Entry<Object, Object> entry: properties.entrySet()) {
      configMap.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
    }
    responseRecord.serverConfigs.configMap = configMap;
  }

  public ByteBuf getResponseBody() {
    return Unpooled.wrappedBuffer(serializedResponse());
  }

  public byte[] serializedResponse() {
    RecordSerializer<AdminResponseRecord> serializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(AdminResponseRecord.SCHEMA$);
    return serializer.serialize(this.responseRecord);
  }

  public int getResponseSchemaIdHeader() {
    return AvroProtocolDefinition.SERVER_ADMIN_RESPONSE_V1.getCurrentProtocolVersion();
  }

  public void setError(boolean error) {
    this.isError = error;
  }

  public boolean isError() {
    return this.isError;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public String getMessage() {
    return this.message;
  }

  public AdminResponseRecord getResponseRecord() {
    return this.responseRecord;
  }

}
