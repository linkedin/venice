package com.linkedin.venice.writer;

import static com.linkedin.venice.memory.ClassSizeEstimator.getClassOverhead;

import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.memory.InstanceSizeEstimator;
import com.linkedin.venice.memory.Measurable;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;


public class ProducerBufferRecord implements Measurable {
  private static final int SHALLOW_CLASS_OVERHEAD = getClassOverhead(ProducerBufferRecord.class);
  private final byte[] serializedKey;
  private final byte[] serializedValue;
  private byte[] serializedUpdate;
  private final MessageType messageType;
  private final int schemaId;
  private final int protocolId;
  private final long timestamp;
  private final PubSubProducerCallback callback;
  private final List<PubSubProducerCallback> dependentCallbackList = new ArrayList<>();
  private final List<ProducerBufferRecord> dependentRecordList = new ArrayList<>();
  private CompletableFuture<PubSubProduceResult> produceResultFuture = null;
  private boolean shouldSkipProduce = false;

  public ProducerBufferRecord(
      MessageType messageType,
      byte[] serializedKey,
      byte[] serializedValue,
      byte[] serializedUpdate,
      int schemaId,
      int protocolId,
      PubSubProducerCallback callback,
      long timestamp) {
    this.serializedKey = serializedKey;
    this.serializedValue = serializedValue;
    this.serializedUpdate = serializedUpdate;
    this.messageType = messageType;
    this.schemaId = schemaId;
    this.protocolId = protocolId;
    this.timestamp = timestamp;
    this.callback = callback;
  }

  public boolean shouldSkipProduce() {
    return shouldSkipProduce;
  }

  public void setSkipProduce(boolean shouldSkipProduce) {
    this.shouldSkipProduce = shouldSkipProduce;
  }

  public byte[] getSerializedKey() {
    return serializedKey;
  }

  public byte[] getSerializedValue() {
    return serializedValue;
  }

  public byte[] getSerializedUpdate() {
    return serializedUpdate;
  }

  public int getSchemaId() {
    return schemaId;
  }

  public int getProtocolId() {
    return protocolId;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public MessageType getMessageType() {
    return messageType;
  }

  public List<PubSubProducerCallback> getDependentCallbackList() {
    return dependentCallbackList;
  }

  public void addDependentCallback(PubSubProducerCallback callback) {
    this.dependentCallbackList.add(callback);
  }

  public PubSubProducerCallback getCallback() {
    return callback;
  }

  public CompletableFuture<PubSubProduceResult> getProduceResultFuture() {
    return produceResultFuture;
  }

  public void setProduceResultFuture(CompletableFuture<PubSubProduceResult> produceResultFuture) {
    this.produceResultFuture = produceResultFuture;
  }

  public void addRecordToDependentRecordList(ProducerBufferRecord record) {
    dependentRecordList.add(record);
  }

  public List<ProducerBufferRecord> getDependentRecordList() {
    return dependentRecordList;
  }

  /**
   * This method convert message into a PUT message type.
   */
  public void updateSerializedUpdate(byte[] serializedUpdate) {
    this.serializedUpdate = serializedUpdate;
  }

  @Override
  public int getHeapSize() {
    int size = SHALLOW_CLASS_OVERHEAD;
    size += InstanceSizeEstimator.getObjectSize(serializedKey);
    if (serializedValue != null) {
      size += InstanceSizeEstimator.getObjectSize(serializedValue);
    }
    if (serializedUpdate != null) {
      size += InstanceSizeEstimator.getObjectSize(serializedUpdate);
    }
    if (callback != null && callback instanceof Measurable) {
      size += ((Measurable) callback).getHeapSize();
    }
    return size;
  }
}
