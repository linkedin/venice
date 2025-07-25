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
  private final byte[] serializedUpdate;
  private final MessageType messageType;
  private final int schemaId;
  private final int protocolId;
  private final long logicalTimestamp;
  private boolean shouldSkipProduce = false;
  private PubSubProducerCallback callback;
  private List<PubSubProducerCallback> dependentCallbackList = new ArrayList<>();
  private CompletableFuture<PubSubProduceResult> produceResultFuture = null;

  public ProducerBufferRecord(
      MessageType messageType,
      byte[] serializedKey,
      byte[] serializedValue,
      byte[] serializedUpdate,
      int schemaId,
      int protocolId,
      PubSubProducerCallback callback,
      long logicalTimestamp) {
    this.serializedKey = serializedKey;
    this.serializedValue = serializedValue;
    this.serializedUpdate = serializedUpdate;
    this.messageType = messageType;
    this.schemaId = schemaId;
    this.protocolId = protocolId;
    // Let's do not consider logical TS as of now.
    this.logicalTimestamp = logicalTimestamp;
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

  public long getLogicalTimestamp() {
    return logicalTimestamp;
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
