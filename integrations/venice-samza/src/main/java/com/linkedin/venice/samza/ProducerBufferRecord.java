package com.linkedin.venice.samza;

import com.linkedin.venice.kafka.protocol.enums.MessageType;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;


public class ProducerBufferRecord {
  private final byte[] keyBytes;
  private final byte[] valueBytes;
  private final MessageType messageType;
  private final int schemaId;
  private final int protocolId;
  private final long logicalTimestamp;
  private boolean shouldSkipProduce = false;
  private CompletableFuture<Void> future = null;
  private List<CompletableFuture<Void>> dependentFutureList = new ArrayList<>();

  public ProducerBufferRecord(
      MessageType messageType,
      byte[] keyBytes,
      byte[] valueBytes,
      int schemaId,
      int protocolId,
      long logicalTimestamp) {
    this.keyBytes = keyBytes;
    this.valueBytes = valueBytes;
    this.messageType = messageType;
    this.schemaId = schemaId;
    this.protocolId = protocolId;
    // Let's do not consider logical TS as of now.
    this.logicalTimestamp = logicalTimestamp;
  }

  public boolean shouldSkipProduce() {
    return shouldSkipProduce;
  }

  public void setSkipProduce(boolean shouldSkipProduce) {
    this.shouldSkipProduce = shouldSkipProduce;
  }

  public CompletableFuture<Void> getFuture() {
    return future;
  }

  public byte[] getKeyBytes() {
    return keyBytes;
  }

  public byte[] getValueBytes() {
    return valueBytes;
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

  public List<CompletableFuture<Void>> getDependentFutureList() {
    return dependentFutureList;
  }

  public void addFutureToDependentFutureList(CompletableFuture<Void> future) {
    this.dependentFutureList.add(future);
  }
}
