package com.linkedin.venice.message;

import java.nio.ByteBuffer;


/**
 * 4 bytes - total size
 * 4 bytes - topic name size
 * 4 bytes - key name size
 * 4 bytes - partition number
 * n bytes - topic name
 * m bytes - key
 */
public class GetRequestObject {
  public static final int HEADER_SIZE = 16;

  private char[] topic;
  private byte[] key;
  private int partition;

  public GetRequestObject() {
  }

  public GetRequestObject(char[] topic, int partition, byte[] key) {
    this.topic = topic;
    this.key = key;
    this.partition = partition;
  }

  public String getTopicString() {
    return new String(topic);
  }

  public String getStoreString() {
    return this.getTopicString();
  }

  public char[] getTopic() {
    return topic;
  }

  public char[] getStore() {
    return this.getTopic();
  }

  public byte[] getTopicBytes() {
    return this.getTopicString().getBytes();
  }

  public byte[] getStoreBytes() {
    return this.getTopicBytes();
  }

  public void setTopic(char[] topic) {
    this.topic = topic;
  }

  public void setTopic(String topic) {
    this.topic = topic.toCharArray();
  }

  public void setStore(char[] store) {
    this.setTopic(store);
  }

  public void setStore(String store) {
    this.setTopic(store);
  }

  public byte[] getKey() {
    return key;
  }

  public void setKey(byte[] key) {
    this.key = key;
  }

  public int getPartition() {
    return partition;
  }

  public void setPartition(int partition) {
    this.partition = partition;
  }

  public void setPartition(String partition) {
    this.partition = Integer.parseInt(partition);
  }

  public byte[] serialize() {
    int topicNameSize = topic.length;
    int keySize = key.length;
    int totalSize = 16 + topicNameSize + keySize; // 12 for 4 ints (at 4 bytes each), totalSize, keySize, topicNameSize,
                                                  // partition number
    ByteBuffer payload = ByteBuffer.allocate(totalSize);
    // Worry about payload.order?
    payload.putInt(totalSize);
    payload.putInt(topicNameSize);
    payload.putInt(keySize);
    payload.putInt(partition);
    payload.put(this.getStoreBytes());
    payload.put(this.getKey());
    return payload.array();
  }

  // This will be wrong if we don't have at least 4 bytes
  public static int parseSize(byte[] payload) {
    return ByteBuffer.wrap(payload).getInt(0);
  }

  static boolean isHeaderComplete(byte[] payload) {
    return (payload.length >= HEADER_SIZE);
  }

  public static boolean isPayloadComplete(byte[] payload) {
    return (isHeaderComplete(payload) && payload.length >= parseSize(payload));
  }

  public static GetRequestObject deserialize(byte[] payload) {
    if (!isHeaderComplete(payload)) {
      throw new IllegalArgumentException("GetRequestObject must have at least a " + HEADER_SIZE + " byte header");
    }
    ByteBuffer bufferedPayload = ByteBuffer.wrap(payload);
    int totalSize = bufferedPayload.getInt();
    if (payload.length < totalSize) {
      throw new IllegalArgumentException(
          "GetRequestObject indicates size of " + totalSize + " but only " + payload.length + " bytes available");
    }
    GetRequestObject obj = new GetRequestObject();
    int topicNameSize = bufferedPayload.getInt();
    int keySize = bufferedPayload.getInt();
    obj.setPartition(bufferedPayload.getInt());

    byte[] topicBytes = new byte[topicNameSize];
    bufferedPayload.get(topicBytes);
    obj.setTopic(new String(topicBytes).toCharArray());

    byte[] key = new byte[keySize];
    bufferedPayload.get(key);
    obj.setKey(key);

    return obj;
  }
}
