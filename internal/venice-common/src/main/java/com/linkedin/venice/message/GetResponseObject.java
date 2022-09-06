package com.linkedin.venice.message;

import java.nio.ByteBuffer;


/**
 * TODO: Abstract GetRequestObject and GetResponseObject as common abstract class?
 * Not worth the time since these are only prototype objects that will be replaced
 *
 * 4 bytes - total size
 * m bytes - value
 */
public class GetResponseObject {
  public static final int HEADER_SIZE = 4;

  private byte[] value;

  public GetResponseObject() {
    value = null;
  }

  public GetResponseObject(byte[] value) {
    this.value = value;
  }

  public byte[] getValue() {
    return value;
  }

  public void setValue(byte[] value) {
    this.value = value;
  }

  public byte[] serialize() {
    int totalSize = HEADER_SIZE + value.length;
    ByteBuffer payload = ByteBuffer.allocate(totalSize);
    // Worry about payload.order?
    payload.putInt(totalSize);
    payload.put(this.getValue());
    return payload.array();
  }

  public static int parseSize(byte[] payload) {
    return ByteBuffer.wrap(payload).getInt(0);
  }

  static boolean isHeaderComplete(byte[] payload) {
    return (payload.length >= HEADER_SIZE);
  }

  public static boolean isPayloadComplete(byte[] payload) {
    return (isHeaderComplete(payload) && payload.length >= parseSize(payload));
  }

  public static GetResponseObject deserialize(byte[] payload) {
    GetResponseObject obj = new GetResponseObject();
    if (!isHeaderComplete(payload)) {
      throw new IllegalArgumentException("GetResponseObject must have at least a " + HEADER_SIZE + " byte header");
    }
    ByteBuffer bufferedPayload = ByteBuffer.wrap(payload);
    int totalSize = bufferedPayload.getInt();
    if (payload.length < totalSize) {
      throw new IllegalArgumentException(
          "GetResponseObject indicates size of " + totalSize + " but only " + payload.length + " bytes available");
    }
    byte[] value = new byte[totalSize - HEADER_SIZE];
    bufferedPayload.get(value);
    obj.setValue(value);

    return obj;
  }
}
