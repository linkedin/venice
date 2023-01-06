package com.linkedin.davinci.store.record;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.ByteUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;


/**
 * This class provides the following functionalities:
 * 1. Concatenate schema id and data array to produce a big binary array, which will be stored in DB;
 * 2. Parse the binary array stored in DB into schema id and data array;
 *
 * Right now, the concatenation part will allocate a new byte array and copy over schema id and data,
 * which might cause some GC issue since this operation will be triggered for every 'PUT'.
 * If this issue happens, we need to consider other ways to improve it:
 * 1. Maybe we can do the concatenation in VeniceWriter, which is being used by VenicePushJob;
 * 2. Investigate whether DB can accept multiple binary arrays for 'PUT' operation;
 * 3. ...
 *
 * For deserialization part, this class is using Netty SlicedByteBuf, which will be backed by the same
 * byte array, and intelligently takes care of the offset.
 *
 */
public class ValueRecord {
  public static final int SCHEMA_HEADER_LENGTH = ByteUtils.SIZE_OF_INT;

  private final int schemaId;
  private final ByteBuf data;
  private final int dataSize;
  private byte[] serializedArr;

  private ValueRecord(int schemaId, byte[] data) {
    this(schemaId, Unpooled.wrappedBuffer(data));
  }

  private ValueRecord(int schemaId, ByteBuf data) {
    this.schemaId = schemaId;
    this.data = data;
    this.dataSize = data.readableBytes();
  }

  private ValueRecord(byte[] combinedData) {
    int dataLen = combinedData.length;
    if (dataLen < SCHEMA_HEADER_LENGTH) {
      throw new VeniceException(
          "Invalid combined data array, the length should be bigger than " + SCHEMA_HEADER_LENGTH);
    }
    this.dataSize = combinedData.length - SCHEMA_HEADER_LENGTH;
    this.schemaId = parseSchemaId(combinedData);
    this.data = parseDataAsByteBuf(combinedData);
    this.serializedArr = combinedData;
  }

  public static ValueRecord create(int schemaId, byte[] data) {
    return new ValueRecord(schemaId, data);
  }

  public static ValueRecord create(int schemaId, ByteBuf data) {
    return new ValueRecord(schemaId, data);
  }

  public static ValueRecord parseAndCreate(byte[] combinedData) {
    return new ValueRecord(combinedData);
  }

  public static int parseSchemaId(byte[] combinedData) {
    return ByteUtils.readInt(combinedData, 0);
  }

  public static ByteBuf parseDataAsByteBuf(byte[] combinedData) {
    // Unpooled.wrappedBuffer will return an SlicedByteBuf backed by the original byte array
    return Unpooled.wrappedBuffer(combinedData, SCHEMA_HEADER_LENGTH, combinedData.length - SCHEMA_HEADER_LENGTH);
  }

  public static ByteBuffer parseDataAsNIOByteBuffer(byte[] combinedData) {
    return ByteBuffer.wrap(combinedData, SCHEMA_HEADER_LENGTH, combinedData.length - SCHEMA_HEADER_LENGTH);
  }

  public int getSchemaId() {
    return schemaId;
  }

  public ByteBuf getData() {
    return data;
  }

  public int getDataSize() {
    return dataSize;
  }

  // This function normally should only be used in testing,
  // Since it will create a copy every time.
  public byte[] getDataInBytes() {
    byte[] dataInBytes = new byte[data.capacity()];
    data.getBytes(0, dataInBytes);

    return dataInBytes;
  }

  // TODO: If we meet GC issue later, we need to tune this part,
  // since it will allocate a new byte array and copy over the data.
  public byte[] serialize() {
    if (serializedArr != null) {
      return serializedArr;
    }
    serializedArr = new byte[SCHEMA_HEADER_LENGTH + dataSize];
    ByteUtils.writeInt(serializedArr, schemaId, 0);
    data.getBytes(data.readerIndex(), serializedArr, SCHEMA_HEADER_LENGTH, data.readableBytes());
    return serializedArr;
  }
}
