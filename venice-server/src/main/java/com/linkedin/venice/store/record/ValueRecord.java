package com.linkedin.venice.store.record;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.ByteUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * This class provides the following functionalities:
 * 1. Concatenate schema id and data array to produce a big binary array, which will be stored in DB;
 * 2. Parse the binary array stored in DB into schema id and data array;
 *
 * Right now, the concatenation part will allocate a new byte array and copy over schema id and data,
 * which might cause some GC issue since this operation will be triggered for every 'PUT'.
 * If this issue happens, we need to consider other ways to improve it:
 * 1. Maybe we can do the concatenation in VeniceWriter, which is being used by KafkaPushJob;
 * 2. Investigate whether DB can accept multiple binary arrays for 'PUT' operation;
 * 3. ...
 *
 * For deserialization part, this class is using Netty SlicedByteBuf, which will be backed by the same
 * byte array, and intelligently takes care of the offset.
 *
 */
public class ValueRecord {
  private final int schemaId;
  private final ByteBuf data;
  private byte[] serializedArr;

  private ValueRecord(int schemaId, byte[] data) {
    this.schemaId = schemaId;
    this.data = Unpooled.wrappedBuffer(data);
  }

  private ValueRecord(byte[] combinedData) {
    int dataLen = combinedData.length;
    if (dataLen < ByteUtils.SIZE_OF_INT) {
      throw new VeniceException("Invalid combined data array, the length should be bigger than " + ByteUtils.SIZE_OF_INT);
    }
    this.schemaId = ByteUtils.readInt(combinedData, 0);
    // Unpooled.wrappedBuffer will return an SlicedByteBuf backed by the original byte array
    this.data = Unpooled.wrappedBuffer(combinedData, ByteUtils.SIZE_OF_INT, dataLen - ByteUtils.SIZE_OF_INT);
    this.serializedArr = combinedData;
  }

  public static ValueRecord create(int schemaId, byte[] data) {
    return new ValueRecord(schemaId, data);
  }

  public static ValueRecord parseAndCreate(byte[] combinedData) {
    return new ValueRecord(combinedData);
  }

  public int getSchemaId() {
    return schemaId;
  }

  public ByteBuf getData() {
    return data;
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
    if (null != serializedArr) {
      return serializedArr;
    }
    byte[] dataArr = data.array();

    serializedArr = new byte[ByteUtils.SIZE_OF_INT + dataArr.length];
    ByteUtils.writeInt(serializedArr, schemaId, 0);
    System.arraycopy(dataArr, 0, serializedArr, ByteUtils.SIZE_OF_INT, dataArr.length);

    return serializedArr;
  }
}
