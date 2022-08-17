package com.linkedin.venice.client.store.streaming;

import com.linkedin.venice.compute.protocol.response.ComputeResponseRecordV1;
import java.nio.ByteBuffer;


public class ComputeResponseRecordV1ChunkedDeserializer
    extends ReadEnvelopeChunkedDeserializer<ComputeResponseRecordV1> {
  @Override
  public ValueContainer<ComputeResponseRecordV1> tryDeserializeRecord(int currentPos) throws NotEnoughBytesException {
    int currentOffset = currentPos;

    // keyIndex: int
    ValueContainer<Integer> keyIndex = tryReadInt(currentOffset);

    // value: ByteBuffer
    currentOffset += keyIndex.bytesUsed;
    ValueContainer<Integer> bytesMeta = readBytesMeta(currentOffset);
    int bytesStartOffset = currentOffset + bytesMeta.bytesUsed;
    int bytesLen = bytesMeta.value;

    byte[] value = readBytes(bytesStartOffset, bytesLen);

    int totalUsed = bytesStartOffset + bytesLen - currentPos;
    ComputeResponseRecordV1 record = new ComputeResponseRecordV1();
    record.keyIndex = keyIndex.value;
    record.value = ByteBuffer.wrap(value);

    return new ValueContainer<>(record, totalUsed);
  }
}
