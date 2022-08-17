package com.linkedin.venice.client.store.streaming;

import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import java.nio.ByteBuffer;


public class MultiGetResponseRecordV1ChunkedDeserializer
    extends ReadEnvelopeChunkedDeserializer<MultiGetResponseRecordV1> {
  @Override
  public ValueContainer<MultiGetResponseRecordV1> tryDeserializeRecord(int currentPos) throws NotEnoughBytesException {
    int currentOffset = currentPos;

    // keyIndex: int
    ValueContainer<Integer> keyIndex = tryReadInt(currentOffset);

    // value: ByteBuffer
    currentOffset += keyIndex.bytesUsed;
    ValueContainer<Integer> bytesMeta = readBytesMeta(currentOffset);
    int bytesStartOffset = currentOffset + bytesMeta.bytesUsed;
    int bytesLen = bytesMeta.value;

    // schemaId: int
    currentOffset += bytesMeta.bytesUsed + bytesMeta.value;
    ValueContainer<Integer> schemaId = tryReadInt(currentOffset);

    // To avoid unnecessary byte array copying, here will only do the byte copy after
    // verifying that schema id is available.
    byte[] value = readBytes(bytesStartOffset, bytesLen);

    int totalUsed = schemaId.bytesUsed + currentOffset - currentPos;
    MultiGetResponseRecordV1 record = new MultiGetResponseRecordV1();
    record.keyIndex = keyIndex.value;
    record.value = ByteBuffer.wrap(value);
    record.schemaId = schemaId.value;

    return new ValueContainer<>(record, totalUsed);
  }
}
