package com.linkedin.venice.client.store.streaming;

import com.linkedin.data.ByteString;
import com.linkedin.venice.exceptions.VeniceException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.util.LinkedList;
import java.util.List;
import org.apache.avro.io.BinaryDecoder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class provides support to deserialize customized records even the input doesn't contain
 * the full record.
 *
 * The user could use {@link #write(ByteBuffer)} to keep writing more data, and the user can use
 * {@link #consume()} to consume all the available records so far.
 * All the records can only be consumed once, and after the consumption, the underlying byte array
 * might be freed to reduce GC.
 *
 * @param <V>
 */
public abstract class ReadEnvelopeChunkedDeserializer<V> {
  private static final Logger LOGGER = LogManager.getLogger(ReadEnvelopeChunkedDeserializer.class);

  // All available deserialized bytes
  private final LinkedList<BytesContainer> buffers = new LinkedList<>();
  // Current offset to deserialize
  private int currentOffset = 0;
  private int totalBytes = 0;
  private List<V> currentAvailableRecords = new LinkedList<>();

  private static class BytesContainer {
    final ByteBuffer bytes;
    // inclusive offset
    int globalStartOffset;
    // inclusive offset
    int globalEndOffset;

    public BytesContainer(ByteBuffer bytes, int globalStartOffset, int globalEndOffset) {
      this.bytes = bytes;
      this.globalStartOffset = globalStartOffset;
      this.globalEndOffset = globalEndOffset;
    }
  }

  /**
   * Exception when the deserialization hits partial record.
   */
  public static class NotEnoughBytesException extends Exception {
  }

  public static final NotEnoughBytesException NOT_ENOUGH_BYTES_EXCEPTION = new NotEnoughBytesException();

  public List<V> consume() {
    tryDeserializeRecords();
    if (currentAvailableRecords.isEmpty()) {
      return currentAvailableRecords;
    }
    List<V> returnedRecords = currentAvailableRecords;
    currentAvailableRecords = new LinkedList<>();

    return returnedRecords;
  }

  public void write(ByteBuffer bytes) {
    if (!bytes.hasRemaining()) {
      return;
    }
    buffers.add(new BytesContainer(bytes, totalBytes, totalBytes + bytes.remaining() - 1));
    totalBytes += bytes.remaining();
  }

  private void tryDeserializeRecords() {
    while (true) {
      try {
        ValueContainer<V> record = tryDeserializeRecord(currentOffset);
        currentAvailableRecords.add(record.value);
        currentOffset += record.bytesUsed;
        // Remove useless chunks
        int removedBytes = 0;
        while (buffers.peek() != null && buffers.peek().globalEndOffset < currentOffset) {
          removedBytes = buffers.pop().globalEndOffset + 1;
        }
        final int finalRemovedBytes = removedBytes;
        buffers.forEach(buffer -> {
          buffer.globalStartOffset -= finalRemovedBytes;
          buffer.globalEndOffset -= finalRemovedBytes;
        });
        totalBytes -= removedBytes;
        currentOffset -= removedBytes;
      } catch (NotEnoughBytesException e) {
        // Hit partial record
        break;
      }
    }
  }

  /**
   * This class contains two fields:
   * 1. value: deserialized value;
   * 2. bytesUsed: byte length for the original serialized value;
   * @param <V>
   */
  protected static class ValueContainer<V> {
    V value;
    int bytesUsed;

    ValueContainer(V value, int bytesUsed) {
      this.value = value;
      this.bytesUsed = bytesUsed;
    }
  }

  /**
   * All the derived class will implement this function to provide the customized logic to deserialize
   * a record, and when hitting partial record, it should throw {@link NotEnoughBytesException}.
   *
   * @param currentPos : current global offset to deserialize
   * @return
   * @throws NotEnoughBytesException
   */
  protected abstract ValueContainer<V> tryDeserializeRecord(int currentPos) throws NotEnoughBytesException;

  /**
   * Utility method to retrieve a byte with the given offset.
   *
   * @param offset
   * @return
   * @throws NotEnoughBytesException
   */
  private byte getByte(int offset) throws NotEnoughBytesException {
    if (offset >= totalBytes) {
      throw NOT_ENOUGH_BYTES_EXCEPTION;
    }
    for (BytesContainer bytesContainer: buffers) {
      if (offset >= bytesContainer.globalStartOffset && offset <= bytesContainer.globalEndOffset) {
        return bytesContainer.bytes.get(offset - bytesContainer.globalStartOffset + bytesContainer.bytes.position());
      }
    }
    throw NOT_ENOUGH_BYTES_EXCEPTION;
  }

  /**
   * The following implementation is equivalent to {@link BinaryDecoder#readIndex()}
   */
  protected ValueContainer<Integer> tryReadInt(int currentOffset) throws NotEnoughBytesException {
    int len = 1;
    int b = getByte(currentOffset) & 0xff;
    int n = b & 0x7f;
    if (b > 0x7f) {
      b = getByte(currentOffset + len++) & 0xff;
      n ^= (b & 0x7f) << 7;
      if (b > 0x7f) {
        b = getByte(currentOffset + len++) & 0xff;
        n ^= (b & 0x7f) << 14;
        if (b > 0x7f) {
          b = getByte(currentOffset + len++) & 0xff;
          n ^= (b & 0x7f) << 21;
          if (b > 0x7f) {
            b = getByte(currentOffset + len++) & 0xff;
            n ^= (b & 0x7f) << 28;
            if (b > 0x7f) {
              throw new VeniceException("Invalid int encoding");
            }
          }
        }
      }
    }
    int intRes = (n >>> 1) ^ -(n & 1); // back to two's-complement
    return new ValueContainer<>(intRes, len);
  }

  /**
   * Utility method to read bytes array metadata, which contains the byte length and the byte length
   * to store the serialized byte-length.
   *
   * The reason to introduce this method is to avoid the unnecessary byte array copy until current record
   * is a full record.
   *
   * @param currentOffset
   * @return
   * @throws NotEnoughBytesException
   */
  protected ValueContainer<Integer> readBytesMeta(int currentOffset) throws NotEnoughBytesException {
    // try read bytes length first
    ValueContainer<Integer> intValueContainer = tryReadInt(currentOffset);
    // check whether current buffers contain all the bytes
    int bytesStartOffset = currentOffset + intValueContainer.bytesUsed;
    int bytesLen = intValueContainer.value;
    if (bytesLen == 0) {
      return new ValueContainer(0, intValueContainer.bytesUsed);
    }
    // inclusive
    int bytesEndOffset = bytesStartOffset + bytesLen - 1;
    if (bytesEndOffset >= totalBytes) {
      throw NOT_ENOUGH_BYTES_EXCEPTION;
    }

    return new ValueContainer<>(bytesLen, intValueContainer.bytesUsed);
  }

  /**
   * Utility method to read byte arrays.
   *
   * We could not reuse the underlying byte array even the requested bytes are fully contained by one single
   * {@link BytesContainer} since the internal {@link ByteBuffer} is read-only since {@link ByteBuffer#array()}
   * will throw {@link ReadOnlyBufferException}.
   *
   * You could refer to {@link ByteString#asByteBuffer()} to find more details, which is being
   * used by {@link com.linkedin.venice.client.store.transport.D2TransportClient}.
   *
   * @param bytesStartOffset
   * @param bytesLen
   * @return
   * @throws NotEnoughBytesException
   */
  protected byte[] readBytes(int bytesStartOffset, int bytesLen) throws NotEnoughBytesException {
    // inclusive
    int bytesEndOffset = bytesStartOffset + bytesLen - 1;
    if (bytesEndOffset >= totalBytes) {
      throw NOT_ENOUGH_BYTES_EXCEPTION;
    }
    // read bytes
    byte[] value = new byte[bytesLen];
    int copiedLen = 0;
    for (BytesContainer bytesContainer: buffers) {
      int currentStart = bytesContainer.globalStartOffset;
      int currentEnd = bytesContainer.globalEndOffset;

      if (bytesStartOffset > currentEnd) {
        continue;
      }
      if (bytesEndOffset < currentStart) {
        break;
      }

      int actualStart = Math.max(currentStart, bytesStartOffset);
      int actualEnd = Math.min(currentEnd, bytesEndOffset);
      int copiedLenFromCurrentBuffer = actualEnd - actualStart + 1;

      ByteBuffer currentBuffer = bytesContainer.bytes;
      currentBuffer.mark();
      currentBuffer.position(actualStart - currentStart + currentBuffer.position());
      currentBuffer.get(value, copiedLen, copiedLenFromCurrentBuffer);
      currentBuffer.reset();
      copiedLen += copiedLenFromCurrentBuffer;
    }

    return value;
  }
}
