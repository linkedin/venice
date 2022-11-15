package com.linkedin.venice.compression;

import static com.linkedin.venice.utils.ByteUtils.BYTES_PER_KB;
import static com.linkedin.venice.utils.ByteUtils.BYTES_PER_MB;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDecompressCtx;
import com.github.luben.zstd.ZstdDictCompress;
import com.github.luben.zstd.ZstdDictDecompress;
import com.github.luben.zstd.ZstdDictTrainer;
import com.github.luben.zstd.ZstdException;
import com.github.luben.zstd.ZstdInputStream;
import com.linkedin.venice.compression.protocol.FakeCompressingSchema;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.utils.concurrent.CloseableThreadLocal;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;


public class ZstdWithDictCompressor extends VeniceCompressor {
  private final CloseableThreadLocal<ZstdCompressCtx> compressor;
  private final CloseableThreadLocal<ZstdDecompressCtx> decompressor;
  private final Lazy<ZstdDictCompress> zstdDictCompress;
  private final Lazy<ZstdDictDecompress> zstdDictDecompress;

  public ZstdWithDictCompressor(final byte[] dictionary, int level) {
    super(CompressionStrategy.ZSTD_WITH_DICT);
    this.zstdDictCompress = Lazy.of(() -> new ZstdDictCompress(dictionary, level));
    this.zstdDictDecompress = Lazy.of(() -> new ZstdDictDecompress(dictionary));
    this.compressor =
        new CloseableThreadLocal<>(() -> new ZstdCompressCtx().loadDict(zstdDictCompress.get()).setLevel(level));
    this.decompressor = new CloseableThreadLocal<>(() -> new ZstdDecompressCtx().loadDict(zstdDictDecompress.get()));
  }

  @Override
  public byte[] compress(byte[] data) {
    return compressor.get().compress(data);
  }

  @Override
  public ByteBuffer compress(ByteBuffer data, int startPositionOfOutput) throws IOException {
    long maxDstSize = Zstd.compressBound(data.remaining());
    if (maxDstSize > Integer.MAX_VALUE) {
      throw new ZstdException(Zstd.errGeneric(), "Max output size is greater than Integer.MAX_VALUE");
    }
    int sizeOfOutput = (int) maxDstSize + startPositionOfOutput;
    if (data.isDirect()) {
      // TODO: It might be a decent refactor to add a pool of direct memory buffers so as to always leverage the this
      // interface and copy the results of the compression back into the passed in ByteBuffer. That would avoid
      // some of the data copy going on here.
      ByteBuffer output = ByteBuffer.allocateDirect(sizeOfOutput);
      output.position(startPositionOfOutput);
      data.mark();
      int size = compressor.get().compress(output, data);
      output.position(startPositionOfOutput);
      output.limit(startPositionOfOutput + size);
      data.reset();
      return output;
    } else {
      byte[] dst = new byte[sizeOfOutput];
      int size = compressor.get()
          .compressByteArray(
              dst,
              startPositionOfOutput,
              (int) maxDstSize,
              data.array(),
              data.position(),
              data.remaining());
      return ByteBuffer.wrap(dst, startPositionOfOutput, size);
    }
  }

  @Override
  public ByteBuffer decompress(ByteBuffer data) throws IOException {
    if (data.hasRemaining()) {
      if (data.isDirect()) {
        int expectedSize = validateExpectedDecompressedSize(Zstd.decompressedSize(data));
        ByteBuffer output = ByteBuffer.allocateDirect(expectedSize);
        int actualSize = decompressor.get().decompress(output, data);
        output.position(0);
        validateActualDecompressedSize(actualSize, expectedSize);
        return output;
      } else {
        return decompress(data.array(), data.position(), data.remaining());
      }
    } else {
      return data;
    }
  }

  @Override
  public ByteBuffer decompress(byte[] data, int offset, int length) throws IOException {
    int expectedSize = validateExpectedDecompressedSize(Zstd.decompressedSize(data, offset, length));
    ByteBuffer returnedData = ByteBuffer.allocate(expectedSize);
    int actualSize = decompressor.get()
        .decompressByteArray(
            returnedData.array(),
            returnedData.position(),
            returnedData.remaining(),
            data,
            offset,
            length);
    validateActualDecompressedSize(actualSize, expectedSize);
    returnedData.position(0);
    return returnedData;
  }

  @Override
  public InputStream decompress(InputStream inputStream) throws IOException {
    return new ZstdInputStream(inputStream).setDict(this.zstdDictDecompress.get());
  }

  @Override
  public void close() throws IOException {
    this.zstdDictCompress.ifPresent(ZstdDictCompress::close);
    this.zstdDictDecompress.ifPresent(ZstdDictDecompress::close);
    this.compressor.close();
    this.decompressor.close();
  }

  private int validateExpectedDecompressedSize(long expectedSize) {
    if (expectedSize == 0) {
      throw new IllegalStateException("The size of the compressed payload cannot be known.");
    } else if (expectedSize > Integer.MAX_VALUE) {
      throw new IllegalStateException("The size of the compressed payload is > " + Integer.MAX_VALUE);
    }
    return (int) expectedSize;
  }

  private void validateActualDecompressedSize(int actual, int expected) {
    if (actual != expected) {
      throw new IllegalStateException(
          "The decompressed payload size (" + actual + ") is not as expected (" + expected + ").");
    }
  }

  /**
   * Build a dictionary based on synthetic data.  Used for empty push where there is no
   * available dictionary to retrieve from a push job.
   *
   * @return a zstd compression dictionary trained on small amount of avro data
   */
  public static byte[] buildDictionaryOnSyntheticAvroData() {
    AvroSerializer<Object> serializer = new AvroSerializer<>(FakeCompressingSchema.getClassSchema());
    // Insert fake records. We need to generate at least some data for the
    // dictionary as failing to do so will result in the library throwing
    // an exception (it's only able to generate a dictionary with a minimum threshold of test data).
    // So we train on a small amount of basic avro data to
    // at least gain some partial effectiveness.
    List<byte[]> values = new ArrayList<>(50);
    for (int i = 0; i < 50; ++i) {
      GenericRecord value = new GenericData.Record(FakeCompressingSchema.getClassSchema());
      value.put("id", i);
      String name = i + "_name";
      value.put("name", name);
      values.add(i, serializer.serialize(value, AvroSerializer.REUSE.get()));
    }
    ZstdDictTrainer trainer = new ZstdDictTrainer(200 * BYTES_PER_MB, 100 * BYTES_PER_KB);
    for (byte[] value: values) {
      trainer.addSample(value);
    }
    return trainer.trainSamples();
  }
}
