package com.linkedin.venice.compression;

import static com.linkedin.venice.utils.ByteUtils.BYTES_PER_KB;
import static com.linkedin.venice.utils.ByteUtils.BYTES_PER_MB;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdDictCompress;
import com.github.luben.zstd.ZstdDictDecompress;
import com.github.luben.zstd.ZstdDictTrainer;
import com.github.luben.zstd.ZstdInputStream;
import com.linkedin.venice.compression.protocol.FakeCompressingSchema;
import com.linkedin.venice.serializer.AvroSerializer;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;


public class ZstdWithDictCompressor extends VeniceCompressor {
  private ZstdDictCompress compressorDictionary;
  private ZstdDictDecompress decompressorDictionary;

  public ZstdWithDictCompressor(final byte[] dictionary, int level) {
    super(CompressionStrategy.ZSTD_WITH_DICT);
    this.compressorDictionary = new ZstdDictCompress(dictionary, level);
    this.decompressorDictionary = new ZstdDictDecompress(dictionary);
  }

  @Override
  public byte[] compress(byte[] data) {
    return Zstd.compress(data, compressorDictionary);
  }

  @Override
  public ByteBuffer compress(ByteBuffer data) {
    return Zstd.compress(data, compressorDictionary);
  }

  @Override
  public ByteBuffer decompress(ByteBuffer data) throws IOException {
    return data.hasRemaining() ? decompress(data.array(), data.position(), data.remaining()) : data;
  }

  @Override
  public ByteBuffer decompress(byte[] data, int offset, int length) throws IOException {
    // TODO: Investigate using ZstdDirectBufferDecompressingStream instead of copying data.
    try (InputStream zis = decompress(new ByteArrayInputStream(data, offset, length))) {
      return ByteBuffer.wrap(IOUtils.toByteArray(zis));
    }
  }

  @Override
  public InputStream decompress(InputStream inputStream) throws IOException {
    return new ZstdInputStream(inputStream).setDict(this.decompressorDictionary);
  }

  @Override
  public void close() throws IOException {
    compressorDictionary.close();
    decompressorDictionary.close();
  }

  /**
   * Build a dictionary based on synthetic data.  Used for empty push where there is no
   * available dictionary to retrieve from a push job.
   *
   * @return a zstd compression dictionary trained on small amount of avro data
   */
  public static byte[] buildDictionaryOnSyntheticAvroData() {
    AvroSerializer serializer = new AvroSerializer<Object>(FakeCompressingSchema.getClassSchema());
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
