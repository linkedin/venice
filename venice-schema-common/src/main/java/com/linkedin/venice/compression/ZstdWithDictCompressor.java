package com.linkedin.venice.compression;

import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDictTrainer;
import com.linkedin.venice.compression.protocol.FakeCompressingSchema;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.utils.ByteUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import com.github.luben.zstd.ZstdInputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;

import static com.linkedin.venice.utils.ByteUtils.*;


public class ZstdWithDictCompressor extends VeniceCompressor {
  private ZstdCompressCtx compressor;
  private byte[] dictionary;

  public ZstdWithDictCompressor(final byte[] dictionary, int level) {
    super(CompressionStrategy.ZSTD_WITH_DICT);
    this.dictionary = dictionary;
    compressor = new ZstdCompressCtx().loadDict(this.dictionary).setLevel(level);
  }

  @Override
  public byte[] compress(byte[] data) {
    return compressor.compress(data);
  }

  @Override
  public ByteBuffer compress(ByteBuffer data) {
    if(data.isDirect()) {
      // TODO: It might be a decent refactor to add a pool of direct memory buffers so as to always leverage the this
      // interface and copy the results of the compression back into the passed in ByteBuffer.  That would avoid
      // some of the data copy going on here.
      return compressor.compress(data);
    } else {
      return ByteBuffer.wrap(compressor.compress(ByteUtils.extractByteArray(data)));
    }
  }

  @Override
  public ByteBuffer decompress(ByteBuffer data) throws IOException {
    if (0 == data.remaining()) {
      return data;
    }
    return decompress(data.array(), data.position(), data.remaining());
  }

  @Override
  public ByteBuffer decompress(byte[] data, int offset, int length) throws IOException {
    // TODO: Investigate using ZstdDirectBufferDecompressingStream instead of copying data.
    try (InputStream zis = decompress(new ByteArrayInputStream(data, offset, length))) {
      ByteBuffer decompressed = ByteBuffer.wrap(IOUtils.toByteArray(zis));
      return decompressed;
    }
  }

  @Override
  public InputStream decompress(InputStream inputStream) throws IOException {
    return new ZstdInputStream(inputStream).setDict(this.dictionary);
  }

  @Override
  public void close() throws IOException {
    compressor.close();
  }

  /**
   * Build a dictionary based on synthetic data.  Used for empty push where there is no
   * available dictionary to retrieve from a push job.
   *
   * @return a zstd compression dictionary trained on small amount of avro data
   */
  public static byte[] buildDictionaryOnSyntheticAvroData() {
    AvroSerializer serializer = new AvroSerializer<Object>(FakeCompressingSchema.getClassSchema());
    // Insert fake records.  We need to generate at least some data for the
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
    for (byte[] value : values) {
      trainer.addSample(value);
    }
    return trainer.trainSamples();
  }
}
