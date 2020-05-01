package com.linkedin.venice.compression;

import com.github.luben.zstd.ZstdCompressCtx;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import com.github.luben.zstd.ZstdInputStream;
import org.apache.commons.io.IOUtils;

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
}
