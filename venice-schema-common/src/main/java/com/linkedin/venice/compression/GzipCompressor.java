package com.linkedin.venice.compression;

import com.linkedin.venice.utils.ByteUtils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.io.IOUtils;


public class GzipCompressor extends VeniceCompressor {
  public GzipCompressor() {
    super(CompressionStrategy.GZIP);
  }

  @Override
  public byte[] compress(byte[] data) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    GZIPOutputStream gos = new GZIPOutputStream(bos);
    gos.write(data);
    gos.close();
    return bos.toByteArray();
  }

  @Override
  public ByteBuffer compress(ByteBuffer data) throws IOException {
    return ByteBuffer.wrap(compress(ByteUtils.extractByteArray(data)));
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
    InputStream gis = decompress(new ByteArrayInputStream(data, offset, length));
    ByteBuffer decompressed = ByteBuffer.wrap(IOUtils.toByteArray(gis));
    gis.close();
    return decompressed;
  }

  @Override
  public InputStream decompress(InputStream inputStream) throws IOException {
    return new GZIPInputStream(inputStream);
  }
}
