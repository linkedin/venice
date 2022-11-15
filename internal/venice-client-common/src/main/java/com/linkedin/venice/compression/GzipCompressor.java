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
    try (GZIPOutputStream gos = new GZIPOutputStream(bos)) {
      gos.write(data);
      gos.finish();
      return bos.toByteArray();
    }
  }

  @Override
  public ByteBuffer compress(ByteBuffer data, int startPositionOfOutput) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    for (int i = 0; i < startPositionOfOutput; i++) {
      bos.write(0);
    }
    try (GZIPOutputStream gos = new GZIPOutputStream(bos)) {
      if (data.hasArray()) {
        gos.write(data.array(), data.position(), data.remaining());
      } else {
        gos.write(ByteUtils.extractByteArray(data));
      }
      gos.finish();
      ByteBuffer output = ByteBuffer.wrap(bos.toByteArray());
      output.position(startPositionOfOutput);
      return output;
    }
  }

  @Override
  public ByteBuffer decompress(ByteBuffer data) throws IOException {
    return data.hasRemaining() ? decompress(data.array(), data.position(), data.remaining()) : data;
  }

  @Override
  public ByteBuffer decompress(byte[] data, int offset, int length) throws IOException {
    try (InputStream gis = decompress(new ByteArrayInputStream(data, offset, length))) {
      return ByteBuffer.wrap(IOUtils.toByteArray(gis));
    }
  }

  @Override
  public InputStream decompress(InputStream inputStream) throws IOException {
    return new GZIPInputStream(inputStream);
  }
}
