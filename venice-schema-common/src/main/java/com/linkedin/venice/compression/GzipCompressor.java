package com.linkedin.venice.compression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.io.IOUtils;


public class GzipCompressor extends VeniceCompressor {
  public GzipCompressor() {
    super(CompressionStrategy.GZIP);
  }

  public byte[] compress(byte[] data) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    GZIPOutputStream gos = new GZIPOutputStream(bos);
    gos.write(data);
    gos.close();
    return bos.toByteArray();
  }

  public byte[] decompress(byte[] data) throws IOException {
    GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(data));
    byte[] decompressed = IOUtils.toByteArray(gis);
    gis.close();
    return decompressed;
  }
}
