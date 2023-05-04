package com.linkedin.venice.compression;

import com.linkedin.venice.io.ZeroCopyByteArrayOutputStream;
import com.linkedin.venice.utils.ByteUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class GzipCompressor extends VeniceCompressor {
  private static final Logger LOGGER = LogManager.getLogger(GzipCompressor.class);

  private final GzipPool gzipPool;

  public GzipCompressor() {
    super(CompressionStrategy.GZIP);
    this.gzipPool = new GzipPool();
  }

  @Override
  public byte[] compress(byte[] data) throws IOException {
    ReusableGzipOutputStream out = gzipPool.getReusableGzipOutputStream();
    try {
      out.writeHeader();
      out.write(data);
      out.finish();
      return out.toByteArray();
    } finally {
      out.reset();
    }
  }

  @Override
  public void close() throws IOException {
    try {
      gzipPool.close();
    } catch (Exception e) {
      // ignore the failure
      LOGGER.info("Failed to close gzipPool. Suppress the exception.", e);
    }
  }

  @Override
  public ByteBuffer compress(ByteBuffer data, int startPositionOfOutput) throws IOException {
    /**
     * N.B.: We initialize the size of buffer in this output stream at the size of the deflated payload, which is not
     * ideal, but not necessarily bad either. The assumption is that GZIP usually doesn't compress our payloads that
     * much, maybe shaving off only 10-20%, and so the excess capacity in the buffer will only be this much. In return
     * for the cost of this excess capacity, we maximize the chance that there will be no resizing/copy of the buffer.
     * We say "maximize" and not "eliminate" because in certain cases GZIP can actually bloat the payload, and in those
     * cases there would still be at least one copy.
     */
    ZeroCopyByteArrayOutputStream outputStream = new ZeroCopyByteArrayOutputStream(data.remaining());
    for (int i = 0; i < startPositionOfOutput; i++) {
      outputStream.write(0);
    }
    try (GZIPOutputStream gos = new GZIPOutputStream(outputStream)) {
      if (data.hasArray()) {
        gos.write(data.array(), data.position(), data.remaining());
      } else {
        gos.write(ByteUtils.extractByteArray(data));
      }
      gos.finish();
      ByteBuffer output = outputStream.toByteBuffer();
      output.position(startPositionOfOutput);
      return output;
    }
  }

  @Override
  public ByteBuffer decompress(ByteBuffer data) throws IOException {
    if (data.hasRemaining()) {
      if (data.hasArray()) {
        return decompress(data.array(), data.position(), data.remaining());
      } else if (data.isDirect()) {
        return decompress(ByteUtils.extractByteArray(data), 0, data.remaining());
      } else {
        throw new IllegalArgumentException("The passed in ByteBuffer must be either direct or be backed by an array!");
      }
    } else {
      return data;
    }
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

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    return o != null && o instanceof GzipCompressor;
  }
}
