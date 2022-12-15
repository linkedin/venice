package com.linkedin.venice.compression;

import java.io.IOException;
import java.io.OutputStream;


abstract class GzipPool {
  private static final ThreadLocal<SettableOutputStream> streams =
      ThreadLocal.withInitial(() -> new SettableOutputStream());

  private static final ThreadLocal<ReusableGzipOutputStream> zipStreams =
      ThreadLocal.withInitial(() -> new ReusableGzipOutputStream(streams.get()));

  /**
   * Retrieves an {@link ReusableGzipOutputStream} for the given {@link OutputStream}. Instances are pooled per thread.
   *
   * @param target
   * @return
   */
  public static ReusableGzipOutputStream forStream(OutputStream target) {
    streams.get().target = target;
    return zipStreams.get();
  }

  static class SettableOutputStream extends OutputStream {
    private OutputStream target;

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      target.write(b, off, len);
    }

    @Override
    public void write(byte[] b) throws IOException {
      target.write(b);
    }

    @Override
    public void write(int b) throws IOException {
      target.write((byte) b);
    }
  }
}
