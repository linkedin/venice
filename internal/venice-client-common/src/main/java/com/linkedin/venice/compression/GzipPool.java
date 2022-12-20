package com.linkedin.venice.compression;

import com.linkedin.venice.utils.concurrent.CloseableThreadLocal;
import java.io.IOException;
import java.io.OutputStream;


abstract class GzipPool {
  private static class ReusableObjects implements AutoCloseable {
    final SettableOutputStream stream = new SettableOutputStream();
    final ReusableGzipOutputStream gzipOutputStream = new ReusableGzipOutputStream(stream);

    @Override
    public void close() throws Exception {
      stream.close();
    }
  }

  private static final CloseableThreadLocal<ReusableObjects> reusableObjectsThreadLocal =
      new CloseableThreadLocal(ReusableObjects::new);

  /**
   * Retrieves an {@link ReusableGzipOutputStream} for the given {@link OutputStream}. Instances are pooled per thread.
   *
   * @param target
   * @return
   */
  public static ReusableGzipOutputStream forStream(OutputStream target) {
    reusableObjectsThreadLocal.get().stream.target = target;
    return reusableObjectsThreadLocal.get().gzipOutputStream;
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
