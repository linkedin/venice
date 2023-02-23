package com.linkedin.venice.compression;

import com.linkedin.venice.utils.concurrent.CloseableThreadLocal;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;


class GzipPool implements AutoCloseable {
  @Override
  public void close() throws Exception {
    reusableObjectsThreadLocal.close();
  }

  private static class ReusableObjects implements AutoCloseable {
    final ByteArrayOutputStream stream = new ByteArrayOutputStream();
    final ReusableGzipOutputStream gzipOutputStream = new ReusableGzipOutputStream(stream);

    @Override
    public void close() throws Exception {
      stream.close();
      gzipOutputStream.close();
    }
  }

  private final CloseableThreadLocal<ReusableObjects> reusableObjectsThreadLocal =
      new CloseableThreadLocal(ReusableObjects::new);

  /**
   * Retrieves an {@link ReusableGzipOutputStream} for the given {@link OutputStream}. Instances are pooled per thread.
   *
   */
  public ReusableGzipOutputStream getReusableGzipOutputStream() {
    ReusableObjects reusableObjects = reusableObjectsThreadLocal.get();
    return reusableObjects.gzipOutputStream;
  }
}
