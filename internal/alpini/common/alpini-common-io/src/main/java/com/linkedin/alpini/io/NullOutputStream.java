package com.linkedin.alpini.io;

import java.io.IOException;
import java.io.OutputStream;


/**
 * A simple @{link OutputStream} implementation which is equivalent to
 * {@code /dev/null} by acting as a sink for bytes.
 *
 * @author Antony T Curtis <acurtis@linkedin.com>
 */
public final class NullOutputStream extends OutputStream {
  private long _bytesWritten = 0;

  @Override
  public void write(int b) throws IOException {
    _bytesWritten++;
  }

  @Override
  public void write(byte[] b) throws IOException {
    _bytesWritten += b.length;
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    _bytesWritten += len;
  }

  public long getBytesWritten() {
    return _bytesWritten;
  }
}
