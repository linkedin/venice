package com.linkedin.alpini.io;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import javax.annotation.Nonnull;


/**
 * A simple @{link InputStream} implementation which is equivalent to
 * {@code /dev/zero} by acting as a source for a stream of NUL bytes.
 *
 * @author Antony T Curtis <acurtis@linkedin.com>
 */
public final class ZeroInputStream extends InputStream {
  private long _remaining;

  public ZeroInputStream(long remaining) {
    _remaining = remaining;
  }

  @Override
  public int read() throws IOException {
    if (_remaining == 0) {
      return -1;
    }
    _remaining--;
    return 0;
  }

  @Override
  public int read(@Nonnull byte[] b, int off, int len) throws IOException {
    if (len < 0 || off < 0 || off + len > b.length) {
      throw new ArrayIndexOutOfBoundsException();
    }
    int available = available();
    if (available <= 0) {
      return -1;
    }
    len = Math.min(len, available);
    Arrays.fill(b, off, off + len, (byte) 0);
    _remaining -= len;
    return len;
  }

  @Override
  public int read(@Nonnull byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public long skip(long n) throws IOException {
    if (n < 0) {
      throw new IllegalArgumentException();
    }
    if (_remaining < 0) {
      return n;
    }
    n = Math.min(n, _remaining);
    _remaining -= n;
    return n;
  }

  @Override
  public int available() throws IOException {
    return _remaining >= 0 && _remaining <= Integer.MAX_VALUE ? (int) _remaining : Integer.MAX_VALUE;
  }

  public long getBytesRemaining() {
    return _remaining;
  }
}
