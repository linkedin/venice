package com.linkedin.alpini.io;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;
import javax.annotation.Nonnull;


/**
 * @author Antony T Curtis <acurtis@linkedin.com>
 */
public class MeteredOutputStream extends FilterOutputStream {
  private long _bytes;

  /**
   * Creates an output stream filter built on top of the specified
   * underlying output stream.
   *
   * @param out the underlying output stream.
   */
  public MeteredOutputStream(@Nonnull OutputStream out) {
    super(Objects.requireNonNull(out));
  }

  public long getBytesWritten() {
    return _bytes;
  }

  /**
   * Writes the specified <code>byte</code> to this output stream.
   *
   * @param b the <code>byte</code>.
   * @throws java.io.IOException if an I/O error occurs.
   */
  @Override
  public void write(int b) throws IOException {
    out.write(b);
    _bytes++;
  }

  /**
   * Writes <code>b.length</code> bytes to this output stream.
   *
   * @param b the data to be written.
   * @throws java.io.IOException if an I/O error occurs.
   */
  @Override
  public void write(@Nonnull byte[] b) throws IOException {
    out.write(b);
    _bytes += b.length;
  }

  /**
   * Writes <code>len</code> bytes from the specified
   * <code>byte</code> array starting at offset <code>off</code> to
   * this output stream.
  
   * @param b   the data.
   * @param off the start offset in the data.
   * @param len the number of bytes to write.
   * @throws java.io.IOException if an I/O error occurs.
   */
  @Override
  public void write(@Nonnull byte[] b, int off, int len) throws IOException {
    out.write(b, off, len);
    _bytes += len;
  }
}
