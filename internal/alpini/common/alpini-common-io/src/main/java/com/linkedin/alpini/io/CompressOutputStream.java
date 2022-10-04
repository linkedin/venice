package com.linkedin.alpini.io;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;
import javax.annotation.Nonnull;
import javax.annotation.WillCloseWhenClosed;


/**
 * BackupOutputStream encapsulates an GZip compressor and provides methods
 * to allow other threads to wait upon the completion of the file.
 * However, when an exception occurs, the OutputStream is closed and the
 * GZip compressor is not flushed, which should cut off the trailing footer
 * leaving an invalid gzipped file.
 *
 * @author Antony T Curtis <acurtis@linkedin.com>
 */
public final class CompressOutputStream extends FilterOutputStream {
  private final CountDownLatch _sync = new CountDownLatch(1);
  private final OutputStream _destination;
  private volatile IOException _exception;
  private boolean _closed;

  /**
   * Creates a compressed backup output stream built on top of the specified
   * underlying output stream.
   *
   * @param compressionLevel the compression level requested.
   * @param executor Thread pool for the parallel compressor.
   * @param numThreads requested compression parallelism.
   * @param out the underlying output stream to be assigned to
   *            the field <tt>this.out</tt> for later use, or
   *            <code>null</code> if this instance is to be
   *            created without an underlying stream.
   */
  public CompressOutputStream(
      final int compressionLevel,
      ExecutorService executor,
      int numThreads,
      @Nonnull @WillCloseWhenClosed OutputStream out) throws IOException {
    super(
        numThreads > 1
            ? new PigzOutputStream(compressionLevel, executor, numThreads, out)
            : new GZIPOutputStream(out, PigzOutputStream.DEFAULT_BLOCK_SIZE) {
              {
                def.setLevel(compressionLevel);
              }
            });
    _destination = out;
  }

  private void test() throws IOException {
    if (_exception != null) {
      throw _exception;
    }
  }

  /**
   * Writes the specified <code>byte</code> to this output stream.
   *
   * @param b the <code>byte</code>.
   * @throws java.io.IOException if an I/O error occurs.
   */
  @Override
  public void write(int b) throws IOException {
    test();
    try {
      out.write(b);
    } catch (IOException e) {
      throw setException(e);
    }
  }

  /**
   * Writes <code>b.length</code> bytes to this output stream.
   *
   * @param b the data to be written.
   * @throws java.io.IOException if an I/O error occurs.
   */
  @Override
  public void write(@Nonnull byte[] b) throws IOException {
    test();
    try {
      out.write(b);
    } catch (IOException e) {
      throw setException(e);
    }
  }

  /**
   * Writes <code>len</code> bytes from the specified
   * <code>byte</code> array starting at offset <code>off</code> to
   * this output stream.
   *
   * @param b   the data.
   * @param off the start offset in the data.
   * @param len the number of bytes to write.
   * @throws java.io.IOException if an I/O error occurs.
   */
  @Override
  public void write(@Nonnull byte[] b, int off, int len) throws IOException {
    test();
    try {
      out.write(b, off, len);
    } catch (IOException e) {
      throw setException(e);
    }
  }

  /**
   * Flushes this output stream and forces any buffered output bytes
   * to be written out to the stream.
   * <p/>
   * The <code>flush</code> method of <code>FilterOutputStream</code>
   * calls the <code>flush</code> method of its underlying output stream.
   *
   * @throws java.io.IOException if an I/O error occurs.
   * @see java.io.FilterOutputStream#out
   */
  @Override
  public void flush() throws IOException {
    test();
    try {
      out.flush();
    } catch (IOException e) {
      throw setException(e);
    }
  }

  public void await() throws IOException, InterruptedException {
    try {
      _sync.await();
    } finally {
      test();
    }
  }

  public boolean await(long timeout, @Nonnull TimeUnit unit) throws IOException, InterruptedException {
    try {
      return _sync.await(timeout, unit);
    } finally {
      test();
    }
  }

  /**
   * Closes this output stream and releases any system resources
   * associated with the stream.
   *
   * @throws java.io.IOException if an I/O error occurs.
   */
  @Override
  public void close() throws IOException {
    try {
      if (!_closed) {
        if (_exception == null) {
          super.close();
        } else {
          // When there is an exception, we don't bother closing the compressor
          // and only close the destination output stream.
          // This should ensure that the archive is "broken"
          _destination.close();
        }
      }
      _closed = true;
    } catch (IOException e) {
      throw setException(e);
    } finally {
      test();
      _sync.countDown();
    }
  }

  public synchronized IOException setException(@Nonnull IOException cause) {
    cause = Objects.requireNonNull(cause);
    if (_exception == null) {
      _exception = cause;
      _sync.countDown();
    }
    return _exception;
  }
}
