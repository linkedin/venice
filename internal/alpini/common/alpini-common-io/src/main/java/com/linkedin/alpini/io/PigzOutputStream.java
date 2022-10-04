package com.linkedin.alpini.io;

import com.linkedin.alpini.base.hash.Crc32;
import com.linkedin.alpini.base.misc.Time;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.WillCloseWhenClosed;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * @author Antony T Curtis <acurtis@linkedin.com>
 */
public class PigzOutputStream extends FilterOutputStream {
  private static final Logger LOG = LogManager.getLogger(PigzOutputStream.class);

  private static final int[] ODD = precomputeCrc32Comb();
  private static final int CHECK_INIT = Crc32.crc32(0, new byte[0], 0, 0);

  public static final int DICT_LENGTH = 32768;
  public static final int DEFAULT_BLOCK_SIZE = 128 * 1024;

  private final Executor _executor;
  private final BlockingQueue<Compressor> _idleCompressors;
  private final ConcurrentLinkedQueue<IOException> _exceptions = new ConcurrentLinkedQueue<>();
  private final BlockingQueue<Semaphore> _writeSemaphoreQueue;
  private final BlockingQueue<Semaphore> _crcSemaphoreQueue;
  private final CountDownLatch _finished = new CountDownLatch(1);
  private final int _compressionLevel;
  private final byte[] _unaryByte = new byte[1];
  private Compressor _current;
  private int _sequence;
  private volatile int _crc = CHECK_INIT;
  private volatile long _ulen;
  private volatile long _clen;
  private byte[] _dict;
  private int _dictLength;
  private boolean _finalBlockSubmitted;
  private boolean _closed;
  private boolean _footerWritten;

  /**
   * Creates a new output stream with the specified compression level, concurrency and a default block size.
   * @param compressionLevel gzip compression level, 1..9
   * @param executor executor to run threads.
   * @param concurrency maximum number of worker threads.
   * @param out the output stream.
   * @throws IOException if an I/O error has occurred.
   */
  public PigzOutputStream(
      int compressionLevel,
      @Nonnull Executor executor,
      int concurrency,
      @Nonnull @WillCloseWhenClosed OutputStream out) throws IOException {
    this(compressionLevel, executor, concurrency, out, DEFAULT_BLOCK_SIZE);
  }

  /**
   * Creates a new output stream with the specified compression level, concurrency and block size.
   * @param compressionLevel gzip compression level, 1..9
   * @param executor executor to run threads.
   * @param concurrency maximum number of worker threads.
   * @param out the output stream.
   * @param blockSize compressor block size
   * @throws IOException if an I/O error has occurred.
   */
  public PigzOutputStream(
      int compressionLevel,
      @Nonnull Executor executor,
      int concurrency,
      @Nonnull @WillCloseWhenClosed OutputStream out,
      int blockSize) throws IOException {
    this(
        compressionLevel,
        executor,
        concurrency,
        out,
        blockSize,
        (pigz) -> new PigzDeflateCompressor(pigz, compressionLevel, blockSize));
  }

  public PigzOutputStream(
      int compressionLevel,
      @Nonnull Executor executor,
      int concurrency,
      @Nonnull @WillCloseWhenClosed OutputStream out,
      int blockSize,
      @Nonnull Function<PigzOutputStream, Compressor> compressorSupplier) throws IOException {
    super(Objects.requireNonNull(out));
    try {
      executor = Objects.requireNonNull(executor, "executor cannot be null");

      if (concurrency < 1 || compressionLevel < 1 || compressionLevel > 9 || blockSize < DICT_LENGTH) {
        throw new IllegalArgumentException();
      }

      _executor = executor;
      _compressionLevel = compressionLevel;

      writeHeader();

      _idleCompressors = new LinkedBlockingQueue<>();
      _writeSemaphoreQueue = new LinkedBlockingQueue<>();
      _crcSemaphoreQueue = new LinkedBlockingQueue<>();

      while (concurrency > 0) {
        Compressor compressor = compressorSupplier.apply(this);
        if (compressor._pigz != this || _idleCompressors.contains(compressor)) {
          throw new IllegalArgumentException();
        }
        _idleCompressors.add(compressor);
        concurrency--;
      }

      _writeSemaphoreQueue.add(new Semaphore(1));
      _crcSemaphoreQueue.add(new Semaphore(1));
    } catch (RuntimeException ex) {
      IOUtils.closeQuietly(out);
      throw ex;
    }
  }

  private void writeHeader() throws IOException {
    int time = (int) (Time.currentTimeMillis() / 1000L);
    byte[] header = { (byte) 31, (byte) 139, (byte) 8, (byte) 0, (byte) ((time) & 0xff), (byte) ((time >>> 8) & 0xff),
        (byte) ((time >>> 16) & 0xff), (byte) ((time >>> 24) & 0xff),
        _compressionLevel >= 9 ? (byte) 2 : _compressionLevel == 1 ? (byte) 4 : (byte) 0, (byte) 3 };
    out.write(header, 0, 10);
    out.flush();
  }

  /**
   * Writes the specified <code>byte</code> to this output stream.
   * <p/>
   * Implements the abstract <tt>write</tt> method of <tt>OutputStream</tt>.
   *
   * @param b the <code>byte</code>.
   * @throws IOException if an I/O error occurs.
   */
  @Override
  public void write(int b) throws IOException {
    _unaryByte[0] = (byte) b;
    write(_unaryByte, 0, 1);
  }

  private void throwPendingException() throws IOException {
    if (!_exceptions.isEmpty()) {
      throw _exceptions.remove();
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
   * @throws IOException if an I/O error occurs.
   * @see FilterOutputStream#write(int)
   */
  @Override
  public void write(@Nonnull byte[] b, int off, int len) throws IOException {
    if (_closed) {
      throw new IOException("already closed");
    }
    if (off < 0 || len < 0 || b.length < off + len) {
      throw new ArrayIndexOutOfBoundsException();
    }
    while (len > 0) {
      throwPendingException();

      checkCompressor();

      int count = _current.write(b, off, len);
      if (_current.isInputBufferFull()) {
        submit();
      }
      off += count;
      len -= count;
    }
  }

  /**
   * Flushes this output stream and forces any buffered output bytes
   * to be written out to the stream.
   * <p/>
   * The <code>flush</code> method of <code>FilterOutputStream</code>
   * calls the <code>flush</code> method of its underlying output stream.
   *
   * @throws IOException if an I/O error occurs.
   * @see FilterOutputStream#out
   */
  @Override
  public void flush() throws IOException {
    throwPendingException();

    if (_current != null) {
      submit();
    }
  }

  private void submit() {
    _current.setDictionary(_dict, _dictLength);

    /* Set the compression dictionary for the next block */
    if (_current.getInputAvailable() >= DICT_LENGTH || _dict == null) {
      _dictLength = Math.min(DICT_LENGTH, _current.getInputAvailable());
      _dict = new byte[DICT_LENGTH];
      System.arraycopy(_current.getInputBuffer(), _current.getInputAvailable() - _dictLength, _dict, 0, _dictLength);
    } else if (_dictLength + _current.getInputAvailable() <= DICT_LENGTH) {
      System.arraycopy(_current.getInputBuffer(), 0, _dict, _dictLength, _current.getInputAvailable());
      _dictLength += _current.getInputAvailable();
    } else {
      byte[] old = _dict;
      int excess = _dictLength + _current.getInputAvailable() - DICT_LENGTH;
      _dict = new byte[DICT_LENGTH];
      _dictLength -= excess;
      System.arraycopy(old, excess, _dict, 0, _dictLength);
      System.arraycopy(_current.getInputBuffer(), 0, _dict, _dictLength, _current.getInputAvailable());
      _dictLength += _current.getInputAvailable();
    }

    _executor.execute(_current);
    _current = null;
  }

  private Compressor checkCompressor() throws IOException {
    if (_current == null) {
      try {
        Semaphore writeSemaphore = _writeSemaphoreQueue.take();
        Semaphore crcSemaphore = _crcSemaphoreQueue.take();
        Semaphore nextWriteSemaphore;
        Semaphore nextCrcSemaphore;

        while ((nextWriteSemaphore = _writeSemaphoreQueue.peek()) == null) {
          _writeSemaphoreQueue.add(new Semaphore(0));
        }
        while ((nextCrcSemaphore = _crcSemaphoreQueue.peek()) == null) {
          _crcSemaphoreQueue.add(new Semaphore(0));
        }

        _current = _idleCompressors.take()
            .reset(_sequence++, writeSemaphore, nextWriteSemaphore, crcSemaphore, nextCrcSemaphore);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }
    return _current;
  }

  /**
   * Closes this output stream and releases any system resources
   * associated with the stream.
   *
   * @throws IOException if an I/O error occurs.
   * @see FilterOutputStream#flush()
   * @see FilterOutputStream#out
   */
  @Override
  public void close() throws IOException {
    boolean awaiting = false;
    try {
      _closed = true;

      throwPendingException();

      if (!_finalBlockSubmitted) {
        checkCompressor().setFinalBlock();
        _finalBlockSubmitted = true;
      }

      flush();

      awaiting = true;
      _finished.await();
      awaiting = false;

      if (!_footerWritten) {
        long crc = _crc;
        long ulen = _ulen;

        byte[] trailer = new byte[] { (byte) (crc & 0xff), (byte) ((crc >>> 8) & 0xff), (byte) ((crc >>> 16) & 0xff),
            (byte) ((crc >>> 24) & 0xff), (byte) (ulen & 0xff), (byte) ((ulen >>> 8) & 0xff),
            (byte) ((ulen >>> 16) & 0xff), (byte) ((ulen >>> 24) & 0xff) };
        out.write(trailer, 0, 8);
        out.flush();
        _footerWritten = true;
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    } finally {
      if (!awaiting) {
        out.close();
      }
    }
  }

  /**
   * Returns how many bytes has been written to this OutputStream.
   * @return number of bytes
   */
  public long getBytesCompressed() {
    return _ulen;
  }

  /**
   * Returns how many bytes has been written to the underlying OutputStream.
   * @return number of bytes
   */
  public long getBytesWritten() {
    return _clen;
  }

  private static final byte[] EMPTY = new byte[0];

  public static abstract class Compressor implements Runnable {
    protected static final Logger LOG = PigzOutputStream.LOG;
    private final @Nonnull PigzOutputStream _pigz;

    private final int[] _even = new int[32];
    private final int[] _odd = new int[32];

    private int _crc32;
    private byte[] _dict;
    private int _dictLength;

    private boolean _finalBlock;

    private int _sequence;
    private Semaphore _writeSerializer;
    private Semaphore _nextWriteSerializer;
    private Semaphore _crcSerializer;
    private Semaphore _nextCrcSerializer;

    protected Compressor(@Nonnull PigzOutputStream pigz) {
      _pigz = pigz;
    }

    private Compressor reset(
        int sequence,
        Semaphore writeSerializer,
        Semaphore nextWriteSerializer,
        Semaphore crcSerializer,
        Semaphore nextCrcSerializer) {
      _sequence = sequence;
      resetDeflator();
      _crc32 = Crc32.crc32(0, EMPTY, 0, 0);
      _writeSerializer = writeSerializer;
      _nextWriteSerializer = nextWriteSerializer;
      _crcSerializer = crcSerializer;
      _nextCrcSerializer = nextCrcSerializer;
      return this;
    }

    private void setFinalBlock() {
      _finalBlock = true;
    }

    public final boolean isFinalBlock() {
      return _finalBlock;
    }

    public final int getSequence() {
      return _sequence;
    }

    public final void setDictionary(byte[] dict, int dictLength) {
      _dict = dict;
      _dictLength = dictLength;
    }

    protected abstract void setDeflateDictionary(byte[] dict, int dictLength);

    protected abstract void resetDeflator();

    protected abstract void reinitDeflator();

    protected abstract int write(@Nonnull byte[] b, int off, int len);

    protected abstract int getInputAvailable();

    protected abstract boolean isInputBufferFull();

    protected abstract @Nonnull byte[] getInputBuffer();

    protected abstract @Nonnull byte[] getOutputBuffer();

    protected abstract int getOutputAvailable();

    protected abstract void deflate() throws IOException;

    @Override
    public final void run() {
      try {
        if (_dict != null) {
          setDeflateDictionary(_dict, _dictLength);
        }

        _crc32 = Crc32.crc32(_crc32, getInputBuffer(), 0, getInputAvailable());

        try {
          deflate();
        } catch (IOException ex) {
          LOG.warn("deflate exception", ex);
          _pigz._exceptions.add(ex);
        }

        /* Wait for the write ticket */
        _writeSerializer.acquireUninterruptibly();
        _pigz._writeSemaphoreQueue.add(_writeSerializer);
        _writeSerializer = null;

        int ofs = 0;
        int len = getOutputAvailable();
        if (len != 0) {
          try {
            _pigz.out.write(getOutputBuffer(), ofs, len);
            _pigz.out.flush();
            _pigz._clen += len;
          } catch (IOException e) {
            _pigz._exceptions.add(e);
          }
        }

        /* Release the next write ticket */
        _nextWriteSerializer.release();
        _nextWriteSerializer = null;

        /* Wait for the CRC ticket */
        _crcSerializer.acquireUninterruptibly();
        _pigz._crcSemaphoreQueue.add(_crcSerializer);
        _crcSerializer = null;

        _pigz._crc = crc32Comb(_pigz._crc, _crc32, getInputAvailable());
        _pigz._ulen += getInputAvailable();

        if (_finalBlock) {
          /* Indicate that we have finished and that the trailer can be written */
          _pigz._finished.countDown();
        } else {
          /* Release the next CRC ticket */
          _nextCrcSerializer.release();
          _nextCrcSerializer = null;
        }
      } finally {
        reinitDeflator();
        _pigz._idleCompressors.add(this);
      }
    }

    private int crc32Comb(int crc1, int crc2, int len2) {
      /* degenerate case (also disallow negative lengths) */
      if (len2 <= 0) {
        return crc1;
      }

      System.arraycopy(PigzOutputStream.ODD, 0, _odd, 0, 32);

      do {
        gf2MatrixSquare(_even, _odd);
        if ((len2 & 1) != 0) {
          crc1 = gf2MatrixTimes(_even, crc1);
        }
        len2 >>>= 1;

        if (len2 == 0) {
          break;
        }

        gf2MatrixSquare(_odd, _even);
        if ((len2 & 1) != 0) {
          crc1 = gf2MatrixTimes(_odd, crc1);
        }
        len2 >>>= 1;
      } while (len2 != 0);
      return crc1 ^ crc2;
    }
  }

  private static @Nonnull int[] precomputeCrc32Comb() {
    int[] even = new int[32];
    int[] odd = new int[32];

    odd[0] = 0xedb88320;
    int row = 1;
    for (int n = 1; n < 32; n++) {
      odd[n] = row;
      row <<= 1;
    }

    gf2MatrixSquare(even, odd);
    gf2MatrixSquare(odd, even);

    return odd;
  }

  private static int gf2MatrixTimes(@Nonnull int[] mat, int vec) {
    int sum = 0;
    int index = 0;
    while (vec != 0) {
      if ((vec & 1) != 0) {
        sum ^= mat[index];
      }
      vec >>>= 1;
      index++;
    }
    return sum;
  }

  private static void gf2MatrixSquare(@Nonnull int[] square, @Nonnull int[] mat) {
    for (int n = 0; n < 32; n++) {
      square[n] = gf2MatrixTimes(mat, mat[n]);
    }
  }
}
