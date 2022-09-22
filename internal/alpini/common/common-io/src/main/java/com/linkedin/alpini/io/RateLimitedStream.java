package com.linkedin.alpini.io;

import com.linkedin.alpini.base.concurrency.AsyncFuture;
import com.linkedin.alpini.base.concurrency.AsyncFutureListener;
import com.linkedin.alpini.base.concurrency.AsyncPromise;
import com.linkedin.alpini.base.misc.Time;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.Logger;


/**
 * Utility class for copying data from an {@linkplain InputStream} to an {@linkplain OutputStream},
 * providing facility to perform rate limiting and asynchronous completion notification.
 *
 * Similar to Apache Commons IOUtils.copyLarge() but with rate limiting.
 *
 * @author Antony T Curtis <acurtis@linkedin.com>
 */
public class RateLimitedStream implements Runnable {
  private final boolean _shouldRateLimit;
  private final long _quanta; // Number of bytes we send per transmission (Or burst)
  // NOTE: _delayBetweenQuantaNs * _quantaPerSec = 1
  private long _delayBetweenQuantaNs; // Number of nano seconds between two quanta
  private long _quantaPerSec; // Quanta per second. How many times we send packets of bytes in a second

  // MAX_BUFFER_SIZE * MAX_QUANTA_PER_SEC is the max capacity that this rate limiter can stream. Due to the
  // 1G NIC, default configuration is cap to 1MB / 100us which is 1GB / s.
  public static final long DEFAULT_MAX_BUFFER_SIZE = 1024 * 1024; // Cap default buffer size to 1MB to cap the possible
                                                                  // throughput to 1MB / 100us
  public static final long DEFAULT_MAX_QUANTA_PER_SEC = 10000L; // Max quanta per second is 10^4 Hz or 100us per quanta
  // Total number of quanta can be stored in buffer. This number is set to 100 to be backward compatible for storage
  // node
  // original settings. e.g. when rateLimit is 100MB/s, numQuantaInBuffer needs to be 100 so that buffer created is 1MB
  // Suggested setting is 15 rather than 100. Additional constructor for storage node is added to support tuning
  public static final int DEFAULT_TOTAL_NUM_QUANTA_IN_BUFFER = 100;
  public static final int MIN_NUM_QUANTA_IN_BUFFER = 1; // Buffer size should at least be 1 quanta

  private final long _maxBufferSize;
  private final long _maxQuantaPerSec;

  private final Logger _log;
  private final boolean _closeInputStreamWhenDone;
  private final InputStream _inputStream;
  private final OutputStream _outputStream;
  private final AsyncPromise<RateLimitedStream> _completionFuture = AsyncFuture.deferred(true);

  private final byte[] _buffer;
  private final long _rateLimit;

  // Minimum write size is to reduce the overhead for transmitting over network
  private int _minimumWriteSize = 1024;

  private long _startTimeMs = 0L;
  private long _endTimeMs = 0L;
  private long _totalBytes = 0L;

  protected void start() {
    _startTimeMs = System.currentTimeMillis();
  }

  protected void end() {
    _endTimeMs = System.currentTimeMillis();
  }

  protected void addBytes(long numBytes) {
    _totalBytes += numBytes;
  }

  public long getBytesCopied() {
    return _totalBytes;
  }

  public long getActualBps() {
    if (_startTimeMs == 0L) {
      throw new IllegalStateException("Rate limited copy is not started");
    }
    if (_endTimeMs == 0L) {
      throw new IllegalStateException("Rate limited copy is not terminated");
    }

    long duration = _endTimeMs - _startTimeMs;
    if (duration < 0) {
      throw new IllegalStateException("Invalid duration " + duration);
    }

    duration = Math.max(duration, 1L); // account for small transfer

    return 1000L * _totalBytes / duration;
  }

  /**
   * Constructs a rate limited stream copy.
   *
   * @param log Logger instance.
   * @param inputStream the input stream.
   * @param outputStream the output stream.
   * @param rateLimit the rate limit, in bytes per second.
   * @param unwrapInputStream true if the input stream should be unwrapped to use the underlying file descriptor. Typically used with Process.
   * @throws IOException if an I/O exception has occurred.
   */
  public RateLimitedStream(
      @Nonnull Logger log,
      @Nonnull InputStream inputStream,
      @Nonnull OutputStream outputStream,
      long rateLimit,
      boolean unwrapInputStream) throws IOException {
    this(log, inputStream, outputStream, rateLimit, unwrapInputStream, true);
  }

  /**
   * Constructs a rate limited stream copy.
   *
   * @param log Logger instance.
   * @param inputStream the input stream.
   * @param outputStream the output stream.
   * @param rateLimit the rate limit, in bytes per second.
   * @param unwrapInputStream true if the input stream should be unwrapped to use the underlying file descriptor. Typically used with Process.
   * @param closeInputStreamWhenDone true if the input stream should be closed at the end.
   * @throws IOException if an I/O exception has occurred.
   */
  public RateLimitedStream(
      @Nonnull Logger log,
      @Nonnull InputStream inputStream,
      @Nonnull OutputStream outputStream,
      long rateLimit,
      boolean unwrapInputStream,
      boolean closeInputStreamWhenDone) throws IOException {
    this(
        log,
        inputStream,
        outputStream,
        rateLimit,
        unwrapInputStream,
        closeInputStreamWhenDone,
        DEFAULT_MAX_BUFFER_SIZE,
        DEFAULT_MAX_QUANTA_PER_SEC,
        DEFAULT_TOTAL_NUM_QUANTA_IN_BUFFER);
  }

  /**
   * Constructs a rate limited stream copy.
   *
   * @param log Logger instance.
   * @param inputStream the input stream.
   * @param outputStream the output stream.
   * @param rateLimit the rate limit, in bytes per second.
   * @param unwrapInputStream true if the input stream should be unwrapped to use the underlying file descriptor. Typically used with Process.
   * @param closeInputStreamWhenDone true if the input stream should be closed at the end.
   * @param numQuantaInBuffer number of quanta in the buffer
   * @throws IOException if an I/O exception has occurred.
   */
  public RateLimitedStream(
      @Nonnull Logger log,
      @Nonnull InputStream inputStream,
      @Nonnull OutputStream outputStream,
      long rateLimit,
      boolean unwrapInputStream,
      boolean closeInputStreamWhenDone,
      int numQuantaInBuffer) throws IOException {
    this(
        log,
        inputStream,
        outputStream,
        rateLimit,
        unwrapInputStream,
        closeInputStreamWhenDone,
        DEFAULT_MAX_BUFFER_SIZE,
        DEFAULT_MAX_QUANTA_PER_SEC,
        numQuantaInBuffer);
  }

  /**
   * Constructs a rate limited stream copy.
   *
   * @param log Logger instance.
   * @param inputStream the input stream.
   * @param outputStream the output stream.
   * @param rateLimit the rate limit, in bytes per second.
   * @param unwrapInputStream true if the input stream should be unwrapped to use the underlying file descriptor. Typically used with Process.
   * @param closeInputStreamWhenDone true if the input stream should be closed at the end.
   * @param maxBufferSize max buffer size to use. For every burst of the transmit, this is the max amount of data that can
   *                      be transferred. Minimum maxBufferSize should be 1KB.
   * @param maxQuantaPerSec max number of quanta per second to define the granularity of the stream.
   * @param numQuantaInBuffer
   * @throws IOException if an I/O exception has occurred.
   */
  public RateLimitedStream(
      @Nonnull Logger log,
      @Nonnull InputStream inputStream,
      @Nonnull OutputStream outputStream,
      long rateLimit,
      boolean unwrapInputStream,
      boolean closeInputStreamWhenDone,
      long maxBufferSize,
      long maxQuantaPerSec,
      int numQuantaInBuffer) throws IOException {
    this._log = Objects.requireNonNull(log, "log");

    if (unwrapInputStream) {
      log.debug("Unwrapping input stream");

      while (inputStream instanceof BufferedInputStream) {
        inputStream = IOUtils.unwrapFilterInputStream((BufferedInputStream) inputStream);
      }

      if (inputStream instanceof FileInputStream) {
        inputStream = new FileInputStream(((FileInputStream) inputStream).getFD());
      }
    }

    _closeInputStreamWhenDone = closeInputStreamWhenDone;
    _inputStream = Objects.requireNonNull(inputStream, "inputStream");
    _outputStream = Objects.requireNonNull(outputStream, "outputStream");
    _shouldRateLimit = rateLimit > 0;
    _rateLimit = _shouldRateLimit ? rateLimit : -1L;

    // At low rates, it's unreasonable to expect to write whole 1k blocks and keep rate.
    if (rateLimit > 0 && rateLimit < 2000) {
      _minimumWriteSize = 1;
    } else if (rateLimit < 10000) {
      _minimumWriteSize = 100;
    }

    // max buffer size should be at least minimum write size
    _maxBufferSize = maxBufferSize < _minimumWriteSize ? _minimumWriteSize : maxBufferSize;
    _maxQuantaPerSec = maxQuantaPerSec;

    _quanta = Math.max(_minimumWriteSize, _rateLimit / _maxQuantaPerSec); // Unit for quanta is byte

    // Buffer size should be fairly larger than _quanta so that it has certain ability to catch up if behind schedule
    // Buffer size should be at least the same size with quanta
    numQuantaInBuffer = Math.max(numQuantaInBuffer, MIN_NUM_QUANTA_IN_BUFFER);
    _buffer = new byte[(int) Math.min(_maxBufferSize, numQuantaInBuffer * _quanta)];

    if (_shouldRateLimit) {
      _quantaPerSec = _rateLimit / _quanta; // Number of bursts we send per second
      _delayBetweenQuantaNs = TimeUnit.SECONDS.toNanos(1) / _quantaPerSec; // Delay between two quanta in nano seconds
    }

    log.debug("RateLimitedStream configured to {} bytes per second", _rateLimit);
  }

  @Override
  public void run() {
    start();
    String stream = "";
    int available = 0;

    try {
      int target = 0;
      for (long nextRateCheck = Time.nanoTime(); !_completionFuture.isCancelled();) {
        stream = "input stream";
        available = Math.min(_buffer.length, _inputStream.available());

        if (_shouldRateLimit) {
          long curTime = Time.nanoTime();

          // Since this is comparison between two time on unit of nano second, it's likely that time goes backward
          // based on how nanosecond is implemented. This check is to prevent such cases.
          if (curTime >= nextRateCheck) {
            // Calculate the number of _delayBetweenQuantaNs has passed.
            // e.g. if _rateLimit is 60MB/s, then _delayBetweenQuantaNs is set to 100,000 ns on instantiation.
            // If curTime - nextRateCheck = 1,100,000ns = 11 * 100,000ns = 11 * _delayBetweenQuantaNs
            // Then delta obtained here is 11
            long delta = (curTime - nextRateCheck) / _delayBetweenQuantaNs;

            // nextRateCheck is increased by unit of _delayBetweenQuantaNs and always behind curTime due to precision
            // loss from the previous step
            nextRateCheck += delta * _delayBetweenQuantaNs; // SUPPRESS CHECKSTYLE ModifiedControlVariable

            // Rate limiting calculation
            // NOTE: target is none-zero when available bytes are less than the target. So the previous transmission
            // doesn't
            // reach the rate requirement. It's added to make sure it's trying to catch up in this round.
            target = Math.min(target + (int) ((delta * _rateLimit) / _quantaPerSec), _buffer.length);
          }

          if (target < 1 || (target < _minimumWriteSize && available >= _minimumWriteSize)) {
            long wakeUp = curTime - nextRateCheck;
            if (wakeUp < 1000L) {
              Thread.yield();
            } else {
              Time.sleep(wakeUp / 1000000, (int) (wakeUp % 1000000));
            }
            continue;
          }

          available = Math.min(available, target);
        }

        int bytesRead;

        if (available <= 1) {
          int c = _inputStream.read();
          if (c == -1) {
            _log.debug("End of stream");
            break;
          }

          _buffer[0] = (byte) c;
          bytesRead = 1;
        } else {
          bytesRead = _inputStream.read(_buffer, 0, available);
          if (bytesRead == -1) {
            return;
          }
        }

        stream = "output stream";
        _outputStream.write(_buffer, 0, bytesRead);
        _outputStream.flush();

        if (_shouldRateLimit) {
          target -= bytesRead;
        }

        addBytes(bytesRead);
      }

      _completionFuture.setSuccess(this);
    } catch (IOException e) {
      _log.error("{} available={}", stream, available, e);
      _completionFuture.setFailure(e);
    } catch (InterruptedException e) {
      _log.error("Interrupted", e);
      _completionFuture.setFailure(e);
    } finally {
      end();
      if (_closeInputStreamWhenDone) {
        IOUtils.closeQuietly(_inputStream);
      }
    }
  }

  public RateLimitedStream setMinimumWriteSize(int minimumWriteSize) {
    if (minimumWriteSize < 1) {
      throw new IllegalArgumentException();
    }
    _minimumWriteSize = minimumWriteSize;
    return this;
  }

  /**
   * Return the AsyncFuture for the completion of this task.
   * @return the AsyncFuture.
   */
  public AsyncFuture<RateLimitedStream> getCompletionFuture() {
    return _completionFuture;
  }

  /**
   * Add a listener to the completion future of this task.
   * @param listener the listener
   * @return this
   */
  public RateLimitedStream addCompletionListener(AsyncFutureListener<RateLimitedStream> listener) {
    _completionFuture.addListener(listener);
    return this;
  }
}
