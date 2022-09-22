package com.linkedin.alpini.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A proxy class for java.lang.ProcessBuilder which may be overridden
 * for purposes of test.
 *
 * @author Antony T Curtis <acurtis@linkedin.com>
 */
public class SubprocessBuilder {
  private static final Logger LOG = LogManager.getLogger(Process.class);

  private ProcessBuilder _builder;
  /**
   * if not null , the process stdout is written to this logger
   */
  private Consumer<String> _logStdOut;
  /**
   * if not null , the process stderr is written to this logger
   */
  private Consumer<String> _logStdErr;
  /**
   * Used to generate a unique thread name for the logging threads
   */
  private static final AtomicInteger _loggerThreadNumber = new AtomicInteger(0);

  protected ProcessBuilder builder() {
    if (_builder == null) {
      _builder = new ProcessBuilder();
    }
    return _builder;
  }

  /**
   * @see ProcessBuilder#command()
   */
  public List<String> command() {
    return builder().command();
  }

  /**
   * @see ProcessBuilder#command(java.util.List)
   */
  public SubprocessBuilder command(List<String> command) {
    builder().command(command);
    return this;
  }

  /**
   * @see ProcessBuilder#command(String...)
   */
  public SubprocessBuilder command(String... command) {
    return command(Arrays.asList(command));
  }

  /**
   * @see ProcessBuilder#directory()
   */
  public File directory() {
    return builder().directory();
  }

  /**
   * @see ProcessBuilder#directory(java.io.File)
   */
  public SubprocessBuilder directory(File directory) {
    builder().directory(directory);
    return this;
  }

  /**
   * @see ProcessBuilder#environment()
   */
  public Map<String, String> environment() {
    return builder().environment();
  }

  /**
   * @see ProcessBuilder#redirectErrorStream()
   */
  public boolean redirectErrorStream() {
    return builder().redirectErrorStream();
  }

  /**
   * @see ProcessBuilder#redirectErrorStream(boolean)
   */
  public SubprocessBuilder redirectErrorStream(boolean redirectErrorStream) {
    builder().redirectErrorStream(redirectErrorStream);
    return this;
  }

  public ProcessBuilder processBuilder() {
    try {
      return _builder;
    } finally {
      _builder = null;
    }
  }

  /**
   * @see ProcessBuilder#start()
   */
  public Process start() throws IOException {
    Process process = processBuilder().start();
    int threadNumber = _loggerThreadNumber.incrementAndGet();

    Runnable startLogStdOut;
    if (_logStdOut != null) {
      Thread thd = new Thread(
          new LogStream(process.getInputStream(), _logStdOut),
          "EspressoProcessBuilder-Stdout-" + threadNumber);
      startLogStdOut = thd::start;
    } else {
      startLogStdOut = () -> {};
    }

    Runnable startLogErrOut;
    if (_logStdErr != null) {
      Thread thd = new Thread(
          new LogStream(process.getErrorStream(), _logStdErr),
          "EspressoProcessBuilder-Stderr-" + threadNumber);
      startLogErrOut = thd::start;
    } else {
      startLogErrOut = () -> {};
    }

    startLogStdOut.run();
    startLogErrOut.run();
    return process;
  }

  @Override
  public String toString() {
    return builder().toString();
  }

  /**
   * When the process is started , log the process stdout
   * to the Logger. This must be called before starting the process.
   * @param consumer Log for std out
   */
  public void setStdOutLog(Consumer<String> consumer) {
    _logStdOut = consumer;
  }

  /**
   * When the process is started , log the process stderr
   * to the Logger. This must be called before starting the process.
   * @param consumer Log for std err
   */
  public void setStdErrLog(Consumer<String> consumer) {
    _logStdErr = consumer;
  }

  private static class LogStream implements Runnable {
    final BufferedReader _reader;
    Consumer<String> _consumer;

    public LogStream(InputStream input, Consumer<String> consumer) throws IOException {
      _reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));
      _consumer = consumer;
    }

    @Override
    public void run() {
      try {
        while (true) {
          String line = _reader.readLine();
          if (line == null) {
            break;
          }
          if (!line.isEmpty()) {
            _consumer.accept(line);
          }
        }
      } catch (IOException ex) {
        LOG.warn("IOException reading from stream", ex);
      } finally {
        IOUtils.closeQuietly(_reader);
      }
    }
  }

}
