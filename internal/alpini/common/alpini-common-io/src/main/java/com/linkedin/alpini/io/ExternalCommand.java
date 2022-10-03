package com.linkedin.alpini.io;

/*
* Copyright 2010-2010 LinkedIn, Inc
*
* Licensed under the Apache License, Version 2.0 (the "License"); you may not
* use this file except in compliance with the License. You may obtain a copy of
* the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
* WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
* License for the specific language governing permissions and limitations under
* the License.
*/

import com.linkedin.alpini.base.concurrency.AsyncFuture;
import com.linkedin.alpini.base.concurrency.AsyncPromise;
import com.linkedin.alpini.base.misc.Preconditions;
import com.linkedin.alpini.consts.Level;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.CheckReturnValue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
* This class encapsulates a java <code>Process</code> to handle properly
* output and error and preventing potential deadlocks. The idea is that the
* command is executed and you can get the error or output. Simple to use.
* Should not be used for big outputs because they are buffered internally.
*
* @author ypujante@linkedin.com
*/
public class ExternalCommand {
  public static final String JDK_HOME_DIR = System.getProperty("java.home");
  public static final String MODULE = ExternalCommand.class.getName();
  public static final Logger LOG = LogManager.getLogger(ExternalCommand.class);

  private final ProcessBuilder _processBuilder;

  private Process _process;
  private InputReader _out;
  private InputReader _err;

  private boolean _isTimedOut;

  private static class InputReader extends Thread {
    private static final int BUFFER_SIZE = 2048;

    private final Future<InputStream> _in;
    private final ByteArrayOutputStream _out;
    private final Level _logLevel;
    private final boolean _logVerbose;
    private boolean _running = false;
    private final String _sType;

    InputReader(Future<InputStream> in, Level logLevel, String type) {
      _in = in;
      _out = new ByteArrayOutputStream();
      _logLevel = logLevel;
      _logVerbose = LOG.isDebugEnabled() || logLevel == Level.ERROR;
      _sType = type;
    }

    @Override
    public void run() {
      _running = true;

      byte[] buf = new byte[BUFFER_SIZE];
      String prefix = "<EXTERNAL_OUTPUT>";
      int n;
      InputStream in = null;

      try {
        in = new BufferedInputStream(_in.get());
        while ((n = in.available()) >= 0) {
          int nbytes = 0;
          if (n > 0) {
            nbytes = in.read(buf);
            _out.write(buf, 0, nbytes);
          } else if (n == 0) {
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              break;
            }
          } else {
            break;
          }

          // Turning on debug logging drastically hurts perf:
          if (nbytes > 0 && _logVerbose) {
            try {
              String outputAsUtf8 = new String(buf, 0, nbytes, StandardCharsets.UTF_8);
              if (!prefix.isEmpty()) {
                outputAsUtf8 = prefix + outputAsUtf8;
                prefix = "";
              }
              int pos;
              while ((pos = outputAsUtf8.indexOf('\n')) != -1) {
                log(outputAsUtf8.substring(0, pos));
                outputAsUtf8 = outputAsUtf8.substring(pos + 1);
              }
              if (!outputAsUtf8.isEmpty()) {
                prefix += outputAsUtf8;
              }
            } catch (UnsupportedOperationException e) {
              // Should not happen
              LOG.warn("!!!FAILED TO READ OUTPUT!!!");
            }
          }
        }
      } catch (IOException e) {
        LOG.error("error while reading external command", e);
      } catch (InterruptedException e) {
        LOG.error("interrupted while starting to read from external command", e);
      } catch (ExecutionException e) {
        LOG.error("exception while starting to read from external command", e.getCause());
      } finally {
        IOUtils.closeQuietly(in);
        if (_logVerbose) {
          log(prefix + "</EXTERNAL_OUTPUT>");
        }
      }

      _running = false;
    }

    public byte[] getOutput() {
      if (_running) {
        throw new IllegalStateException("wait for process to be completed");
      }

      return _out.toByteArray();
    }

    private void log(String line) {
      Level.logWithLevel(LOG, _logLevel, "{}", line);
    }
  }

  /**
   * Constructor */
  public ExternalCommand(ProcessBuilder processBuilder) {
    _isTimedOut = false;
    _processBuilder = processBuilder;
  }

  /**
   * After creating the command, you have to start it...
   *
   * @throws IOException
   */
  public void start() throws IOException {
    if (_process != null || _out != null || _err != null) {
      throw new IllegalStateException("already started");
    }

    AsyncPromise<InputStream> out = AsyncFuture.deferred(true);
    AsyncPromise<InputStream> err = AsyncFuture.deferred(true);

    Level logLevel = Level.ERROR;
    if (LOG.isDebugEnabled()) {
      logLevel = Level.DEBUG;
    } else if (LOG.isInfoEnabled()) {
      logLevel = Level.INFO;
    }

    _out = new InputReader(out, logLevel, "Subprocess output stream");
    _err = new InputReader(err, Level.INFO, "Subprocess error stream");

    _out.start();
    _err.start();

    try {
      _process = _processBuilder.start();
      out.setSuccess(_process.getInputStream());
      err.setSuccess(_process.getErrorStream());
    } finally {
      // These will fail to cancel if successfully set above.
      out.cancel(true);
      err.cancel(true);
    }
  }

  /**
   * @see ProcessBuilder
   */
  public Map<String, String> getEnvironment() {
    return _processBuilder.environment();
  }

  /**
   * @see ProcessBuilder
   */
  public File getWorkingDirectory() {
    return _processBuilder.directory();
  }

  /**
   * @see ProcessBuilder
   */
  public void setWorkingDirectory(File directory) {
    _processBuilder.directory(directory);
  }

  /**
   * @see ProcessBuilder
   */
  public boolean getRedirectErrorStream() {
    return _processBuilder.redirectErrorStream();
  }

  /**
   * @see ProcessBuilder
   */
  public void setRedirectErrorStream(boolean redirectErrorStream) {
    _processBuilder.redirectErrorStream(redirectErrorStream);
  }

  public void setTimedOut(boolean val) {
    _isTimedOut = val;
  }

  public boolean isTimedOut() {
    return _isTimedOut;
  }

  public byte[] getOutput() throws InterruptedException {
    waitFor();
    return _out.getOutput();
  }

  public byte[] getError() throws InterruptedException {
    waitFor();
    return _err.getOutput();
  }

  /**
   * Returns the output as a string.
   *
   * @param encoding
   * @return encoded string
   * @throws InterruptedException
   * @throws UnsupportedEncodingException
   */
  public String getStringOutput(String encoding) throws InterruptedException, UnsupportedEncodingException {
    return getStringOutput(Charset.forName(encoding));
  }

  /**
   * Returns the output as a string.
   *
   * @param encoding
   * @return encoded string
   * @throws InterruptedException
   * @throws UnsupportedEncodingException
   */
  public String getStringOutput(Charset encoding) throws InterruptedException {
    return new String(getOutput(), Objects.requireNonNull(encoding, "encoding"));
  }

  /**
   * Returns the output as a string. Uses encoding "UTF-8".
   *
   * @return utf8 encoded string
   * @throws InterruptedException
   */
  public String getStringOutput() throws InterruptedException {
    return getStringOutput(StandardCharsets.UTF_8);
  }

  /**
    * Returns the error as a string.
    *
    * @param encoding
    * @return error as string
    * @throws InterruptedException
    */
  public String getStringError(Charset encoding) throws InterruptedException {
    return new String(getError(), Objects.requireNonNull(encoding, "encoding"));
  }

  /**
   * Returns the error as a string.
   *
   * @param encoding
   * @return error as string
   * @throws InterruptedException
   * @throws UnsupportedEncodingException
   */
  public String getStringError(String encoding) throws InterruptedException, UnsupportedEncodingException {
    return getStringError(Charset.forName(encoding));
  }

  /**
   * Returns the error as a string. Uses encoding "UTF-8".
   *
   * @return error as string
   * @throws InterruptedException
   */
  public String getStringError() throws InterruptedException {
    return getStringError(StandardCharsets.UTF_8);
  }

  /**
   * Properly waits until everything is complete: joins on the thread that
   * reads the output, joins on the thread that reads the error and finally
   * wait for the process to be finished.
   * @return the status code of the process.
   *
   * @throws InterruptedException
   */
  public int waitFor() throws InterruptedException {
    if (_process == null) {
      throw new IllegalStateException("you must call start first");
    }

    int exitCode = _process.waitFor();

    _out.interrupt();
    _err.interrupt();

    _out.join();
    _err.join();

    return exitCode;
  }

  private Process checkStart() {
    Process process = _process;
    Preconditions.checkState(process != null, "you must call start first");
    return process;
  }

  /**
   * Properly waits until everything is complete: joins on the thread that
   * reads the output, joins on the thread that reads the error and finally
   * wait for the process to be finished.
   * If the process has not completed before the timeout, throws a
   * {@link TimeoutException}
   * @return the status code of the process.
   *
   * @throws TimeoutException
   * @throws InterruptedException
   */
  public int waitFor(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException, IOException {
    if (checkStart().waitFor(timeout, unit)) {
      return waitFor();
    } else {
      throw new TimeoutException();
    }
  }

  /**
   * Properly waits until everything is complete: joins on the thread that
   * reads the output, joins on the thread that reads the error and finally
   * wait for the process to be finished.
   * If the process has not completed before the timeout, throws a
   * {@link TimeoutException}
   * @return the status code of the process.
   *
   * @throws TimeoutException
   * @throws InterruptedException
   */
  public int waitFor(long timeout) throws InterruptedException, TimeoutException, IOException {
    return waitFor(timeout, TimeUnit.MILLISECONDS);
  }

  public int exitValue() {
    return checkStart().exitValue();
  }

  public void destroy() {
    checkStart();

    _out.interrupt();
    _err.interrupt();

    _process.destroy();

    try {
      waitFor();
    } catch (Exception e) {
      LOG.warn("Error waiting for process termination: " + e.getMessage());
    }
  }

  /**
   * Creates an external process from the command. It is not started and you have to call
   * start on it!
   *
   * @param commands the command to execute
   * @return the process */
  public static ExternalCommand create(String... commands) {
    ExternalCommand ec = new ExternalCommand(new ProcessBuilder(commands));
    return ec;
  }

  /**
   * Creates an external process from the command. It is not started and you have to call
   * start on it!
   *
   * @param commands the command to execute
   * @return the process */
  public static ExternalCommand create(List<String> commands) {
    ExternalCommand ec = new ExternalCommand(new ProcessBuilder(commands));
    return ec;
  }

  /**
   * Creates an external process from the command. The command is executed.
   *
   * @param commands the commands to execute
   * @return the process
   * @throws IOException if there is an error */
  @CheckReturnValue
  public static ExternalCommand start(String... commands) throws IOException {
    ExternalCommand ec = new ExternalCommand(new ProcessBuilder(commands));
    ec.start();
    return ec;
  }

  /**
   * Executes the external command in the given working directory and waits for it to be
   * finished.
   *
   * @param workingDirectory the root directory from where to run the command
   * @param command the command to execute (should be relative to the working directory
   * @param args the arguments to the command
   * @return the process */
  @CheckReturnValue
  public static ExternalCommand execute(File workingDirectory, String command, String... args)
      throws IOException, InterruptedException {
    return executeWithTimeout(workingDirectory, command, 0, args);
  }

  /**
   * Executes the external command in the given working directory and waits (until timeout
   * is elapsed) for it to be finished.
   *
   * @param workingDirectory
   * the root directory from where to run the command
   * @param command
   * the command to execute (should be relative to the working directory
   * @param timeout
   * the maximum amount of time to wait for this external command (in ms). If
   * this value is less than or equal to 0, timeout is ignored
   * @param args
   * the arguments to the command
   * @return the process
   */
  @CheckReturnValue
  public static ExternalCommand executeWithTimeout(File workingDirectory, String command, long timeout, String... args)
      throws IOException, InterruptedException {
    return executeWithTimeoutWithEnv(workingDirectory, command, timeout, null, args);
  }

  @CheckReturnValue
  public static ExternalCommand executeWithTimeoutWithEnv(
      File workingDirectory,
      String command,
      long timeout,
      Map<String, String> env,
      String... args) throws IOException, InterruptedException {
    List<String> arguments = new ArrayList<>(args.length + 1);

    arguments.add(new File(workingDirectory, command).getAbsolutePath());
    arguments.addAll(Arrays.asList(args));

    ExternalCommand cmd = ExternalCommand.create(arguments);

    cmd.setWorkingDirectory(workingDirectory);

    cmd.setRedirectErrorStream(true);

    Map<String, String> cmdEnvironment = cmd.getEnvironment();
    if (env != null) {
      cmdEnvironment.putAll(env);
    }
    if (!cmdEnvironment.get("PATH").contains(JDK_HOME_DIR + "/bin")) {
      final String newpath = String.format("%s/%s:%s", JDK_HOME_DIR, "bin", cmdEnvironment.get("PATH"));
      cmdEnvironment.put("PATH", newpath);
      LOG.info("forcing JDK8 path {}", newpath);
    }
    cmdEnvironment.put("JAVA_HOME", JDK_HOME_DIR);
    cmdEnvironment.put("JDK_HOME", JDK_HOME_DIR);

    cmd.start();
    try {
      /* Use timeout if it is a valid value! */
      if (timeout <= 0) {
        cmd.waitFor();
      } else {
        cmd.waitFor(timeout);
      }
    } catch (TimeoutException te) {
      cmd.setTimedOut(true);
      LOG.warn("Command has timed out [{}] ms", timeout);
    } finally {
      // clean up
      cmd.destroy();
    }
    return cmd;
  }
}
