package com.linkedin.venice.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;


/**
 * Adapted from https://stackoverflow.com/a/723914/791758
 */
public final class ForkedJavaProcess extends Process {

  /**
   * Debug mode is only intended for debugging individual forked processes, as it will block the forked JVM until
   * the debugger is attached. Debug mode is not intended to be used as part of the regular suite.
   */
  private static final boolean debug = false;
  private final ExecutorService executorService;
  private final Process process;
  private final Thread processReaper;
  private final Logger logger;
  private final AtomicBoolean destroyCalled = new AtomicBoolean();

  public static Process exec(Class klass, String... params) throws IOException, InterruptedException {
    // Argument preparation
    String javaBin = Paths.get(System.getProperty("java.home"), "bin", "java").toAbsolutePath().toString();
    String classpath = System.getProperty("java.class.path");
    String className = klass.getCanonicalName();
    List<String> args = new ArrayList<>();
    args.addAll(Arrays.asList(javaBin, "-cp", classpath));
    int debugPort = -1;
    if (debug) {
      debugPort = getFreePort();
      args.add("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=" + debugPort);
    }
    args.add(className);
    args.addAll(Arrays.asList(params));

    // Actual process forking
    ProcessBuilder builder = new ProcessBuilder(args).redirectErrorStream(true);
    Process rawProcess = builder.start();

    // Logging shenanigans
    long pid = getPidOfProcess(rawProcess);
    String loggerName = "Forked process [" + klass.getSimpleName();
    if (pid > -1) {
      loggerName += ", PID " + pid;
    }
    if (debug) {
      loggerName += ", debugPort " + debugPort;
    }
    loggerName += "]";
    Logger logger = Logger.getLogger(loggerName);
    logger.info("Process forked with params: " + Arrays.stream(params).collect(Collectors.joining(" ")));

    return new ForkedJavaProcess(rawProcess, logger);
  }

  /**
   * Construction should happen via {@link #exec(Class, String...)}
   */
  private ForkedJavaProcess(Process process, Logger logger) {
    this.processReaper = new Thread(() -> process.destroyForcibly());
    Runtime.getRuntime().addShutdownHook(processReaper);

    this.process = process;
    this.logger = logger;
    this.executorService = Executors.newSingleThreadExecutor();
    executorService.submit(() -> {
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
        while (true) {
          String line = reader.readLine();
          if (line != null) {
            logger.info(line);
          } else {
            Thread.sleep(100);
          }
        }
      } catch (IOException e) {
        if (!destroyCalled.get()) {
          logger.error("Got an unexpected IOException in LoggingProcess.", e);
        }
      } catch (InterruptedException e) {
        logger.error("Got an InterruptedException in LoggingProcess.", e);
        Thread.currentThread().interrupt();
      }
    });
  }

  /**
   * Only works for UNIX.
   *
   * Adapted from https://stackoverflow.com/a/33171840/791758
   */
  private static synchronized long getPidOfProcess(Process p) {
    long pid = -1;

    try {
      if (p.getClass().getName().equals("java.lang.UNIXProcess")) {
        Field f = p.getClass().getDeclaredField("pid");
        f.setAccessible(true);
        pid = f.getLong(p);
        f.setAccessible(false);
      }
    } catch (Exception e) {
      pid = -2;
    }
    return pid;
  }

  @Override
  public OutputStream getOutputStream() {
    return process.getOutputStream();
  }

  @Override
  public InputStream getInputStream() {
    return process.getInputStream();
  }

  @Override
  public InputStream getErrorStream() {
    return process.getErrorStream();
  }

  @Override
  public int waitFor() throws InterruptedException {
    return process.waitFor();
  }

  @Override
  public int exitValue() {
    return process.exitValue();
  }

  @Override
  public void destroy() {
    logger.info(getClass().getSimpleName() + ".destroy() called.");
    long killStartTime = System.currentTimeMillis();
    long maxTime = killStartTime + 30 * Time.MS_PER_SECOND;
    destroyCalled.set(true);
    process.destroyForcibly();
    Runtime.getRuntime().removeShutdownHook(processReaper);

    /**
     * Apparently, we can leak FDs if we don't manually close these streams.
     *
     * Source: http://www.ryanchapin.com/fv-b-4-689/Too-Many-Open-Files-Errors-When-Using-Runtime-exec---or-ProcessBuilder-start---to-Execute-A-Process.html
     */
    IOUtils.closeQuietly(process.getInputStream());
    IOUtils.closeQuietly(process.getOutputStream());
    IOUtils.closeQuietly(process.getErrorStream());

    int attempt = 1;
    while (System.currentTimeMillis() < maxTime) {
      try {
        int exitValue = process.exitValue();
        long elapsedTime = System.currentTimeMillis() - killStartTime;
        logger.info(process.getClass().getSimpleName() + ".destroy() called called. "
                + "Exit value was: " + exitValue + " after " + attempt + " attempt(s) (" + elapsedTime + " ms).");
        break;
      } catch (IllegalThreadStateException e) {
        attempt++;
        try {
          Thread.sleep(100);
        } catch (InterruptedException e1) {
          logger.warn("Sleep interrupted while trying to kill process.");
          break;
        }
      }
    }
    executorService.shutdownNow();
  }

  static int getFreePort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch(IOException e) {
      throw new RuntimeException(e);
    }
  }
}