package com.linkedin.venice.utils;

import io.github.classgraph.ClassGraph;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Adapted from https://stackoverflow.com/a/723914/791758
 */
public final class ForkedJavaProcess extends Process {
  private static final Logger logger = Logger.getLogger(ForkedJavaProcess.class);

  /**
   * Debug mode is only intended for debugging individual forked processes, as it will block the forked JVM until
   * the debugger is attached. Debug mode is not intended to be used as part of the regular suite.
   */
  private static final boolean debug = false;

  private final Process process;
  private final Thread processReaper;
  private final ExecutorService executorService;
  private final AtomicBoolean isDestroyed = new AtomicBoolean();

  public static ForkedJavaProcess exec(Class appClass, String... args) throws IOException, InterruptedException {
    logger.info("Forking " + appClass.getSimpleName() + " with arguments " + Arrays.asList(args));

    List<String> command = new ArrayList<>();
    command.add(Paths.get(System.getProperty("java.home"), "bin", "java").toAbsolutePath().toString());
    command.add("-cp");
    command.add(buildClassPath());
    command.add("-Djava.io.tmpdir=" + System.getProperty("java.io.tmpdir"));

    /**
     * Add log4j2 configuration file. This config will inherit the log4j2 config file from parent process and set up correct logging level.
     */
    for (String arg : ManagementFactory.getRuntimeMXBean().getInputArguments()) {
      if (arg.contains("log4j2.configuration")) {
        String log4jConfigFilePath = arg.split("=")[1];
        command.add("-Dlog4j2.configuration=" + log4jConfigFilePath);
        command.add("-Dlog4j2.configurationFile=" + log4jConfigFilePath);
      }
    }

    int debugPort = -1;
    if (debug) {
      debugPort = Utils.getFreePort();
      command.add("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=" + debugPort);
    }

    command.add(appClass.getCanonicalName());
    command.addAll(Arrays.asList(args));

    Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
    Logger logger = Logger.getLogger(appClass.getSimpleName() + ", PID=" + getPidOfProcess(process) + ", debugPort=" + debugPort);
    return new ForkedJavaProcess(process, logger);
  }

  private static String buildClassPath() throws IOException {
    // Using set to remove duplicate classPath folders to avoid argument list too long error.
    Set<String> classPathDirs = new HashSet<>();

    // Get all jar file paths and extract their parent folders.
    List<URL> urls = new ClassGraph().scan().getClasspathURLs();
    for (URL url : urls) {
      String jarFilePath = url.getPath();
      classPathDirs.add(jarFilePath.substring(0, jarFilePath.lastIndexOf('/')));
    }

    // Adding classPath from current context classloader.
    Enumeration<URL> roots = Thread.currentThread().getContextClassLoader().getResources("");
    while (roots.hasMoreElements()) {
      String classPathRootDir = roots.nextElement().getPath();
      classPathDirs.add(classPathRootDir);
    }

    String originalClassPath = System.getProperty("java.class.path");
    StringBuilder classPath = new StringBuilder(originalClassPath);
    for (String classPathDir : classPathDirs) {
      logger.debug("Adding class path directory:  " + classPathDir);
      classPath.append(":").append(classPathDir).append("/*");
    }

    logger.debug("Original class path length: " + originalClassPath.length());
    logger.debug("Updated class path length: " + classPath.length());
    return classPath.toString();
  }

  /**
   * Construction should happen via {@link #exec(Class, String...)}
   */
  private ForkedJavaProcess(Process process, Logger logger) {
    this.process = process;
    this.processReaper = new Thread(this::destroy);
    Runtime.getRuntime().addShutdownHook(processReaper);

    this.executorService = Executors.newSingleThreadExecutor();
    executorService.submit(() -> {
      logger.info("Started logging standard output of the forked process.");
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
        for (;;) {
          String line = reader.readLine();
          if (line != null) {
            logger.info(line);
          } else {
            Thread.sleep(100);
          }
        }
      } catch (IOException e) {
        if (!isDestroyed.get()) {
          logger.error("Got an unexpected IOException in forked process logging task.", e);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
  }

  @Override
  public void destroy() {
    if (isDestroyed.getAndSet(true)) {
      logger.info("Ignoring duplicate destroy attempt.");
      return;
    }

    logger.info("Destroying forked process.");
    long startTime = System.currentTimeMillis();
    Runtime.getRuntime().removeShutdownHook(processReaper);

    try {
      process.destroy();
      if (!process.waitFor(30, TimeUnit.SECONDS)) {
        process.destroyForcibly();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();

    } finally {
      executorService.shutdownNow();
      /**
       * Apparently, we can leak FDs if we don't manually close these streams.
       *
       * Source: http://www.ryanchapin.com/fv-b-4-689/Too-Many-Open-Files-Errors-When-Using-Runtime-exec---or-ProcessBuilder-start---to-Execute-A-Process.html
       */
      IOUtils.closeQuietly(process.getInputStream());
      IOUtils.closeQuietly(process.getOutputStream());
      IOUtils.closeQuietly(process.getErrorStream());

      long elapsedTime = System.currentTimeMillis() - startTime;
      if (process.isAlive()) {
        logger.warn("Unable to terminate forked process after " + elapsedTime + "ms.");
      } else {
        logger.info("Forked process exited with code " + process.exitValue() + " after " + elapsedTime + "ms.");
      }
    }
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

  public long getPid() {
    return getPidOfProcess(process);
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
}
