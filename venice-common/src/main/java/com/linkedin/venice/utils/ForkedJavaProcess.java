package com.linkedin.venice.utils;

import com.linkedin.venice.exceptions.VeniceException;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ScanResult;
import java.util.Optional;
import nonapi.io.github.classgraph.utils.JarUtils;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
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
  private static final Logger LOGGER = Logger.getLogger(ForkedJavaProcess.class);

  private static final String JAVA_PATH = Paths.get(System.getProperty("java.home"), "bin", "java").toString();
  private static final List<String> DEFAULT_JAVA_ARGS = new ArrayList() {{
    // Inherit Java tmp folder setting from parent process.
    add("-Djava.io.tmpdir=" + System.getProperty("java.io.tmpdir"));
    /*
      Add log4j2 configuration file and JVM arguments.
      This config will inherit the log4j2 config file from parent process and set up correct logging level and it will
      inherit all JVM arguments from parent process. Users can provide JVM arguments to override these settings.
     */
    for (String arg : ManagementFactory.getRuntimeMXBean().getInputArguments()) {
      /**
       * Explicitly block JVM debugging setup in forked process as (1) it is not used anywhere (2) debug port binding
       * will conflict with main process
       */
      if (arg.startsWith("-Xdebug") || arg.startsWith("-agentlib:jdwp") || arg.startsWith("-Xrunjdwp")) {
        LOGGER.info("Skipping debug related arguments in forked process:" + arg);
        continue;
      }

      if (arg.startsWith("-Dlog4j2")) {
        add(arg);
      }
      if (arg.startsWith("-Dlog4j2.configuration=")) {
        add("-Dlog4j2.configurationFile=" + arg.split("=")[1]);
      }
      if (arg.startsWith("-X")) {
        add(arg);
      }
    }
  }};

  private final Logger logger;
  private final Process process;
  private final Thread processReaper;
  private final ExecutorService executorService;
  private final AtomicBoolean isDestroyed = new AtomicBoolean();

  public static ForkedJavaProcess exec(Class appClass, String... args) throws IOException {
    return exec(appClass, Arrays.asList(args), Collections.emptyList(), Optional.empty());
  }

  public static ForkedJavaProcess exec(Class appClass, List<String> args, List<String> jvmArgs, Optional<String> extraLoggerName) throws IOException {
    LOGGER.info("Forking " + appClass.getSimpleName() + " with arguments " + args + " and jvm arguments " + jvmArgs +
        " extraLoggerName: " + (extraLoggerName.isPresent() ? extraLoggerName.get() : ""));

    List<String> command = new ArrayList<>();
    command.add(JAVA_PATH);
    command.add("-cp");
    command.add(getClasspath());

    command.addAll(DEFAULT_JAVA_ARGS);
    // Add user provided customized JVM arguments for child process.
    command.addAll(jvmArgs);

    command.add(appClass.getCanonicalName());
    command.addAll(args);

    Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
    Logger logger = Logger.getLogger((extraLoggerName.isPresent() ? extraLoggerName.get() + ", " : "") + appClass.getSimpleName() + ", PID=" + getPidOfProcess(process));
    return new ForkedJavaProcess(process, logger);
  }

  public static String getClasspath() {
    try (ScanResult scanResult = new ClassGraph().scan()) {
      Set<File> classpathDirs = new LinkedHashSet<>();
      for (File file : scanResult.getClasspathFiles()) {
        if (file.isDirectory() || file.getName().equals("*") || file.getAbsolutePath().contains(".gradle")) {
          classpathDirs.add(file);
        } else {
          classpathDirs.add(new File(file.getParent(), "*"));
        }
      }

      /*
        Prepend WEB-INF/lib classpath to the forked Java process's classpath so the forked
        process will have correct(newest) jar version for every dependencies when deployed in regular-war fashion.
       */
      for (File file : new ArrayList<>(classpathDirs)) {
        if (!file.getPath().contains("WEB-INF/lib")) {
          classpathDirs.remove(file);
          classpathDirs.add(file);
        }
      }

      /*
        Prepend extra_webapp_resources/lib classpath to the forked Java process's classpath so the forked
        process will have correct(newest) jar version for every dependencies when deployed in thin-war fashion.
       */
      for (File file : new ArrayList<>(classpathDirs)) {
        if (!file.getPath().contains("extra_webapp_resources")) {
          classpathDirs.remove(file);
          classpathDirs.add(file);
        }
      }
      return JarUtils.pathElementsToPathStr(classpathDirs);
    }
  }

  /**
   * Construction should happen via {@link #exec(Class, String...)}
   */
  private ForkedJavaProcess(Process process, Logger logger) {
    this.logger = logger;
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
      } finally {
        logger.info("Stopped logging standard output of the forked process.");
      }
    });
  }

  @Override
  public void destroy() {
    if (isDestroyed.getAndSet(true)) {
      logger.warn("Ignoring duplicate destroy attempt.", new VeniceException("Duplicate destroy attempt."));
      return;
    }

    logger.info("Destroying forked process.");
    long startTime = System.currentTimeMillis();

    try {
      if (Thread.currentThread() != processReaper) {
        process.destroy();
        if (!process.waitFor(60, TimeUnit.SECONDS)) {
          logger.info("Destroying forked process forcibly.");
          process.destroyForcibly().waitFor();
          logger.info("Forked process has been terminated forcibly");
        }
        Runtime.getRuntime().removeShutdownHook(processReaper);
      } else {
        logger.info("Destroying forked process forcibly.");
        process.destroyForcibly().waitFor();
        logger.info("Forked process has been terminated forcibly");
      }

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();

    } finally {
      executorService.shutdownNow();
      /*
        Apparently, we can leak FDs if we don't manually close these streams.

        Source: http://www.ryanchapin.com/fv-b-4-689/Too-Many-Open-Files-Errors-When-Using-Runtime-exec---or-ProcessBuilder-start---to-Execute-A-Process.html
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

  private static synchronized long getPidOfProcess(Process process) {
    try {
      if (process.getClass().getName().equals("java.lang.UNIXProcess")) {
        // Java 8: `pid` is a private field
        Field pidField = process.getClass().getDeclaredField("pid");
        pidField.setAccessible(true);
        return pidField.getInt(process);
      } else {
        // Java 9+: `pid()` is a public method
        Method pidMethod = Process.class.getMethod("pid");
        return (long) pidMethod.invoke(process);
      }
    } catch (Exception e) {
      LOGGER.error("Unable to access pid of " + process.getClass().getName(), e);
      return -1;
    }
  }

  public long pid() {
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
