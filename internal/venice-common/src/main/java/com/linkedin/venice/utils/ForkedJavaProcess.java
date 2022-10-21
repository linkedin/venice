package com.linkedin.venice.utils;

import com.linkedin.venice.exceptions.VeniceException;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ScanResult;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import nonapi.io.github.classgraph.utils.JarUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Adapted from https://stackoverflow.com/a/723914/791758
 */
public final class ForkedJavaProcess extends Process {
  private static final Logger LOGGER = LogManager.getLogger(ForkedJavaProcess.class);
  private static final String FORKED_PROCESS_LOG4J2_PROPERTIES = "status = error\n" + "name = PropertiesConfig\n"
      + "filters = threshold\n" + "filter.threshold.type = ThresholdFilter\n" + "filter.threshold.level = debug\n"
      + "appenders = console\n" + "appender.console.type = Console\n" + "appender.console.name = STDOUT\n"
      + "appender.console.layout.type = PatternLayout\n"
      + "appender.console.layout.pattern =%d{HH:mm:ss} %p [%c{1}] %replace{%m%n}{[\\r\\n]}{|}%throwable{separator(|)}%n\n"
      + "rootLogger.level = info\n" + "rootLogger.appenderRefs = stdout\n"
      + "rootLogger.appenderRef.stdout.ref = STDOUT";

  private static final String JAVA_PATH = Paths.get(System.getProperty("java.home"), "bin", "java").toString();

  private final Logger logger;
  private final Process process;
  private final ExecutorService executorService;
  private final AtomicBoolean isDestroyed = new AtomicBoolean();

  private Thread processReaper;

  public static ForkedJavaProcess exec(
      Class appClass,
      List<String> args,
      List<String> jvmArgs,
      String classPath,
      boolean killOnExit,
      Optional<String> loggerPrefix) throws IOException {
    LOGGER.info("Forking " + appClass.getSimpleName() + " with arguments " + args + " and jvm arguments " + jvmArgs);
    List<String> command = prepareCommandArgList(appClass, classPath, args, jvmArgs);

    Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
    Logger logger = LogManager.getLogger(
        loggerPrefix.map(s -> s + ", ").orElse("") + appClass.getSimpleName() + ", PID=" + getPidOfProcess(process));
    return new ForkedJavaProcess(process, logger, killOnExit);
  }

  public static ForkedJavaProcess exec(
      Class appClass,
      List<String> args,
      List<String> jvmArgs,
      boolean killOnExit,
      Optional<String> loggerPrefix) throws IOException {
    return exec(appClass, args, jvmArgs, getClasspath(), killOnExit, loggerPrefix);
  }

  public static ForkedJavaProcess exec(Class appClass, List<String> args, List<String> jvmArgs, boolean killOnExit)
      throws IOException {
    return exec(appClass, args, jvmArgs, killOnExit, Optional.empty());
  }

  public static ForkedJavaProcess exec(Class appClass, List<String> args, List<String> jvmArgs) throws IOException {
    return exec(appClass, args, jvmArgs, true, Optional.empty());
  }

  public static ForkedJavaProcess exec(Class appClass, String... args) throws IOException {
    return exec(appClass, Arrays.asList(args), Collections.emptyList());
  }

  public static String getClasspath() {
    try (ScanResult scanResult = new ClassGraph().scan()) {
      Set<File> classpathDirs = new LinkedHashSet<>();
      for (File file: scanResult.getClasspathFiles()) {
        if (file.isDirectory() || file.getName().equals("*") || file.getAbsolutePath().contains(".gradle")) {
          classpathDirs.add(file);
        } else {
          classpathDirs.add(new File(file.getParent(), "*"));
        }
      }

      /*
        Prepend WEB-INF/lib classpath to the forked Java process's classpath so the forked
        process will have correct(newest) jar version for every dependency when deployed in regular-war fashion.
       */
      for (File file: new ArrayList<>(classpathDirs)) {
        if (!file.getPath().contains("WEB-INF/lib")) {
          classpathDirs.remove(file);
          classpathDirs.add(file);
        }
      }

      /*
        Prepend extra_webapp_resources/lib classpath to the forked Java process's classpath so the forked
        process will have correct(newest) jar version for every dependency when deployed in thin-war fashion.
       */
      for (File file: new ArrayList<>(classpathDirs)) {
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
  private ForkedJavaProcess(Process process, Logger logger, boolean killOnExit) {
    this.logger = logger;
    this.process = process;
    if (killOnExit) {
      this.processReaper = new Thread(this::destroy);
      Runtime.getRuntime().addShutdownHook(processReaper);
    }

    this.executorService = Executors.newSingleThreadExecutor();
    executorService.submit(() -> {
      logger.info("Started logging standard output of the forked process.");
      LogInfo logInfo = new LogInfo();
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
        for (;;) {
          String line = reader.readLine();
          if (!StringUtils.isEmpty(line)) {
            processAndExtractLevelFromForkedProcessLog(logInfo, line);
            logger.log(logInfo.getLevel(), logInfo.getLog());
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
          logger.info("Forked process has been terminated forcibly.");
        }
        if (processReaper != null) {
          Runtime.getRuntime().removeShutdownHook(processReaper);
        }
      } else {
        logger.info("Destroying forked process forcibly.");
        process.destroyForcibly().waitFor();
        logger.info("Forked process has been terminated forcibly by process reaper.");
      }

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();

    } finally {
      executorService.shutdownNow();
      /*
        Apparently, we can leak FDs if we don't manually close these streams.
      
        Source: http://www.ryanchapin.com/fv-b-4-689/Too-Many-Open-Files-Errors-When-Using-Runtime-exec---or-ProcessBuilder-start---to-Execute-A-Process.html
       */
      Utils.closeQuietlyWithErrorLogged(process.getInputStream());
      Utils.closeQuietlyWithErrorLogged(process.getOutputStream());
      Utils.closeQuietlyWithErrorLogged(process.getErrorStream());

      long elapsedTime = System.currentTimeMillis() - startTime;
      if (process.isAlive()) {
        logger.warn("Unable to terminate forked process after " + elapsedTime + "ms.");
      } else {
        logger.info("Forked process exited with code " + process.exitValue() + " after " + elapsedTime + "ms.");
      }
    }
  }

  // Clean up and extract log and log level from forked process logging.
  public static void processAndExtractLevelFromForkedProcessLog(LogInfo logInfo, String log) {
    char[] logCharArray = log.toCharArray();
    int startIndex = 0;
    int endIndex = log.length() - 1;

    // Restore multi-line logs from forked process.
    for (int idx = startIndex; idx <= endIndex; idx++) {
      if (logCharArray[idx] == '|') {
        logCharArray[idx] = '\n';
      }
    }

    // Trim extra whitespace and newline.
    while (logCharArray[startIndex] == ' ') {
      startIndex++;
    }
    while (logCharArray[endIndex] == ' ' || logCharArray[endIndex] == '\n') {
      endIndex--;
    }

    int levelStartIndex = -1;
    for (int idx = startIndex; idx < endIndex; idx++) {
      if (logCharArray[idx] == ' ') {
        levelStartIndex = idx + 1;
        break;
      }
    }
    int levelEndIndex = -1;
    for (int idx = levelStartIndex + 1; idx < endIndex; idx++) {
      if (logCharArray[idx] == ' ') {
        levelEndIndex = idx;
        break;
      }
    }
    if (levelStartIndex == -1 || levelEndIndex == -1) {
      logInfo.setLevel(Level.INFO);
      logInfo.setLog(new String(logCharArray, startIndex, endIndex - startIndex + 1));
      return;
    }
    // Try to retrieve the original log level from forked process logging message.
    Level level = Level.getLevel(log.substring(levelStartIndex, levelEndIndex));
    if (level == null) {
      logInfo.setLevel(Level.INFO);
      logInfo.setLog(new String(logCharArray, startIndex, endIndex - startIndex + 1));
      return;
    }

    // Build the final string.
    System.arraycopy(logCharArray, levelEndIndex + 1, logCharArray, levelStartIndex, endIndex - levelEndIndex);
    int totalLength = (levelStartIndex - startIndex) + (endIndex - levelEndIndex);
    logInfo.setLevel(level);
    logInfo.setLog(new String(logCharArray, startIndex, totalLength));
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

  private static String generateLog4j2ConfigForForkedProcess() {
    String configFilePath =
        Paths.get(Utils.getTempDataDirectory().getAbsolutePath(), "log4j2.properties").toAbsolutePath().toString();
    File configFile = new File(configFilePath);
    try (FileWriter fw = new FileWriter(configFile)) {
      fw.write(FORKED_PROCESS_LOG4J2_PROPERTIES);
      LOGGER.info("log4j2 property file for forked process is stored into: " + configFilePath);
    } catch (IOException e) {
      throw new VeniceException(e);
    }
    return configFilePath;
  }

  private static List<String> prepareCommandArgList(
      Class appClass,
      String classPath,
      List<String> args,
      List<String> jvmArgs) {
    List<String> command = new ArrayList<>();
    command.add(JAVA_PATH);
    command.add("-cp");
    command.add(classPath);

    // Inherit Java tmp folder setting from parent process.
    command.add("-Djava.io.tmpdir=" + System.getProperty("java.io.tmpdir"));
    /**
     Add log4j2 configuration file and JVM arguments.
     This config will inherit the log4j2 config file from parent process and set up correct logging level and it will
     inherit all JVM arguments from parent process. Users can provide JVM arguments to override these settings.
     */
    for (String arg: ManagementFactory.getRuntimeMXBean().getInputArguments()) {
      /**
       * Explicitly block JVM debugging setup in forked process as (1) it is not used anywhere (2) debug port binding
       * will conflict with main process
       */
      if (arg.startsWith("-Xdebug") || arg.startsWith("-agentlib:jdwp") || arg.startsWith("-Xrunjdwp")) {
        LOGGER.info("Skipping debug related arguments in forked process:" + arg);
        continue;
      }
      if (arg.startsWith("-X")) {
        command.add(arg);
      }
    }
    command.addAll(jvmArgs);
    // Add customized log4j2 config file path.
    command.add("-Dlog4j2.configurationFile=" + generateLog4j2ConfigForForkedProcess());
    command.add(appClass.getCanonicalName());
    command.addAll(args);
    return command;
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

  public static class LogInfo {
    private Level level;
    private String log;

    public Level getLevel() {
      return level;
    }

    public void setLevel(Level level) {
      this.level = level;
    }

    public String getLog() {
      return log;
    }

    public void setLog(String log) {
      this.log = log;
    }
  }
}
