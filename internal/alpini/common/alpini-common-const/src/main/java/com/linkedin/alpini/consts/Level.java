package com.linkedin.alpini.consts;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.spi.LoggerContext;


/**
 * This is the subset of org.apache.log4j.Level and org.apache.log4j.Priority with version 1.2.17
 * Copied here because slf4j does not have a Level concept
 */
public class Level {
  public final static int OFF_INT = Integer.MAX_VALUE;
  public final static int ERROR_INT = 40000;
  public final static int WARN_INT = 30000;
  public final static int INFO_INT = 20000;
  public final static int DEBUG_INT = 10000;
  public static final int TRACE_INT = 5000;
  public final static int ALL_INT = Integer.MIN_VALUE;

  private final int level;
  private final String levelStr;
  private final Predicate<Logger> isEnabled;
  private final Log log;

  /**
   The {@code OFF} has the highest possible rank and is
   intended to turn off logging.  */
  final static public Level OFF = new Level(OFF_INT, "OFF", logger -> false, (logger, message, arguments) -> {});

  /**
   The {@code ERROR} level designates error events that
   might still allow the application to continue running.  */
  final static public Level ERROR = new Level(ERROR_INT, "ERROR", Logger::isErrorEnabled, Logger::error);

  /**
   The {@code WARN} level designates potentially harmful situations.
   */
  final static public Level WARN = new Level(WARN_INT, "WARN", Logger::isWarnEnabled, Logger::warn);

  /**
   The {@code INFO} level designates informational messages
   that highlight the progress of the application at coarse-grained
   level.  */
  final static public Level INFO = new Level(INFO_INT, "INFO", Logger::isInfoEnabled, Logger::info);

  /**
   The {@code DEBUG} Level designates fine-grained
   informational events that are most useful to debug an
   application.  */
  final static public Level DEBUG = new Level(DEBUG_INT, "DEBUG", Logger::isDebugEnabled, Logger::debug);

  /**
   * The {@code TRACE} Level designates finer-grained
   * informational events than the {@code DEBUG} level.
   */
  public static final Level TRACE = new Level(TRACE_INT, "TRACE", Logger::isTraceEnabled, Logger::trace);

  /**
   The <code>ALL</code> has the lowest possible rank and is intended to
   turn on all logging.  */
  final static public Level ALL = new Level(ALL_INT, "ALL", logger -> true, Level::logAtLevel);

  /**
   Instantiate a Level object.
   */
  protected Level(int level, String levelStr, Predicate<Logger> isEnabled, Log log) {
    this.level = level;
    this.levelStr = Objects.requireNonNull(levelStr);
    this.isEnabled = Objects.requireNonNull(isEnabled);
    this.log = Objects.requireNonNull(log);
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof Level && (this.level == ((Level) o).level);
  }

  @Override
  public int hashCode() {
    return levelStr.hashCode();
  }

  public boolean isGreaterOrEqual(Level r) {
    return level >= r.level;
  }

  @Override
  public String toString() {
    return levelStr;
  }

  private static void logAtLevel(Logger logger, String txt, Object... args) {
    logWithLevel(logger, getLevel(logger), txt, args);
  }

  /**
   * Log at the specified level. If the "logger" is null, nothing is logged.
   * If the "level" is null, nothing is logged. If the "txt" is null,
   * behaviour depends on the SLF4J implementation.
   */
  public static void logWithLevel(Logger logger, Level level, String txt, Object... args) {
    if (logger != null && level != null) {
      level.log.log(logger, Objects.requireNonNull(txt), args);
    }
  }

  private interface Log {
    void log(Logger logger, String message, Object... arguments);
  }

  public static boolean isEnabledFor(Logger logger, Level level) {
    return logger != null && level != null && level.isEnabled.test(logger);
  }

  private static final List<Level> LEVEL_LIST =
      Collections.unmodifiableList(Arrays.asList(TRACE, DEBUG, INFO, WARN, ERROR));

  public static Level getLevel(Logger logger) {
    return LEVEL_LIST.stream().filter(level -> level.isEnabled.test(logger)).findFirst().orElse(OFF);
  }

  /**
   * This uses reflection to call the innards of log4j2 without requiring the module to pull in more than the
   * log4j2Api library. Since this method should only be used during test setup, there is no need for it to
   * be performant.
   */
  public static void setLevel(Logger logger, Level level) {
    LoggerContext ctx = LogManager.getContext(false);
    org.apache.logging.log4j.Level log4jLevel = org.apache.logging.log4j.Level.toLevel(level.toString());
    try {
      Method getConfigurationMethod = ctx.getClass().getMethod("getConfiguration");
      Object config = getConfigurationMethod.invoke(ctx);
      Method getLoggerConfigMethod = config.getClass().getMethod("getLoggerConfig", String.class);
      Object loggerConfig = getLoggerConfigMethod.invoke(config, logger.getName());
      Object specificConfig = loggerConfig;
      if (!logger.getName().equals(loggerConfig.getClass().getMethod("getName").invoke(loggerConfig))) {
        specificConfig = getLoggerConfigMethod.getReturnType()
            .getConstructor(String.class, org.apache.logging.log4j.Level.class, Boolean.TYPE)
            .newInstance(logger.getName(), log4jLevel, true);
        specificConfig.getClass()
            .getMethod("setParent", getLoggerConfigMethod.getReturnType())
            .invoke(specificConfig, loggerConfig);
        config.getClass()
            .getMethod("addLogger", String.class, getLoggerConfigMethod.getReturnType())
            .invoke(config, logger.getName(), specificConfig);
      }
      specificConfig.getClass()
          .getMethod("setLevel", org.apache.logging.log4j.Level.class)
          .invoke(specificConfig, log4jLevel);
      ctx.getClass().getMethod("updateLoggers", getConfigurationMethod.getReturnType()).invoke(ctx, config);
    } catch (Throwable ex) {
      LogManager.getLogger(Level.class).debug("Cannot set level of {} to {}", logger.getName(), log4jLevel, ex);
    }
  }
}
