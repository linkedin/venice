package com.linkedin.alpini.log;

import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.WeakHashMap;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.message.MessageFactory;
import org.apache.logging.log4j.spi.ExtendedLogger;
import org.apache.logging.log4j.spi.LoggerContext;


public final class FastLogContext implements LoggerContext {
  private static final ThreadLocal<Map<FastLogContext, Map<ExtendedLogger, Reference<FastLogger>>>> MAP =
      ThreadLocal.withInitial(WeakHashMap::new);

  private final FastLogMBean _fastLog;
  private final LoggerContext _context;

  public FastLogContext(@Nonnull FastLogMBean fastLog, @Nonnull LoggerContext context) {
    _fastLog = fastLog;
    _context = context;
  }

  public FastLogMBean getManagementMBean() {
    return _fastLog;
  }

  LoggerContext internalContext() {
    return _context;
  }

  @Override
  public Object getExternalContext() {
    return _context.getExternalContext();
  }

  @Override
  public FastLogger getLogger(String name) {
    return getLogger(_context.getLogger(name));
  }

  @Override
  public FastLogger getLogger(String name, MessageFactory messageFactory) {
    return getLogger(_context.getLogger(name, messageFactory));
  }

  private Map<ExtendedLogger, Reference<FastLogger>> contextMap() {
    return MAP.get().computeIfAbsent(this, FastLogContext::newContextMap);
  }

  private static Map<ExtendedLogger, Reference<FastLogger>> newContextMap(FastLogContext fastLogContext) {
    return new WeakHashMap<>();
  }

  private FastLogger getLogger(ExtendedLogger logger) {
    if (logger == null) {
      return null;
    }
    Map<ExtendedLogger, Reference<FastLogger>> map = contextMap();
    FastLogger result;
    do {
      result = map.compute(logger, this::computeLogger).get();
    } while (result == null);
    return result;
  }

  private Reference<FastLogger> computeLogger(@Nonnull ExtendedLogger logger, Reference<FastLogger> ref) {
    if (ref == null || ref.get() == null) {
      ref = new WeakReference<>(new FastLogger(this, logger));
    }
    return ref;
  }

  @Override
  public boolean hasLogger(String name) {
    return _context.hasLogger(name);
  }

  @Override
  public boolean hasLogger(String name, MessageFactory messageFactory) {
    return _context.hasLogger(name, messageFactory);
  }

  @Override
  public boolean hasLogger(String name, Class<? extends MessageFactory> messageFactoryClass) {
    return _context.hasLogger(name, messageFactoryClass);
  }

  public boolean isDebugEnabled() {
    return _fastLog.isDebugEnabled();
  }

  public boolean isTraceEnabled() {
    return _fastLog.isTraceEnabled();
  }
}
