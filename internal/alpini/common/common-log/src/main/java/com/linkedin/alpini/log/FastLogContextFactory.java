package com.linkedin.alpini.log;

import java.lang.management.ManagementFactory;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.net.URI;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.WeakHashMap;
import javax.annotation.Nonnull;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.simple.SimpleLoggerContextFactory;
import org.apache.logging.log4j.spi.LoggerContext;
import org.apache.logging.log4j.spi.LoggerContextFactory;
import org.apache.logging.log4j.spi.Provider;
import org.apache.logging.log4j.status.StatusLogger;
import org.apache.logging.log4j.util.ProviderUtil;


/**
 * An alternate Log4j2 {@link LoggerContextFactory} implementation which returns loggers which can
 * short-circuit debug and trace messages so that they may be disabled without incurring cost from
 * filters installed within Log4j.
 *
 * This may be installed by specifying
 * <TT>-Dlog4j2.loggerContextFactory=com.linkedin.alpini.log.FastLogContextFactory</TT>
 *
 * It should install a management mbean {@literal com.linkedin.alpini.log:type=FastLog} which will
 * allow enabling DEBUG/TRACE propagation.
 */
public final class FastLogContextFactory implements LoggerContextFactory {
  private static final Logger LOGGER = StatusLogger.getLogger();

  private static final ThreadLocal<Map<FastLogContextFactory, Map<LoggerContext, Reference<FastLogContext>>>> MAP =
      ThreadLocal.withInitial(WeakHashMap::new);

  private volatile LoggerContextFactory _factory;
  private final Map<LoggerContext, FastLogContext> _map = new IdentityHashMap<>();
  private FastLog _fastLog;

  public FastLogContextFactory(@Nonnull LoggerContextFactory factory) {
    _factory = factory;
  }

  public FastLogContextFactory() {
    this(defaultContextFactory());
  }

  private static LoggerContextFactory defaultContextFactory() {
    LoggerContextFactory factory;
    final SortedMap<Integer, LoggerContextFactory> factories = new TreeMap<>();
    // note that the following initial call to ProviderUtil may block until a Provider has been installed when
    // running in an OSGi environment
    if (ProviderUtil.hasProviders()) {
      for (final Provider provider: ProviderUtil.getProviders()) {
        final Class<? extends LoggerContextFactory> factoryClass = provider.loadLoggerContextFactory();
        if (factoryClass != null && !FastLogContextFactory.class.isAssignableFrom(factoryClass)) {
          try {
            factories.put(provider.getPriority(), factoryClass.newInstance());
          } catch (final Exception e) {
            LOGGER.error(
                "Unable to create class {} specified in provider URL {}",
                factoryClass.getName(),
                provider.getUrl(),
                e);
          }
        }
      }

      if (factories.isEmpty()) {
        LOGGER.error(
            "Log4j2 could not find a logging implementation. "
                + "Please add log4j-core to the classpath. Using SimpleLogger to log to the console...");
        factory = new SimpleLoggerContextFactory();
      } else if (factories.size() == 1) {
        factory = factories.get(factories.lastKey());
      } else {
        final StringBuilder sb = new StringBuilder("Multiple logging implementations found: \n");
        for (final Map.Entry<Integer, LoggerContextFactory> entry: factories.entrySet()) {
          sb.append("Factory: ").append(entry.getValue().getClass().getName());
          sb.append(", Weighting: ").append(entry.getKey()).append('\n');
        }
        factory = factories.get(factories.lastKey());
        sb.append("Using factory: ").append(factory.getClass().getName());
        LOGGER.warn(sb.toString());

      }
    } else {
      LOGGER.error(
          "Log4j2 could not find a logging implementation. "
              + "Please add log4j-core to the classpath. Using SimpleLogger to log to the console...");
      factory = new SimpleLoggerContextFactory();
    }
    return factory;
  }

  public FastLogMBean getManagementMBean() {
    return _fastLog;
  }

  private static Map<LoggerContext, Reference<FastLogContext>> newContextMap(@Nonnull FastLogContextFactory factory) {
    return new WeakHashMap<>();
  }

  private Map<LoggerContext, Reference<FastLogContext>> contextMap() {
    return MAP.get().computeIfAbsent(this, FastLogContextFactory::newContextMap);
  }

  private synchronized FastLogContext getLogContext(@Nonnull LoggerContext context) {
    return _map.computeIfAbsent(context, this::makeLogContext);
  }

  private synchronized boolean removeLogContext(@Nonnull LoggerContext context) {
    return _map.remove(context) != null;
  }

  private FastLogContext makeLogContext(@Nonnull LoggerContext context) {
    if (_fastLog == null) {
      try {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName name = new ObjectName("com.linkedin.alpini.log:type=FastLog");
        if (mbs.isRegistered(name)) {
          LOGGER.info("unregistering previous instance of FastLog mbean");
          mbs.unregisterMBean(name);
        }
        _fastLog = new FastLog();
        mbs.registerMBean(_fastLog, name);
      } catch (Exception ex) {
        LOGGER.error("Error registering mbean", ex);
      }
    }
    return new FastLogContext(getManagementMBean(), context);
  }

  private FastLogContext getContext(LoggerContext context) {
    if (context == null) {
      return null;
    }
    Map<LoggerContext, Reference<FastLogContext>> map = contextMap();
    FastLogContext result;
    do {
      result = map.compute(context, this::computeReference).get();
    } while (result == null);
    return result;
  }

  private Reference<FastLogContext> computeReference(LoggerContext context, Reference<FastLogContext> ref) {
    if (ref == null || ref.get() == null) {
      ref = new WeakReference<>(getLogContext(context));
    }
    return ref;
  }

  @Override
  public LoggerContext getContext(String fqcn, ClassLoader loader, Object externalContext, boolean currentContext) {
    return getContext(_factory.getContext(fqcn, loader, externalContext, currentContext));
  }

  @Override
  public LoggerContext getContext(
      String fqcn,
      ClassLoader loader,
      Object externalContext,
      boolean currentContext,
      URI configLocation,
      String name) {
    return getContext(_factory.getContext(fqcn, loader, externalContext, currentContext, configLocation, name));
  }

  @Override
  public void removeContext(LoggerContext context) {
    if (context instanceof FastLogContext) {
      removeContext(((FastLogContext) context).internalContext());
    } else {
      if (removeLogContext(context)) {
        _factory.removeContext(context);
      }
    }
  }
}
