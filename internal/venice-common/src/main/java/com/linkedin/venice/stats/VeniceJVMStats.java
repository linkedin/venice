package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.List;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Simple class that spins JVM platform stats into Venice stats. Explanations for these
 * stats can be got from {@code BufferPoolMXBean} and {@code MemoryMxBean}
 */

public class VeniceJVMStats extends AbstractVeniceStats {
  private static final Logger LOGGER = LogManager.getLogger(VeniceJVMStats.class);

  /**
   * Returns an estimate of the memory that the Java virtual machine is using
   * for this buffer pool. The value returned by this method may differ
   * from the estimate of the total capacity of
   * the buffers in this pool due to implementation.
   */
  public final Sensor directMemoryUsage;

  /**
   * An estimate of the total capacity of the buffers in this pool.
   * A buffer's capacity is the number of elements it contains and the value
   * returned by this method is an estimate of the total capacity of buffers
   * in the pool in bytes.
   */
  public final Sensor directMemoryCapacity;

  /**
   * An estimate of the number of buffers in the pool.
   */
  public final Sensor directMemoryPoolCount;

  /**
   * The current memory usage of the heap that
   * is used for object allocation. The amount of used memory in the
   * returned memory usage is the amount of memory occupied by both live objects
   * and garbage objects that have not been collected, if any.
   */
  public final Sensor heapUsage;

  private static Long getDirectMemoryBufferPoolBean(Function<BufferPoolMXBean, Long> function) {
    List<BufferPoolMXBean> bufferPoolMXBeans = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
    for (BufferPoolMXBean pool: bufferPoolMXBeans) {
      if (pool.getName().equalsIgnoreCase("direct")) {
        try {
          return function.apply(pool);
        } catch (Throwable e) {
          LOGGER.warn("Could not read direct memory stat with exception:", e);
          return -1L;
        }
      }
    }
    return -1L;
  }

  public VeniceJVMStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);

    directMemoryCapacity = registerSensor(
        new AsyncGauge(
            (c, t) -> getDirectMemoryBufferPoolBean(BufferPoolMXBean::getTotalCapacity),
            "DirectMemoryCapacity"));

    directMemoryUsage = registerSensor(
        new AsyncGauge((c, t) -> getDirectMemoryBufferPoolBean(BufferPoolMXBean::getMemoryUsed), "DirectMemoryUsage"));

    directMemoryPoolCount = registerSensor(
        new AsyncGauge((c, t) -> getDirectMemoryBufferPoolBean(BufferPoolMXBean::getCount), "DirectPoolCount"));

    heapUsage = registerSensor(new AsyncGauge((c, t) -> {
      MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();

      if (memoryBean != null) {
        return memoryBean.getHeapMemoryUsage().getUsed();
      }
      return -1;
    }, "HeapUsage"));

    // Removing maxDirectMemory sensor as sun.misc.VM class is removed in JDK11
    // This makes code compatible with both JDK8 and JDK11
  }
}
