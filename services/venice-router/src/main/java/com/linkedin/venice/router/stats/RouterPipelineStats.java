package com.linkedin.venice.router.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.TehutiUtils;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;


public class RouterPipelineStats extends AbstractVeniceStats {
  private final Sensor preHandlerLatencySensor;
  private final Sensor handlerChainLatencySensor;
  private final ConcurrentMap<String, Sensor> handlerLatencySensors = new ConcurrentHashMap<>();

  public RouterPipelineStats(
      MetricsRepository metricsRepository,
      String name,
      MultithreadEventLoopGroup workerEventLoopGroup,
      Supplier<Integer> unwritableChannelCountSupplier) {
    super(metricsRepository, name);

    // EventLoop pool stats (analogous to ThreadPoolStats for ThreadPoolExecutor)
    registerSensor(
        new AsyncGauge((ignored1, ignored2) -> workerEventLoopGroup.executorCount(), "eventloop_worker_count"));

    registerSensor(new AsyncGauge((ignored1, ignored2) -> {
      int active = 0;
      for (EventExecutor executor: workerEventLoopGroup) {
        if (executor instanceof SingleThreadEventExecutor) {
          if (((SingleThreadEventExecutor) executor).pendingTasks() > 0) {
            active++;
          }
        }
      }
      return active;
    }, "eventloop_active_count"));

    registerSensor(new AsyncGauge((ignored1, ignored2) -> {
      long totalPending = 0;
      for (EventExecutor executor: workerEventLoopGroup) {
        if (executor instanceof SingleThreadEventExecutor) {
          totalPending += ((SingleThreadEventExecutor) executor).pendingTasks();
        }
      }
      return totalPending;
    }, "eventloop_pending_tasks_total"));

    registerSensor(new AsyncGauge((ignored1, ignored2) -> {
      long totalPending = 0;
      int count = 0;
      for (EventExecutor executor: workerEventLoopGroup) {
        if (executor instanceof SingleThreadEventExecutor) {
          totalPending += ((SingleThreadEventExecutor) executor).pendingTasks();
          count++;
        }
      }
      return count > 0 ? (double) totalPending / count : 0;
    }, "eventloop_pending_tasks_avg"));

    registerSensor(new AsyncGauge((ignored1, ignored2) -> {
      int maxPending = 0;
      for (EventExecutor executor: workerEventLoopGroup) {
        if (executor instanceof SingleThreadEventExecutor) {
          maxPending = Math.max(maxPending, ((SingleThreadEventExecutor) executor).pendingTasks());
        }
      }
      return maxPending;
    }, "eventloop_pending_tasks_max"));

    // Infrastructure gauge: unwritable channel count
    registerSensor(
        new AsyncGauge((ignored1, ignored2) -> unwritableChannelCountSupplier.get(), "unwritable_channel_count"));

    // Latency sub-breakdown sensors (P50/P95/P99)
    preHandlerLatencySensor =
        registerSensor("pre_handler_latency", TehutiUtils.getPercentileStat(getName(), "pre_handler_latency"));
    handlerChainLatencySensor =
        registerSensor("handler_chain_latency", TehutiUtils.getPercentileStat(getName(), "handler_chain_latency"));
  }

  public void recordPreHandlerLatency(double latencyMs) {
    preHandlerLatencySensor.record(latencyMs);
  }

  public void recordHandlerChainLatency(double latencyMs) {
    handlerChainLatencySensor.record(latencyMs);
  }

  public void recordHandlerLatency(String handlerName, double latencyMs) {
    handlerLatencySensors
        .computeIfAbsent(
            handlerName,
            name -> registerSensor(
                "handler_latency_" + name,
                TehutiUtils.getPercentileStat(getName(), "handler_latency_" + name)))
        .record(latencyMs);
  }
}
