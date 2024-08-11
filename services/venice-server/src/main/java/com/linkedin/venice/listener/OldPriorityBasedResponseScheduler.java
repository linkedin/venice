package com.linkedin.venice.listener;

import com.linkedin.venice.utils.DaemonThreadFactory;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class OldPriorityBasedResponseScheduler {
  private static final Logger LOGGER = LogManager.getLogger(OldPriorityBasedResponseScheduler.class);

  private final Map<EventLoop, LinkedBlockingQueue<ResponseEvent>> responseEventMap;
  private final ThreadPoolExecutor executor;
  private static final long HOLD_THRESHOLD = Duration.ofMillis(100).toNanos();

  public OldPriorityBasedResponseScheduler(
      PriorityBasedResponseSchedulerContext priorityBasedResponseSchedulerContext) {
    responseEventMap = new ConcurrentHashMap<>(priorityBasedResponseSchedulerContext.numQueues);
    executor = createThreadPool(priorityBasedResponseSchedulerContext.numThreads);
    final Set<ChannelHandlerContext> contexts = new HashSet<>();
    executor.execute(() -> {
      while (true) {
        long sleepTime = 5_000_000; // 10 ms
        for (Map.Entry<EventLoop, LinkedBlockingQueue<ResponseEvent>> entry: responseEventMap.entrySet()) {
          LinkedBlockingQueue<ResponseEvent> responseEventQueue = entry.getValue();
          ResponseEvent responseEvent = responseEventQueue.peek();
          int count = 0;
          while (responseEvent != null && System.nanoTime() - responseEvent.timestampInNanos >= HOLD_THRESHOLD
              && count < 1024) {
            responseEvent = responseEventQueue.poll();
            if (responseEvent == null) {
              break;
            }
            count++;
            sleepTime =
                Math.max(0, Math.min(sleepTime, responseEvent.timestampInNanos + HOLD_THRESHOLD - System.nanoTime()));
            try {
              responseEvent.context.write(responseEvent.message);
              contexts.add(responseEvent.context);
            } catch (Exception e) {
              LOGGER.error("Failed to write and flush response", e);
            }
          }

          for (ChannelHandlerContext context: contexts) {
            try {
              context.flush();
            } catch (Exception e) {
              LOGGER.error("Failed to flush response", e);
            }
          }
          contexts.clear();

          //
          // responseEvent = responseEventQueue.poll();
          // if (responseEvent == null) {
          // continue;
          // }
          // ResponseEvent finalResponseEvent = responseEvent;
          // executor.execute(() -> {
          // try {
          // finalResponseEvent.context.writeAndFlush(finalResponseEvent.message);
          // } catch (Exception e) {
          // LOGGER.error("Failed to write and flush response", e);
          // }
          // });
        }

        // sleep for sleepTime nanos
        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
          LOGGER.error("Interrupted while sleeping", e);
        }
      }
    });
  }

  public void writeAndFlush(ChannelHandlerContext context, Object message) {
    EventLoop currentEventLoop = context.channel().eventLoop();
    LinkedBlockingQueue<ResponseEvent> responseEventQueue = responseEventMap.get(currentEventLoop);
    if (responseEventQueue == null) {
      responseEventQueue = new LinkedBlockingQueue<>();
      responseEventMap.put(currentEventLoop, responseEventQueue);
    }
    ResponseEvent responseEvent = new ResponseEvent();
    responseEvent.context = context;
    responseEvent.message = message;
    responseEvent.timestampInNanos = System.nanoTime();
    responseEventQueue.add(responseEvent);
  }

  public static class ResponseEvent {
    public ChannelHandlerContext context;
    public Object message;
    public long timestampInNanos;
  }

  private ThreadPoolExecutor createThreadPool(int threadCount) {
    ThreadPoolExecutor executor = new ThreadPoolExecutor(
        threadCount,
        threadCount,
        5,
        TimeUnit.MINUTES,
        new LinkedBlockingQueue<>(),
        new DaemonThreadFactory("ResponseScheduler"));
    executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
    executor.prestartCoreThread();
    return executor;
  }
}
