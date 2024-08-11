package com.linkedin.venice.listener;

import static com.linkedin.venice.listener.NettWriteTask.*;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class PriorityBasedResponseScheduler {
  private static final Logger LOGGER = LogManager.getLogger(PriorityBasedResponseScheduler.class);

  private final Map<EventLoop, PriorityBasedThreadPoolExecutor> eventLoopFlushExecutorMap;

  public PriorityBasedResponseScheduler(PriorityBasedResponseSchedulerContext priorityBasedResponseSchedulerContext) {
    eventLoopFlushExecutorMap = new ConcurrentHashMap<>(priorityBasedResponseSchedulerContext.numQueues);
  }

  public void writeAndFlush(
      StatusBasedReorderingQueue.NettyWriteEventType status,
      long arrivalTime,
      ChannelHandlerContext context,
      Object message) {
    EventLoop currentEventLoop = context.channel().eventLoop();
    PriorityBasedThreadPoolExecutor responseEventQueue =
        eventLoopFlushExecutorMap.computeIfAbsent(currentEventLoop, key -> new PriorityBasedThreadPoolExecutor(1));
    responseEventQueue.submit(new NettWriteTask(arrivalTime, status, context, message));
  }
}
