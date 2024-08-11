package com.linkedin.venice.listener;

import static com.linkedin.venice.listener.StatusBasedReorderingQueue.NettyWriteEventType;

import com.linkedin.venice.utils.DaemonThreadFactory;
import io.netty.channel.ChannelHandlerContext;
import java.util.concurrent.FutureTask;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class NettWriteTask implements Runnable {
  public static final long TIME_DELTA = 100_000_000; // 100ms
  long arrivalTime;
  NettyWriteEventType status;
  ChannelHandlerContext ctx;
  Object payload;

  @Override
  public void run() {
    // System.out.println("Running: " + status + " tid: " + Thread.currentThread().getName());
    ctx.writeAndFlush(payload);
  }

  public NettWriteTask(long arrivalTime, NettyWriteEventType status, ChannelHandlerContext ctx, Object payload) {
    this.arrivalTime = arrivalTime;
    this.status = status;
    this.ctx = ctx;
    this.payload = payload;
  }

  public static class PriorityBasedThreadPoolExecutor extends ThreadPoolExecutor {
    public PriorityBasedThreadPoolExecutor(int numThreads) {
      super(
          numThreads,
          numThreads,
          1000,
          TimeUnit.SECONDS,
          new PriorityBlockingQueue<Runnable>(),
          new DaemonThreadFactory("PriorityBasedThreadPoolExecutor"));
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
      return new NettyWriteFutureTask<>(runnable);
    }
  }

  public static class NettyWriteFutureTask<T> extends FutureTask<T> implements Comparable<NettyWriteFutureTask<T>> {
    private final NettWriteTask nettWriteTask;

    public NettyWriteFutureTask(Runnable task) {
      super(task, null);
      this.nettWriteTask = (NettWriteTask) task;
    }

    @Override
    public int compareTo(NettyWriteFutureTask other) {
      // System.out.println("Comparing: " + this.nettWriteTask.status + " with " + other.nettWriteTask.status);
      // If both statuses are the same, compare by timestamp (earlier is better)
      if (this.nettWriteTask.status == other.nettWriteTask.status) {
        return Long.compare(this.nettWriteTask.arrivalTime, other.nettWriteTask.arrivalTime);
      }

      // Special handling for OK and NOT_OK comparison
      if (this.nettWriteTask.status == NettyWriteEventType.OK
          && other.nettWriteTask.status == NettyWriteEventType.NOT_OK) {
        // OK event is normally prioritized, unless NOT_OK is within 100ms after OK
        long timeDeltaInNs = this.nettWriteTask.arrivalTime - other.nettWriteTask.arrivalTime;
        if (timeDeltaInNs >= TIME_DELTA) {
          return 1; // prioritized NOT_OK since it has timestamp smaller by 10ms
        } else {
          return -1;
        }
      }

      if (this.nettWriteTask.status == NettyWriteEventType.NOT_OK
          && other.nettWriteTask.status == NettyWriteEventType.OK) {
        // NOT_OK event is normally deprioritized, unless OK is within 100ms after NOT_OK
        long timeDeltaInNs = other.nettWriteTask.arrivalTime - this.nettWriteTask.arrivalTime;
        if (timeDeltaInNs >= TIME_DELTA) {
          return -1; // prioritized NOT_OK since it has timestamp smaller by 10ms
        } else {
          return 1;
        }
      }
      return Long.compare(this.nettWriteTask.arrivalTime, other.nettWriteTask.arrivalTime);
    }
  }

  public static void main(String[] args) throws InterruptedException {
    System.out.println("Hello, World!");
    ThreadPoolExecutor executor = new PriorityBasedThreadPoolExecutor(1);
    executor.submit(new NettWriteTask(1, NettyWriteEventType.OK, null, null));
    executor.submit(new NettWriteTask(2, NettyWriteEventType.OK, null, null));
    executor.submit(new NettWriteTask(3, NettyWriteEventType.NOT_OK, null, null));
    executor.submit(new NettWriteTask(4, NettyWriteEventType.OK, null, null));
    executor.submit(new NettWriteTask(12, NettyWriteEventType.OK, null, null));
    executor.submit(new NettWriteTask(13, NettyWriteEventType.OK, null, null));
    executor.submit(new NettWriteTask(13, NettyWriteEventType.NOT_OK, null, null));
    executor.submit(new NettWriteTask(14, NettyWriteEventType.OK, null, null));
    executor.submit(new NettWriteTask(14, NettyWriteEventType.NOT_OK, null, null));
    executor.submit(new NettWriteTask(26, NettyWriteEventType.OK, null, null));
    executor.submit(new NettWriteTask(23, NettyWriteEventType.OK, null, null));

    Thread.sleep(30_000);

  }
}
