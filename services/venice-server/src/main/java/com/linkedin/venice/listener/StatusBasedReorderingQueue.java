package com.linkedin.venice.listener;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;


public class StatusBasedReorderingQueue {
  public enum NettyWriteEventType {
    OK, NOT_OK
  }

  // pojo: status and timestamp in nanoseconds
  public static class WriteAndFlushEvent implements Comparable<WriteAndFlushEvent> {
    public NettyWriteEventType status;
    public long timestamp;

    public WriteAndFlushEvent(NettyWriteEventType status, long timestamp) {
      this.status = status;
      this.timestamp = timestamp;
    }

    @Override
    public int compareTo(WriteAndFlushEvent other) {
      // If both statuses are the same, compare by timestamp (earlier is better)
      if (this.status == other.status) {
        return Long.compare(this.timestamp, other.timestamp);
      }

      // Special handling for OK and NOT_OK comparison
      if (this.status == NettyWriteEventType.OK && other.status == NettyWriteEventType.NOT_OK) {
        // OK event is normally prioritized, unless NOT_OK is within 100ms after OK
        long timeDeltaInNs = this.timestamp - other.timestamp;
        if (timeDeltaInNs >= 10) {
          return 1; // prioritized NOT_OK since it has timestamp smaller by 10ms
        } else {
          return -1;
        }
      }

      if (this.status == NettyWriteEventType.NOT_OK && other.status == NettyWriteEventType.OK) {
        // NOT_OK event is normally deprioritized, unless OK is within 100ms after NOT_OK
        long timeDeltaInNs = other.timestamp - this.timestamp;
        if (timeDeltaInNs >= 10) {
          return -1; // prioritized NOT_OK since it has timestamp smaller by 10ms
        } else {
          return 1;
        }
      }
      return Long.compare(this.timestamp, other.timestamp);
    }
  }

  public static void main(String[] args) {
    // create 10 sample events
    List<WriteAndFlushEvent> events = new ArrayList<>(16);
    events.add(new WriteAndFlushEvent(NettyWriteEventType.OK, 1));
    events.add(new WriteAndFlushEvent(NettyWriteEventType.OK, 2));
    events.add(new WriteAndFlushEvent(NettyWriteEventType.NOT_OK, 3));
    events.add(new WriteAndFlushEvent(NettyWriteEventType.OK, 4));
    events.add(new WriteAndFlushEvent(NettyWriteEventType.OK, 12));
    events.add(new WriteAndFlushEvent(NettyWriteEventType.OK, 13));
    events.add(new WriteAndFlushEvent(NettyWriteEventType.NOT_OK, 13));
    events.add(new WriteAndFlushEvent(NettyWriteEventType.OK, 14));
    events.add(new WriteAndFlushEvent(NettyWriteEventType.NOT_OK, 14));
    events.add(new WriteAndFlushEvent(NettyWriteEventType.OK, 26));
    events.add(new WriteAndFlushEvent(NettyWriteEventType.OK, 23));

    PriorityBlockingQueue<WriteAndFlushEvent> queue = new PriorityBlockingQueue<>(events.size());
    while (!events.isEmpty()) {
      queue.add(events.remove(0));
    }

    while (!queue.isEmpty()) {
      WriteAndFlushEvent event = queue.poll();
      System.out.println("Status: " + event.status + ", Timestamp: " + event.timestamp);
    }

  }
}
