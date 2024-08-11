package com.linkedin.venice.listener;

public class PriorityBasedResponseSchedulerContext {
  public int numThreads;
  public int numQueues;

  public PriorityBasedResponseSchedulerContext(int numThreads, int numQueues) {
    int availableProcessors = Runtime.getRuntime().availableProcessors();
    if (numThreads <= 0) {
      numThreads = availableProcessors;
    }
    if (numQueues <= 0) {
      numQueues = availableProcessors * 2;
    }
    this.numThreads = numThreads;
    this.numQueues = numQueues;
  }
}
