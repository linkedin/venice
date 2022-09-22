package com.linkedin.alpini.base.pool;

import com.linkedin.alpini.base.pool.impl.AsyncQOSPoolImpl;
import com.linkedin.alpini.base.queuing.QOSPolicy;
import com.linkedin.alpini.consts.QOS;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public interface AsyncQOSPool<T> extends AsyncPool<T> {
  CompletableFuture<T> acquire(String queueName, QOS qos);

  static <T> AsyncQOSPool<T> create(
      LifeCycle<T> lifeCycle,
      QOSPolicy.StaticConfig qosPolicyConfig,
      Executor executor,
      int maxConcurrentCreate,
      int minimumEntities,
      int maximumEntities,
      long maxIdleTime,
      TimeUnit maxIdleUnit) {
    return new AsyncQOSPoolImpl<>(
        lifeCycle,
        qosPolicyConfig,
        executor,
        maxConcurrentCreate,
        minimumEntities,
        maximumEntities,
        maxIdleTime,
        maxIdleUnit);
  }
}
