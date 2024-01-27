package com.linkedin.alpini.netty4.pool;

import com.linkedin.alpini.consts.QOS;
import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;


/**
 * The interface which describes the public methods of a channel pool manager.
 *
 * @author acurtis on 3/30/17.
 */
public interface ChannelPoolManager {
  AttributeKey<Boolean> FAILED_HEALTH_CHECK = AttributeKey.valueOf(ChannelPoolManager.class, "failedHealthCheck");

  /**
   * The number of threads in the eventloopgroup used by this class.
   * @return thread count
   * @deprecated due to confusing name. Use {@link #subpoolCount}
   */
  @Deprecated
  int executorCount();

  /**
   * The number of subpool threads used per pool by this class.
   * Must be less than or equal to the number of threads in the eventloopgroup.
   * Must be greater than zero.
   * @return thread count
   */
  default int subpoolCount() {
    return executorCount();
  }

  /**
   * Acquire a channel from the pool.
   * @param hostNameAndPort host name and port
   * @param queueName queue name
   * @param qos priority
   * @return future for acquired channel
   */
  @CheckReturnValue
  @Nonnull
  Future<Channel> acquire(@Nonnull String hostNameAndPort, @Nonnull String queueName, @Nonnull QOS qos);

  /**
   * Acquire a channel from the pool.
   * @param hostNameAndPort host name and port
   * @param queueName queue name
   * @param qos priority
   * @return future for acquired channel
   */
  @CheckReturnValue
  @Nonnull
  Future<Channel> acquire(
      @Nonnull EventLoop eventLoop,
      @Nonnull String hostNameAndPort,
      @Nonnull String queueName,
      @Nonnull QOS qos);

  /**
   * Release the channel to the pool.
   * @param channel channel
   * @return future which is completed when done
   */
  @Nonnull
  Future<Void> release(@Nonnull Channel channel);

  /**
   * Open the connection pool to the specified host and port.
   * The default implementation simply acquires a channel from the pool and then immediately releases it.
   * @param hostNameAndPort Host and port string
   * @return future which is completed when done
   */
  @Nonnull
  default Future<Void> open(@Nonnull String hostNameAndPort) {
    Promise<Void> promise = ImmediateEventExecutor.INSTANCE.newPromise();
    acquire(hostNameAndPort, "default", QOS.NORMAL).addListener((Future<Channel> future) -> {
      if (future.isSuccess()) {
        release(future.getNow()).addListener(released -> {
          if (released.isSuccess()) {
            promise.setSuccess(null);
          } else {
            promise.setFailure(released.cause());
          }
        });
      } else {
        promise.setFailure(future.cause());
      }
    });
    return promise;
  }

  /**
   * Close the connection pool to the specified host and port.
   * @param hostNameAndPort Host and port string
   * @return future which is completed when done
   */
  @Nonnull
  Future<Void> close(@Nonnull String hostNameAndPort);
}
