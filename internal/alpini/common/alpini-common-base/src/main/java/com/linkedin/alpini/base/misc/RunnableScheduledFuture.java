package com.linkedin.alpini.base.misc;

/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public interface RunnableScheduledFuture<V>
    extends ScheduledFuture<V>, java.util.concurrent.RunnableScheduledFuture<V> {
}
