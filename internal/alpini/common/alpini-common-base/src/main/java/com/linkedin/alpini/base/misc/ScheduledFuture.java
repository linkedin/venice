package com.linkedin.alpini.base.misc;

import com.linkedin.alpini.base.concurrency.AsyncFuture;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public interface ScheduledFuture<V> extends java.util.concurrent.ScheduledFuture<V>, AsyncFuture<V> {
}
