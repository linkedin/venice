package com.linkedin.alpini.base.registry;

import com.linkedin.alpini.base.concurrency.ScheduledExecutorService;


/**
 * A {@link ScheduledExecutorService} interface which also extends the
 * {@link Shutdownable} interface.
 *
 * @author Antony T Curtis &lt;acurtis@linkedin.com&gt;
 */
public interface ShutdownableScheduledExecutorService extends ScheduledExecutorService, ShutdownableExecutorService {
}
