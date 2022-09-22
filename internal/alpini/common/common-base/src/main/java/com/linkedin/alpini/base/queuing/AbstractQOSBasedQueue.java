package com.linkedin.alpini.base.queuing;

import com.linkedin.alpini.consts.QOS;
import java.util.List;
import java.util.Map;


/**
 * Abstract base class for QOS priority queues.
 *
 * @author acurtis
 */
public abstract class AbstractQOSBasedQueue<T extends QOSBasedRequestRunnable> extends AbstractQOS
    implements SimpleQueue<T> {
  public AbstractQOSBasedQueue(Map<QOS, Integer> qosBasedAllocations) {
    super(qosBasedAllocations);

    _log.debug("QOS Based Allocations are : {}", _qosBasedAllocations);
  }

  protected String getQueueName(T e) {
    return e._queueName != null ? e._queueName : "";
  }

  protected QOS getQOS(T e) {
    return e._qos != null ? e._qos : QOS.NORMAL;
  }

  @Override
  public T poll() {
    final List<QOS> order = getQueuePollOrder();
    T next = getElement(order);

    if (next != null) {
      _log.trace("Polled {} QOS element from queue {}", next._qos, next._queueName);
    }

    return next;
  }

  /**
   * Returns or removes an element from one of the specified queues.
   * @param viewOrder order to check the queues in
   * @return the next element; null if queue is empty
   */
  abstract T getElement(List<QOS> viewOrder);

}
