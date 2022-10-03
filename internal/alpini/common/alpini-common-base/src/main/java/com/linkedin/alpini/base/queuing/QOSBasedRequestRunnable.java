package com.linkedin.alpini.base.queuing;

import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.consts.QOS;


/**
 * Wrapper class for "runnables" i.e. a request that must be executed.
 * Also tags a request with a QOS level.
 * @author aauradka
 *
 */
public class QOSBasedRequestRunnable {
  final long _time;
  final Runnable _command;
  final QOS _qos;
  final String _queueName;

  public QOSBasedRequestRunnable(String queueName, QOS qos, Runnable command) {
    _time = Time.nanoTime();
    _queueName = queueName;
    _qos = qos;
    _command = command;
  }

  public final QOS getQOS() {
    return _qos;
  }

  public final Runnable getCommand() {
    return _command;
  }
}
