package com.linkedin.alpini.router.monitoring;

/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public interface ScatterGatherStatsMBean {
  long getTotalRetries();

  long getTotalRetriedKeys();

  long getTotalRetriedOn429();

  long getTotalRetriesDiscarded();

  long getTotalRetriesWinner();

  long getTotalRetriesError();

  long getTotalDiscardedBytes();

  long getTotalRequestsSent();

  long getTotalRequestsReceived();

  long getTotalRetriesOn503();

  long getTotalRetriesOn503Winner();

  long getTotalRetriesOn503Error();
}
