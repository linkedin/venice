package com.linkedin.alpini.router.monitoring;

/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public interface ScatterGatherStatsMBean {
  long getTotalRetries();

  long getTotalRetriedKeys();

  long getTotalRetriesDiscarded();

  long getTotalRetriesWinner();

  long getTotalRetriesError();
}
