package com.linkedin.venice.utils;

import com.linkedin.davinci.notifier.LogNotifier;


public class ExceptionCaptorNotifier extends LogNotifier {
  private Exception latestException;

  @Override
  public void error(String kafkaTopic, int partitionId, String message, Exception ex) {
    this.latestException = ex;
    super.error(kafkaTopic, partitionId, message, ex);
  }

  public Exception getLatestException() {
    return this.latestException;
  }
}
