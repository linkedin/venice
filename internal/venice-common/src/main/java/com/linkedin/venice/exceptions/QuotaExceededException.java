package com.linkedin.venice.exceptions;

import io.netty.handler.codec.http.HttpResponseStatus;


public class QuotaExceededException extends VeniceException {
  public QuotaExceededException(String throttlerName, String currentUsage, String quota) {
    super(getErrorMessage(throttlerName, currentUsage, quota));
  }

  public QuotaExceededException(String throttlerName, String currentUsage, String quota, Throwable e) {
    super(getErrorMessage(throttlerName, currentUsage, quota), e);
  }

  @Override
  public int getHttpStatusCode() {
    return HttpResponseStatus.TOO_MANY_REQUESTS.code();
  }

  public static String getErrorMessage(String throttlerName, String currentUsage, String quota) {
    return "Throttler:" + throttlerName + " quota exceeded. Current usage=" + currentUsage + ", quota=" + quota;
  }
}
