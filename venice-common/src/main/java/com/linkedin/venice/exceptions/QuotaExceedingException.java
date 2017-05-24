package com.linkedin.venice.exceptions;

import org.apache.http.HttpStatus;


public class QuotaExceedingException extends VeniceException {
  public QuotaExceedingException(String throttlerName, String currentUsage, String quota) {
    super(getErrorMessage(throttlerName, currentUsage, quota));
  }

  public QuotaExceedingException(String throttlerName, String currentUsage, String quota, Throwable e) {
    super(getErrorMessage(throttlerName, currentUsage, quota), e);
  }

  @Override
  public int getHttpStatusCode() {
    return HttpStatus.SC_FORBIDDEN;
  }

  public static String getErrorMessage(String throttlerName, String currentUsage, String quota) {
    return "Throttler:" + throttlerName + " quota exceeded. Current usage=" + currentUsage + ", quota=" + quota;
  }
}
