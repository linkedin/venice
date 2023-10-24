package com.linkedin.venice.utils;

public enum VeniceResourceCloseResult {
  SUCCESS(0), ALREADY_CLOSED(1), FAILED(2), UNKNOWN(3);

  private final int statusCode;

  VeniceResourceCloseResult(int statusCode) {
    this.statusCode = statusCode;
  }

  public int getStatusCode() {
    return statusCode;
  }
}
