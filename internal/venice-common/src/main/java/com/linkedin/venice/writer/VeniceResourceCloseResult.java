package com.linkedin.venice.writer;

public enum VeniceResourceCloseResult {
  SUCCESS(0), FAILURE(1), TIMEOUT(2), ALREADY_CLOSED(3);

  private final int statusCode;

  VeniceResourceCloseResult(int statusCode) {
    this.statusCode = statusCode;
  }

  public int getStatusCode() {
    return statusCode;
  }
}
