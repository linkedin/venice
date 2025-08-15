package com.linkedin.venice.utils;

public class BlobTransferCommonUtils {
  /**
   * Enum representing the configuration type for blob transfer in the server.
   * Put this in the common utils package so that it can be used in both router, server and controller.
   */
  public enum BlobTransferInServerConfigType {
    NOT_SPECIFIED, ENABLED, DISABLED
  }
}
