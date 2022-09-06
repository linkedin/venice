package com.linkedin.venice.streaming;

public class StreamingConstants {
  // Those dummy schema ids are used in batch-get response.
  public static final int NON_EXISTING_KEY_SCHEMA_ID = -1000;
  public static final int STREAMING_FOOTER_SCHEMA_ID = -1001;

  /**
   * Here will use a special key id to differentiate the footer record from the regular record.
   *
   * key id is the only suitable field shared by both batch get response and compute response.
   */
  public static final int KEY_ID_FOR_STREAMING_FOOTER = -1000000;

  private StreamingConstants() {
  }
}
