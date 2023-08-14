package com.linkedin.venice.schema.rmd;

/**
 * This is an util class that contains constants related to replication metadata. For example, field names in
 * a replication metadata record.
 */
public class RmdConstants {
  private RmdConstants() {
    // Utility class
  }

  // Replication metadata timestamp field name.
  public static final int TIMESTAMP_FIELD_POS = 0;
  public static final String TIMESTAMP_FIELD_NAME = "timestamp";

  // Replication metadata checkpoint vector field name.
  public static final int REPLICATION_CHECKPOINT_VECTOR_FIELD_POS = 1;
  public static final String REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME = "replication_checkpoint_vector";
}
