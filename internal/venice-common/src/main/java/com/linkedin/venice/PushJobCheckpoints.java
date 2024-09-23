package com.linkedin.venice;

import com.linkedin.venice.utils.EnumUtils;
import com.linkedin.venice.utils.VeniceEnumValue;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * Different successful checkpoints and known error scenarios of the VPJ flow.
 * 1. The enums are not sequential
 * 2. Non-negative enums are successful checkpoints
 * 3. Negative enums are error scenarios (Can be user or system errors)
 */
public enum PushJobCheckpoints implements VeniceEnumValue {
  INITIALIZE_PUSH_JOB(0), NEW_VERSION_CREATED(1), START_DATA_WRITER_JOB(2), DATA_WRITER_JOB_COMPLETED(3),
  START_JOB_STATUS_POLLING(4), JOB_STATUS_POLLING_COMPLETED(5), START_VALIDATE_SCHEMA_AND_BUILD_DICT_MAP_JOB(6),
  VALIDATE_SCHEMA_AND_BUILD_DICT_MAP_JOB_COMPLETED(7), QUOTA_EXCEEDED(-1), WRITE_ACL_FAILED(-2),
  DUP_KEY_WITH_DIFF_VALUE(-3), INPUT_DATA_SCHEMA_VALIDATION_FAILED(-4),
  EXTENDED_INPUT_DATA_SCHEMA_VALIDATION_FAILED(-5), RECORD_TOO_LARGE_FAILED(-6), CONCURRENT_BATCH_PUSH(-7),
  DATASET_CHANGED(-8), INVALID_INPUT_FILE(-9), ZSTD_DICTIONARY_CREATION_FAILED(-10), DVC_INGESTION_ERROR_DISK_FULL(-11),
  DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED(-12), DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES(-13),
  DVC_INGESTION_ERROR_OTHER(-14);

  private final int value;

  /**
   * Default set checkpoints to define push job failures are user errors which can be overridden via controller config
   * {@link ConfigKeys#PUSH_JOB_FAILURE_CHECKPOINTS_TO_DEFINE_USER_ERROR}
   */
  public static final Set<PushJobCheckpoints> DEFAULT_PUSH_JOB_USER_ERROR_CHECKPOINTS = Collections.unmodifiableSet(
      new HashSet<>(
          Arrays.asList(
              QUOTA_EXCEEDED,
              WRITE_ACL_FAILED,
              DUP_KEY_WITH_DIFF_VALUE,
              INPUT_DATA_SCHEMA_VALIDATION_FAILED,
              EXTENDED_INPUT_DATA_SCHEMA_VALIDATION_FAILED,
              RECORD_TOO_LARGE_FAILED,
              CONCURRENT_BATCH_PUSH,
              DATASET_CHANGED,
              INVALID_INPUT_FILE,
              DVC_INGESTION_ERROR_DISK_FULL,
              DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED)));

  private static final Map<Integer, PushJobCheckpoints> TYPES =
      EnumUtils.getEnumValuesSparseList(PushJobCheckpoints.class);

  PushJobCheckpoints(int value) {
    this.value = value;
  }

  @Override
  public int getValue() {
    return value;
  }

  public static PushJobCheckpoints valueOf(int value) {
    return EnumUtils.valueOf(TYPES, value, PushJobCheckpoints.class);
  }
}
