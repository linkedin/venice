package com.linkedin.venice;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.alpini.base.misc.CollectionUtil;
import com.linkedin.venice.utils.VeniceEnumValueTest;
import java.util.Map;
import org.testng.annotations.Test;


public class PushJobCheckPointsTest extends VeniceEnumValueTest<PushJobCheckpoints> {
  public PushJobCheckPointsTest() {
    super(PushJobCheckpoints.class);
  }

  @Override
  protected Map<Integer, PushJobCheckpoints> expectedMapping() {
    return CollectionUtil.<Integer, PushJobCheckpoints>mapBuilder()
        .put(0, PushJobCheckpoints.INITIALIZE_PUSH_JOB)
        .put(1, PushJobCheckpoints.NEW_VERSION_CREATED)
        .put(2, PushJobCheckpoints.START_DATA_WRITER_JOB)
        .put(3, PushJobCheckpoints.DATA_WRITER_JOB_COMPLETED)
        .put(4, PushJobCheckpoints.START_JOB_STATUS_POLLING)
        .put(5, PushJobCheckpoints.JOB_STATUS_POLLING_COMPLETED)
        .put(6, PushJobCheckpoints.START_VALIDATE_SCHEMA_AND_BUILD_DICT_MAP_JOB)
        .put(7, PushJobCheckpoints.VALIDATE_SCHEMA_AND_BUILD_DICT_MAP_JOB_COMPLETED)
        .put(-1, PushJobCheckpoints.QUOTA_EXCEEDED)
        .put(-2, PushJobCheckpoints.WRITE_ACL_FAILED)
        .put(-3, PushJobCheckpoints.DUP_KEY_WITH_DIFF_VALUE)
        .put(-4, PushJobCheckpoints.INPUT_DATA_SCHEMA_VALIDATION_FAILED)
        .put(-5, PushJobCheckpoints.EXTENDED_INPUT_DATA_SCHEMA_VALIDATION_FAILED)
        .put(-6, PushJobCheckpoints.RECORD_TOO_LARGE_FAILED)
        .put(-7, PushJobCheckpoints.CONCURRENT_BATCH_PUSH)
        .put(-8, PushJobCheckpoints.DATASET_CHANGED)
        .put(-9, PushJobCheckpoints.INVALID_INPUT_FILE)
        .put(-10, PushJobCheckpoints.ZSTD_DICTIONARY_CREATION_FAILED)
        .put(-11, PushJobCheckpoints.DVC_INGESTION_ERROR_DISK_FULL)
        .put(-12, PushJobCheckpoints.DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED)
        .put(-13, PushJobCheckpoints.DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES)
        .put(-14, PushJobCheckpoints.DVC_INGESTION_ERROR_OTHER)
        .build();
  }

  @Test
  public void testDefaultPushJobUserErrorCheckpoints() {
    for (PushJobCheckpoints checkpoint: PushJobCheckpoints.values()) {
      switch (checkpoint) {
        case QUOTA_EXCEEDED:
        case WRITE_ACL_FAILED:
        case DUP_KEY_WITH_DIFF_VALUE:
        case INPUT_DATA_SCHEMA_VALIDATION_FAILED:
        case EXTENDED_INPUT_DATA_SCHEMA_VALIDATION_FAILED:
        case RECORD_TOO_LARGE_FAILED:
        case CONCURRENT_BATCH_PUSH:
        case DATASET_CHANGED:
        case INVALID_INPUT_FILE:
        case DVC_INGESTION_ERROR_DISK_FULL:
        case DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED:
          assertTrue(PushJobCheckpoints.DEFAULT_PUSH_JOB_USER_ERROR_CHECKPOINTS.contains(checkpoint));
          break;

        case INITIALIZE_PUSH_JOB:
        case NEW_VERSION_CREATED:
        case START_DATA_WRITER_JOB:
        case DATA_WRITER_JOB_COMPLETED:
        case START_JOB_STATUS_POLLING:
        case JOB_STATUS_POLLING_COMPLETED:
        case START_VALIDATE_SCHEMA_AND_BUILD_DICT_MAP_JOB:
        case VALIDATE_SCHEMA_AND_BUILD_DICT_MAP_JOB_COMPLETED:
        case ZSTD_DICTIONARY_CREATION_FAILED:
        case DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES:
        case DVC_INGESTION_ERROR_OTHER:
          assertFalse(PushJobCheckpoints.DEFAULT_PUSH_JOB_USER_ERROR_CHECKPOINTS.contains(checkpoint));
          break;

        default:
          throw new IllegalArgumentException("Unknown checkpoint: " + checkpoint);
      }
    }
  }
}
