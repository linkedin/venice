package com.linkedin.venice.spark.utils;

import com.linkedin.venice.hadoop.PushJobSetting;
import org.apache.avro.Schema;


/**
 * Utility class for handling replication metadata (RMD) related operations in Venice push jobs.
 */
public class RmdPushUtils {
  public static Schema getInputRmdSchema(PushJobSetting pushJobSetting) {
    if (!rmdFieldPresent(pushJobSetting)) {
      throw new IllegalArgumentException(
          "Push job setting missing rmd field. Please set the rmd field in the job properties.");
    }
    return pushJobSetting.inputDataSchema.getField(pushJobSetting.rmdField).schema();
  }

  public static boolean containsLogicalTimestamp(PushJobSetting pushJobSetting) {
    Schema inputRmdSchema = getInputRmdSchema(pushJobSetting);
    return inputRmdSchema.getType() == Schema.Type.LONG;
  }

  public static boolean rmdFieldPresent(PushJobSetting pushJobSetting) {
    return pushJobSetting.rmdField != null && !pushJobSetting.rmdField.isEmpty();
  }
}
