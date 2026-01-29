package com.linkedin.venice.spark.utils;

import com.linkedin.venice.hadoop.PushJobSetting;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import org.apache.avro.Schema;


/**
 * Utility class for handling replication metadata (RMD) related operations in Venice push jobs.
 */
public final class RmdPushUtils {
  private static final Schema LONG_SCHEMA = Schema.create(Schema.Type.LONG);

  public static RecordDeserializer<Long> getDeserializerForLogicalTimestamp() {
    return FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(LONG_SCHEMA, LONG_SCHEMA);
  }

  public static RecordSerializer<Long> getSerializerForLogicalTimestamp() {
    return FastSerializerDeserializerFactory.getFastAvroGenericSerializer(LONG_SCHEMA);
  }

  public static Schema getInputRmdSchema(PushJobSetting pushJobSetting) {
    if (!rmdFieldPresent(pushJobSetting)) {
      throw new IllegalArgumentException(
          "Push job setting missing rmd field. Please set the rmd field in the job properties.");
    }
    return pushJobSetting.inputDataSchema.getField(pushJobSetting.rmdField).schema();
  }

  public static boolean containsLogicalTimestamp(PushJobSetting pushJobSetting) {
    if (!rmdFieldPresent(pushJobSetting)) {
      return false;
    }

    Schema inputRmdSchema = getInputRmdSchema(pushJobSetting);
    return inputRmdSchema.getType() == Schema.Type.LONG;
  }

  public static boolean rmdFieldPresent(PushJobSetting pushJobSetting) {
    return pushJobSetting.rmdField != null && !pushJobSetting.rmdField.isEmpty();
  }
}
