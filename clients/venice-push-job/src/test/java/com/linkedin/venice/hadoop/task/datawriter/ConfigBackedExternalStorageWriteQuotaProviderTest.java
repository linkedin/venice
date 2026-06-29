package com.linkedin.venice.hadoop.task.datawriter;

import static com.linkedin.venice.vpj.VenicePushJobConstants.PUSH_JOB_EXTERNAL_STORAGE_WRITE_QUOTA_BYTES_PER_REGION_PER_SECOND;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUSH_JOB_EXTERNAL_STORAGE_WRITE_QUOTA_RECORDS_PER_REGION_PER_SECOND;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import org.testng.annotations.Test;


public class ConfigBackedExternalStorageWriteQuotaProviderTest {
  @Test
  public void readsBothQuotasFromConfig() {
    Properties props = new Properties();
    props.setProperty(PUSH_JOB_EXTERNAL_STORAGE_WRITE_QUOTA_RECORDS_PER_REGION_PER_SECOND, "1000");
    props.setProperty(PUSH_JOB_EXTERNAL_STORAGE_WRITE_QUOTA_BYTES_PER_REGION_PER_SECOND, "8000");
    ConfigBackedExternalStorageWriteQuotaProvider provider =
        new ConfigBackedExternalStorageWriteQuotaProvider(new VeniceProperties(props));
    assertEquals(provider.getRecordRatePerSecond(), 1000L);
    assertEquals(provider.getByteRatePerSecond(), 8000L);
  }

  @Test
  public void defaultsToDisabledWhenKeysAbsent() {
    ConfigBackedExternalStorageWriteQuotaProvider provider =
        new ConfigBackedExternalStorageWriteQuotaProvider(new VeniceProperties(new Properties()));
    assertEquals(provider.getRecordRatePerSecond(), -1L, "absent record quota defaults to -1 (disabled)");
    assertEquals(provider.getByteRatePerSecond(), -1L, "absent byte quota defaults to -1 (disabled)");
  }

  @Test
  public void readsSingleConfiguredDimension() {
    Properties props = new Properties();
    props.setProperty(PUSH_JOB_EXTERNAL_STORAGE_WRITE_QUOTA_BYTES_PER_REGION_PER_SECOND, "5000");
    ConfigBackedExternalStorageWriteQuotaProvider provider =
        new ConfigBackedExternalStorageWriteQuotaProvider(new VeniceProperties(props));
    assertEquals(provider.getRecordRatePerSecond(), -1L, "unset record dimension stays disabled");
    assertEquals(provider.getByteRatePerSecond(), 5000L);
  }
}
