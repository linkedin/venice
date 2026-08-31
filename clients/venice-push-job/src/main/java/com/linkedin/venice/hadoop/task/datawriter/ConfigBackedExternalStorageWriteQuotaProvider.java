package com.linkedin.venice.hadoop.task.datawriter;

import static com.linkedin.venice.vpj.VenicePushJobConstants.PUSH_JOB_EXTERNAL_STORAGE_WRITE_QUOTA_BYTES_PER_REGION_PER_SECOND;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUSH_JOB_EXTERNAL_STORAGE_WRITE_QUOTA_RECORDS_PER_REGION_PER_SECOND;

import com.linkedin.venice.utils.VeniceProperties;


/**
 * {@link ExternalStorageWriteQuotaProvider} backed by static VPJ config: it reads the per-region record and
 * byte quotas from the job properties once at construction. Absent keys default to {@code -1} (unthrottled).
 */
class ConfigBackedExternalStorageWriteQuotaProvider implements ExternalStorageWriteQuotaProvider {
  private final long recordRatePerSecond;
  private final long byteRatePerSecond;

  ConfigBackedExternalStorageWriteQuotaProvider(VeniceProperties props) {
    this.recordRatePerSecond = props.getLong(PUSH_JOB_EXTERNAL_STORAGE_WRITE_QUOTA_RECORDS_PER_REGION_PER_SECOND, -1);
    this.byteRatePerSecond = props.getLong(PUSH_JOB_EXTERNAL_STORAGE_WRITE_QUOTA_BYTES_PER_REGION_PER_SECOND, -1);
  }

  @Override
  public long getRecordRatePerSecond() {
    return recordRatePerSecond;
  }

  @Override
  public long getByteRatePerSecond() {
    return byteRatePerSecond;
  }
}
