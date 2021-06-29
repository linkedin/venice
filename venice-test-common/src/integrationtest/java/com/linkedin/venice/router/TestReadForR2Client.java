package com.linkedin.venice.router;

import com.linkedin.venice.router.httpclient.StorageNodeClientType;

public class TestReadForR2Client extends TestRead {
  protected StorageNodeClientType getStorageNodeClientType() {
    return StorageNodeClientType.R2_CLIENT;
  }

  protected boolean isHttp2Enabled() {
    return true;
  }
}
