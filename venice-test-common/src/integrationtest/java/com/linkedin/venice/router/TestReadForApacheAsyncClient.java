package com.linkedin.venice.router;

import com.linkedin.venice.router.httpclient.StorageNodeClientType;

public class TestReadForApacheAsyncClient extends TestRead{
  protected StorageNodeClientType getStorageNodeClientType() {
    return StorageNodeClientType.APACHE_HTTP_ASYNC_CLIENT;
  }
}
