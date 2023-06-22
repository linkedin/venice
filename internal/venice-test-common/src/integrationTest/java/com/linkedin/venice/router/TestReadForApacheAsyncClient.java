package com.linkedin.venice.router;

import com.linkedin.venice.router.httpclient.StorageNodeClientType;


public class TestReadForApacheAsyncClient extends TestRead {
  @Override
  protected StorageNodeClientType getStorageNodeClientType() {
    return StorageNodeClientType.APACHE_HTTP_ASYNC_CLIENT;
  }

  @Override
  protected boolean isRouterHttp2Enabled() {
    return true;
  }
}
