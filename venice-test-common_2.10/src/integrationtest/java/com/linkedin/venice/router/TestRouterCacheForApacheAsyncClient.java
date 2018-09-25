package com.linkedin.venice.router;

import com.linkedin.venice.router.httpclient.StorageNodeClientType;


public class TestRouterCacheForApacheAsyncClient extends TestRouterCache {
  protected StorageNodeClientType getStorageNodeClientType() {
    return StorageNodeClientType.APACHE_HTTP_ASYNC_CLIENT;
  }
}
