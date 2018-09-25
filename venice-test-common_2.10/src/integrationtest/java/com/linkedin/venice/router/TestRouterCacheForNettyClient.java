package com.linkedin.venice.router;

import com.linkedin.venice.router.httpclient.StorageNodeClientType;


public class TestRouterCacheForNettyClient extends TestRouterCache {
  protected StorageNodeClientType getStorageNodeClientType() {
    return StorageNodeClientType.NETTY_4_CLIENT;
  }
}
