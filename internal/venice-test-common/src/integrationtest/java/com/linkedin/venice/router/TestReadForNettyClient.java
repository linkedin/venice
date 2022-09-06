package com.linkedin.venice.router;

import com.linkedin.venice.router.httpclient.StorageNodeClientType;


public class TestReadForNettyClient extends TestRead {
  protected StorageNodeClientType getStorageNodeClientType() {
    return StorageNodeClientType.NETTY_4_CLIENT;
  }
}
