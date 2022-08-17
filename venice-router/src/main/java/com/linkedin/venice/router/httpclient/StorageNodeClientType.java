package com.linkedin.venice.router.httpclient;

public enum StorageNodeClientType {
  APACHE_HTTP_ASYNC_CLIENT, @Deprecated
  NETTY_4_CLIENT, R2_CLIENT, HTTP_CLIENT_5_CLIENT
}
