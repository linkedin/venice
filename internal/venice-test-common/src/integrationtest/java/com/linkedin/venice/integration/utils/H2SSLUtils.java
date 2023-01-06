package com.linkedin.venice.integration.utils;

import com.linkedin.venice.security.DefaultSSLFactory;
import com.linkedin.venice.security.SSLConfig;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SslUtils;


public class H2SSLUtils {
  /**
   * This function will return a http/2 compatible {@link SSLFactory}.
   * TODO: let us use this factory everywhere.
   */
  public static SSLFactory getLocalHttp2SslFactory() throws Exception {
    SSLConfig sslEngineConfig = SslUtils.getLocalSslConfig();
    return new DefaultSSLFactory(sslEngineConfig);
  }
}
