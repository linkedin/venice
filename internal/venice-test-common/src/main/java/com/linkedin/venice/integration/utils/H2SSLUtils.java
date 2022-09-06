package com.linkedin.venice.integration.utils;

import com.linkedin.ddsstorage.linetty4.ssl.SslEngineComponentFactoryImpl;
import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import com.linkedin.security.ssl.access.control.SSLEngineComponentFactoryImpl;
import com.linkedin.venice.utils.SslUtils;


public class H2SSLUtils {
  /**
   * This function will return a http/2 compatible {@link SSLEngineComponentFactory}.
   * TODO: let us use this factory everywhere.
   */
  public static SSLEngineComponentFactory getLocalHttp2SslFactory() throws Exception {
    SSLEngineComponentFactoryImpl.Config sslEngineConfig = SslUtils.getLocalSslConfig();
    SslEngineComponentFactoryImpl.Config http2SslEngineConfig = new SslEngineComponentFactoryImpl.Config();
    http2SslEngineConfig.setSslEnabled(sslEngineConfig.getSslEnabled());
    http2SslEngineConfig.setKeyStoreType(sslEngineConfig.getKeyStoreType());
    http2SslEngineConfig.setKeyStoreData(sslEngineConfig.getKeyStoreData());
    http2SslEngineConfig.setKeyStorePassword(sslEngineConfig.getKeyStorePassword());
    http2SslEngineConfig.setKeyStoreFilePath(sslEngineConfig.getKeyStoreFilePath());
    http2SslEngineConfig.setTrustStoreFilePath(sslEngineConfig.getTrustStoreFilePath());
    http2SslEngineConfig.setTrustStoreFilePassword(sslEngineConfig.getTrustStoreFilePassword());

    return new SslEngineComponentFactoryImpl(http2SslEngineConfig);
  }
}
