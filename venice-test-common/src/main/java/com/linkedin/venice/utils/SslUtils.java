package com.linkedin.venice.utils;

import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import com.linkedin.security.ssl.access.control.SSLEngineComponentFactoryImpl;
import com.linkedin.venice.exceptions.VeniceException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;


public class SslUtils {
  // Self-signed cert, expires 2027, use keystore as truststore since self-signed.
  // cert has CN=localhost
  private static final String LOCAL_PASSWORD = "dev_pass";
  private static final String LOCAL_KEYSTORE_P12 = "localhost.p12";
  private static final String LOCAL_KEYSTORE_JKS = "localhost.jks";
  private static final String LOCAL_CERT = "localhost.cert";
  private static final String LOCAL_KEY = "localhost.key";

  public static SSLEngineComponentFactory getLocalSslFactory() {
    String keyStorePath = getPathForResource(LOCAL_KEYSTORE_JKS);

    SSLEngineComponentFactoryImpl.Config sslConfig = new SSLEngineComponentFactoryImpl.Config();
    sslConfig.setKeyStoreFilePath(keyStorePath);
    sslConfig.setKeyStorePassword(LOCAL_PASSWORD);
    sslConfig.setKeyStoreType("JKS");
    sslConfig.setTrustStoreFilePath(keyStorePath);
    sslConfig.setTrustStoreFilePassword(LOCAL_PASSWORD);
    sslConfig.setSslEnabled(true);
    try {
      return new SSLEngineComponentFactoryImpl(sslConfig);
    } catch (Exception e) {
      throw new VeniceException("Failed to create local ssl factory with a self-signed cert", e);
    }
  }

  protected static String getPathForResource(String resource) {
    String systemTempDir = System.getProperty("java.io.tmpdir");
    String subDir = TestUtils.getUniqueString("venice-keys");
    File tempDir = new File(systemTempDir, subDir);
    tempDir.mkdir();
    tempDir.deleteOnExit();
    File file = new File(tempDir.getAbsolutePath(), resource);
    if (!file.exists()) {
      try(InputStream is = (ClassLoader.getSystemResourceAsStream(resource))){
        Files.copy(is, file.getAbsoluteFile().toPath());
      } catch (IOException e) {
        throw new RuntimeException("Failed to copy resource: " + resource + " to tmp dir", e);
      } finally {
        file.deleteOnExit();
      }
    }
    return file.getAbsolutePath();
  }

}
