package com.linkedin.venice.utils;

import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import com.linkedin.security.ssl.access.control.SSLEngineComponentFactoryImpl;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.guid.GuidUtils;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;


public class SslUtils {
  // Self-signed cert, expires 2027, use keystore as truststore since self-signed.
  // cert has CN=localhost
  private static final String LOCAL_PASSWORD = "dev_pass";
  private static final String LOCAL_KEYSTORE_P12 = "localhost.p12";
  private static final String LOCAL_KEYSTORE_JKS = "localhost.jks";
  private static final String LOCAL_CERT = "localhost.cert";
  private static final String LOCAL_KEY = "localhost.key";

  /**
   * @return factory that corresponds to self-signed development certificate
   */
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
    String subDir = "venice-keys-" + GuidUtils.getGUIDString();
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

  public static SSLIOSessionStrategy getSslStrategy(SSLEngineComponentFactory sslFactory) {
    SSLContext sslContext = SslUtils.getLocalSslFactory().getSSLContext();
    SSLIOSessionStrategy sslSessionStrategy = new SSLIOSessionStrategy(sslContext);
    return sslSessionStrategy;
  }

  /**
   * Use this as an SSL-enabled client for use in unit tests.  Uses the unit-test provided localhost certificate.
   * Most of venice will pick up the local hosts defined hostname, so this provides a return-true hostname verifier
   * This client needs to be started before use, and closed after use.
   * @return
   */
  public static CloseableHttpAsyncClient getSslClient(){
    CloseableHttpAsyncClient httpclient = HttpAsyncClients.custom()
        .setSSLStrategy(getSslStrategy(getLocalSslFactory()))
        .setSSLHostnameVerifier(new HostnameVerifier() {
          @Override
          public boolean verify(String s, SSLSession sslSession) {
            return true;
          }
        })
        .build();
    return httpclient;
  }
}
