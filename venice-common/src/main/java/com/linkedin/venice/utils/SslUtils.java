package com.linkedin.venice.utils;

import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import com.linkedin.security.ssl.access.control.SSLEngineComponentFactoryImpl;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.guid.GuidUtils;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.conn.NoopIOSessionStrategy;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;

import static com.linkedin.venice.HttpConstants.*;


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
    SSLEngineComponentFactoryImpl.Config sslConfig = getLocalSslConfig();
    try {
      return new SSLEngineComponentFactoryImpl(sslConfig);
    } catch (Exception e) {
      throw new VeniceException("Failed to create local ssl factory with a self-signed cert", e);
    }
  }

  public static SSLEngineComponentFactoryImpl.Config getLocalSslConfig(){
    String keyStorePath = getPathForResource(LOCAL_KEYSTORE_JKS);
    SSLEngineComponentFactoryImpl.Config sslConfig = new SSLEngineComponentFactoryImpl.Config();
    sslConfig.setKeyStoreFilePath(keyStorePath);
    sslConfig.setKeyStorePassword(LOCAL_PASSWORD);
    sslConfig.setKeyStoreType("JKS");
    sslConfig.setTrustStoreFilePath(keyStorePath);
    sslConfig.setTrustStoreFilePassword(LOCAL_PASSWORD);
    sslConfig.setSslEnabled(true);
    return sslConfig;
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
    SSLContext sslContext = sslFactory.getSSLContext();
    SSLIOSessionStrategy sslSessionStrategy = new SSLIOSessionStrategy(sslContext);
    return sslSessionStrategy;
  }

  public static CloseableHttpAsyncClient getMinimalHttpClient(int ioThreadNum, int maxConnPerRoute, int maxConnTotal, Optional<SSLEngineComponentFactory> sslFactory) {
    PoolingNHttpClientConnectionManager connectionManager = createConnectionManager(ioThreadNum, maxConnPerRoute, maxConnTotal, sslFactory);
    reapIdleConnections(connectionManager, 10, TimeUnit.MINUTES, 2, TimeUnit.HOURS);
    return HttpAsyncClients.createMinimal(connectionManager);
  }

  public static PoolingNHttpClientConnectionManager createConnectionManager(int ioThreadNum, int perRoute, int total, Optional<SSLEngineComponentFactory> sslFactory) {
    IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
        .setSoKeepAlive(true)
        .setIoThreadCount(ioThreadNum)
        .build();
    ConnectingIOReactor ioReactor = null;
    try {
      ioReactor = new DefaultConnectingIOReactor(ioReactorConfig);
    } catch (IOReactorException e) {
      throw new VeniceException("Router failed to create an IO Reactor", e);
    }
    PoolingNHttpClientConnectionManager connMgr;
    if(sslFactory.isPresent()) {
      SSLIOSessionStrategy sslStrategy = getSslStrategy(sslFactory.get());
      RegistryBuilder<SchemeIOSessionStrategy> registryBuilder = RegistryBuilder.create();
      registryBuilder.register(HTTPS, sslStrategy).register(HTTP, NoopIOSessionStrategy.INSTANCE);
      connMgr = new PoolingNHttpClientConnectionManager(ioReactor, registryBuilder.build());
    } else {
      connMgr = new PoolingNHttpClientConnectionManager(ioReactor);
    }
    connMgr.setMaxTotal(total);
    connMgr.setDefaultMaxPerRoute(perRoute);

    //TODO: Configurable
    reapIdleConnections(connMgr, 10, TimeUnit.MINUTES, 2, TimeUnit.HOURS);

    return connMgr;
  }

  public static CloseableHttpAsyncClient getMinimalHttpClient(int maxConnPerRoute, int maxConnTotal, Optional<SSLEngineComponentFactory> sslFactory) {
    return getMinimalHttpClient(1, maxConnPerRoute, maxConnTotal, sslFactory);
  }

  /**
   * Creates a new thread that automatically cleans up idle connections on the specified connection manager.
   * @param connectionManager  Connection manager with idle connections that should be reaped
   * @param sleepTime how frequently to wake up and reap idle connections
   * @param sleepTimeUnits
   * @param maxIdleTime how long a connection must be idle in order to be eligible for reaping
   * @param maxIdleTimeUnits
   * @return started daemon thread that is doing the reaping.  Interrupt this thread to halt reaping or ignore the return value.
   */
  private static Thread reapIdleConnections(PoolingNHttpClientConnectionManager connectionManager,
    long sleepTime, TimeUnit sleepTimeUnits,
    long maxIdleTime, TimeUnit maxIdleTimeUnits) {
    Thread idleConnectionReaper = new Thread(()->{
      while (true){
        try {
          Thread.sleep(sleepTimeUnits.toMillis(sleepTime));
          connectionManager.closeIdleConnections(maxIdleTime, maxIdleTimeUnits);
        } catch (InterruptedException e){
          break;
        }
      }
    }, "ConnectionManagerIdleReaper");
    idleConnectionReaper.setDaemon(true);
    idleConnectionReaper.start();
    return idleConnectionReaper;
  }
}
