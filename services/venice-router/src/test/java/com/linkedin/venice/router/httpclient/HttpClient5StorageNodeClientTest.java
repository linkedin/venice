package com.linkedin.venice.router.httpclient;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.router.VeniceRouterConfig;
import com.linkedin.venice.security.SSLFactory;
import java.util.Optional;
import javax.net.ssl.SSLContext;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class HttpClient5StorageNodeClientTest {
  private VeniceRouterConfig routerConfig;
  private SSLFactory sslFactory;
  private SSLContext sslContext;

  @BeforeMethod
  public void setUp() throws Exception {
    routerConfig = mock(VeniceRouterConfig.class);
    sslFactory = mock(SSLFactory.class);
    sslContext = SSLContext.getDefault();

    // Setup default config values
    doReturn(2).when(routerConfig).getHttpClient5PoolSize();
    doReturn(4).when(routerConfig).getHttpClient5TotalIOThreadCount();
    doReturn(30000).when(routerConfig).getSocketTimeout();
    doReturn(false).when(routerConfig).isHttpClient5SkipCipherCheck();

    doReturn(sslContext).when(sslFactory).getSSLContext();
  }

  @Test
  public void testConstructorRequiresSslFactory() {
    try {
      new HttpClient5StorageNodeClient(Optional.empty(), routerConfig);
      Assert.fail("Should have thrown VeniceException for missing SSL factory");
    } catch (VeniceException e) {
      Assert.assertTrue(e.getMessage().contains("sslFactory"), "Exception should mention sslFactory requirement");
    }
  }

  @Test
  public void testConstructorWithValidSslFactory() {
    // This test validates that construction works with valid SSL factory
    // The client pool will be created
    HttpClient5StorageNodeClient client = null;
    try {
      client = new HttpClient5StorageNodeClient(Optional.of(sslFactory), routerConfig);
      // If we get here, construction succeeded
      Assert.assertNotNull(client);
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }

  @Test
  public void testStartIsNoOp() {
    HttpClient5StorageNodeClient client = null;
    try {
      client = new HttpClient5StorageNodeClient(Optional.of(sslFactory), routerConfig);
      // start() should be a no-op, just verify it doesn't throw
      client.start();
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }

  @Test
  public void testCloseDoesNotThrow() {
    HttpClient5StorageNodeClient client = new HttpClient5StorageNodeClient(Optional.of(sslFactory), routerConfig);

    // close() should gracefully close all clients without throwing
    client.close();

    // Calling close again should also not throw (idempotent)
    client.close();
  }

  @Test
  public void testConstructorWithPoolSizeOne() {
    doReturn(1).when(routerConfig).getHttpClient5PoolSize();
    doReturn(2).when(routerConfig).getHttpClient5TotalIOThreadCount();

    HttpClient5StorageNodeClient client = null;
    try {
      client = new HttpClient5StorageNodeClient(Optional.of(sslFactory), routerConfig);
      Assert.assertNotNull(client);
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }

  @Test
  public void testConstructorWithPoolSizeFour() {
    doReturn(4).when(routerConfig).getHttpClient5PoolSize();
    doReturn(8).when(routerConfig).getHttpClient5TotalIOThreadCount();

    HttpClient5StorageNodeClient client = null;
    try {
      client = new HttpClient5StorageNodeClient(Optional.of(sslFactory), routerConfig);
      Assert.assertNotNull(client);
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }

  @Test
  public void testConstructorWithSkipCipherCheck() {
    doReturn(true).when(routerConfig).isHttpClient5SkipCipherCheck();

    HttpClient5StorageNodeClient client = null;
    try {
      client = new HttpClient5StorageNodeClient(Optional.of(sslFactory), routerConfig);
      Assert.assertNotNull(client);
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }

  @Test
  public void testIsInstanceReadyToServeAlwaysReturnsTrue() {
    HttpClient5StorageNodeClient client = null;
    try {
      client = new HttpClient5StorageNodeClient(Optional.of(sslFactory), routerConfig);

      // HttpClient5StorageNodeClient doesn't implement connection warming,
      // so isInstanceReadyToServe should just use the default from StorageNodeClient interface
      // which returns true by default
      Assert.assertTrue(client.isInstanceReadyToServe("any-instance"));
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }
}
