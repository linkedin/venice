package com.linkedin.alpini.netty4.ssl;

import com.linkedin.alpini.base.ssl.SslFactory;
import com.linkedin.alpini.io.ssl.SSLContextBuilder;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.io.File;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSessionContext;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestSSLEngineFactory {
  private static final String jksKeyStoreFilePath = "src/test/resources/identity.jks";
  private static final String jksKeyStoreFilePassword = "clientpassword";
  private static final String jksTrustStoreFilePath = "src/test/resources/trustStore.jks";
  private static final String jksTrustStoreFilePassword = "work_around_jdk-6879539";

  private static final SSLContext sslContext = getTestSslContext();
  private static final SSLParameters sslParameters = getTestSslParameters();

  private static SSLContext getTestSslContext() {
    try {
      return new SSLContextBuilder().build(
          new File(jksKeyStoreFilePath),
          jksKeyStoreFilePassword,
          "JKS",
          new File(jksTrustStoreFilePath),
          jksTrustStoreFilePassword);
    } catch (Exception e) {
      Assert.fail();
      return null;
    }
  }

  private static SSLParameters getTestSslParameters() {
    return sslContext.getDefaultSSLParameters();
  }

  private SslFactory createSslFactory(boolean sslEnabled) {
    return new SslFactory() {
      @Override
      public SSLContext getSSLContext() {
        return sslEnabled ? sslContext : null;
      }

      @Override
      public SSLParameters getSSLParameters() {
        return sslEnabled ? sslParameters : null;
      }

      @Override
      public boolean isSslEnabled() {
        return sslEnabled;
      }
    };
  }

  private void validateSSLEngine(SSLEngine sslEngine, boolean isServer, String expectedPeerHost, int expectedPeerPort) {
    Assert.assertEquals(sslEngine.getUseClientMode(), !isServer);
    Assert.assertTrue(sslEngine.getEnabledCipherSuites().length > 0, "At least one cipher suite must be enabled.");
    Assert.assertEquals(sslEngine.getPeerHost(), expectedPeerHost);
    Assert.assertEquals(sslEngine.getPeerPort(), expectedPeerPort);
  }

  private void validateSSLEngineFactory(SSLEngineFactory engineFactory, boolean isServer) {
    SSLEngine sslEngine = engineFactory.createSSLEngine(UnpooledByteBufAllocator.DEFAULT, isServer);
    validateSSLEngine(sslEngine, isServer, null, -1);

    String testHost = "testHost";
    int testPort = 10;
    SSLEngine sslEngineWithPeer =
        engineFactory.createSSLEngine(UnpooledByteBufAllocator.DEFAULT, testHost, testPort, isServer);
    validateSSLEngine(sslEngineWithPeer, isServer, testHost, testPort);

    SSLSessionContext sessionContext = engineFactory.sessionContext(isServer);
    if (isServer) {
      Assert.assertEquals(sessionContext, sslContext.getServerSessionContext());
    } else {
      Assert.assertEquals(sessionContext, sslContext.getClientSessionContext());
    }
  }

  @Test(groups = "unit")
  public void returnsUnAdaptedSslEngineFactory() {
    SslFactory sslFactory = Mockito.mock(SSLEngineFactory.class);
    SSLEngineFactory engineFactory = SSLEngineFactory.adaptSSLFactory(sslFactory);

    Assert.assertSame(engineFactory, sslFactory);
  }

  @Test(groups = "unit")
  public void canAdaptSSLFactoryWhenSslDisabled() {
    boolean sslEnabled = false;
    SslFactory sslFactory = createSslFactory(sslEnabled);
    SSLEngineFactory engineFactory = SSLEngineFactory.adaptSSLFactory(sslFactory);
    Assert.assertEquals(engineFactory.isSslEnabled(), sslEnabled);
    Assert.assertNull(engineFactory.getSSLParameters());
    Assert.assertNull(engineFactory.getSSLContext());
  }

  @Test(groups = "unit")
  public void canAdaptSSLFactoryWhenSslEnabled() {
    boolean sslEnabled = true;
    SslFactory sslFactory = createSslFactory(sslEnabled);
    SSLEngineFactory engineFactory = SSLEngineFactory.adaptSSLFactory(sslFactory);
    Assert.assertEquals(engineFactory.isSslEnabled(), sslEnabled);
    Assert.assertEquals(engineFactory.getSSLParameters(), sslParameters);
    Assert.assertEquals(engineFactory.getSSLContext(), sslContext);

    validateSSLEngineFactory(engineFactory, /*isServer*/ true);
    validateSSLEngineFactory(engineFactory, /*isServer*/ false);
  }
}
