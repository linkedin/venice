package com.linkedin.venice.httpclient5;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.HttpsSupport;
import org.apache.hc.core5.function.Factory;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.http.ssl.TLS;
import org.apache.hc.core5.reactor.ssl.SSLBufferMode;
import org.apache.hc.core5.reactor.ssl.TlsDetails;
import org.apache.hc.core5.ssl.SSLContexts;
import org.apache.hc.core5.util.ReflectionUtils;


/**
 * This class copies most of the logic from {@link ClientTlsStrategyBuilder} to get rid of the cipher check while using
 * http/2 in {@link org.apache.hc.core5.http.ssl.TlsCiphers} to be backward compatible.
 *
 * The only change is in function: {@link #build}, and this class will return {@link VeniceClientTlsStrategy}
 * instead of {@link org.apache.hc.client5.http.ssl.DefaultClientTlsStrategy}.
 */
public class VeniceClientTlsStrategyBuilder {
  public static VeniceClientTlsStrategyBuilder create() {
    return new VeniceClientTlsStrategyBuilder();
  }

  private SSLContext sslContext;
  private String[] tlsVersions;
  private String[] ciphers;
  private SSLBufferMode sslBufferMode;
  private HostnameVerifier hostnameVerifier;
  private Factory<SSLEngine, TlsDetails> tlsDetailsFactory;
  private boolean systemProperties;

  /**
   * Assigns {@link SSLContext} instance.
   */
  public VeniceClientTlsStrategyBuilder setSslContext(final SSLContext sslContext) {
    this.sslContext = sslContext;
    return this;
  }

  /**
   * Assigns enabled {@code TLS} versions.
   */
  public final VeniceClientTlsStrategyBuilder setTlsVersions(final String... tlslVersions) {
    this.tlsVersions = tlslVersions;
    return this;
  }

  /**
   * Assigns enabled {@code TLS} versions.
   */
  public final VeniceClientTlsStrategyBuilder setTlsVersions(final TLS... tlslVersions) {
    this.tlsVersions = new String[tlslVersions.length];
    for (int i = 0; i < tlslVersions.length; i++) {
      this.tlsVersions[i] = tlslVersions[i].id;
    }
    return this;
  }

  /**
   * Assigns enabled ciphers.
   */
  public final VeniceClientTlsStrategyBuilder setCiphers(final String... ciphers) {
    this.ciphers = ciphers;
    return this;
  }

  /**
   * Assigns {@link SSLBufferMode} value.
   */
  public VeniceClientTlsStrategyBuilder setSslBufferMode(final SSLBufferMode sslBufferMode) {
    this.sslBufferMode = sslBufferMode;
    return this;
  }

  /**
   * Assigns {@link HostnameVerifier} instance.
   */
  public VeniceClientTlsStrategyBuilder setHostnameVerifier(final HostnameVerifier hostnameVerifier) {
    this.hostnameVerifier = hostnameVerifier;
    return this;
  }

  /**
   * Assigns {@link TlsDetails} {@link Factory} instance.
   */
  public VeniceClientTlsStrategyBuilder setTlsDetailsFactory(final Factory<SSLEngine, TlsDetails> tlsDetailsFactory) {
    this.tlsDetailsFactory = tlsDetailsFactory;
    return this;
  }

  /**
   * Use system properties when creating and configuring default
   * implementations.
   */
  public final VeniceClientTlsStrategyBuilder useSystemProperties() {
    this.systemProperties = true;
    return this;
  }

  public TlsStrategy build() {
    final SSLContext sslContextCopy;
    if (sslContext != null) {
      sslContextCopy = sslContext;
    } else {
      sslContextCopy = systemProperties ? SSLContexts.createSystemDefault() : SSLContexts.createDefault();
    }
    final String[] tlsVersionsCopy;
    if (tlsVersions != null) {
      tlsVersionsCopy = tlsVersions;
    } else {
      tlsVersionsCopy = systemProperties ? HttpsSupport.getSystemProtocols() : null;
    }
    final String[] ciphersCopy;
    if (ciphers != null) {
      ciphersCopy = ciphers;
    } else {
      ciphersCopy = systemProperties ? HttpsSupport.getSystemCipherSuits() : null;
    }
    final Factory<SSLEngine, TlsDetails> tlsDetailsFactoryCopy;
    if (tlsDetailsFactory != null) {
      tlsDetailsFactoryCopy = tlsDetailsFactory;
    } else {
      tlsDetailsFactoryCopy = sslEngine -> {
        final SSLSession sslSession = sslEngine.getSession();
        final String applicationProtocol = ReflectionUtils.callGetter(sslEngine, "ApplicationProtocol", String.class);
        return new TlsDetails(sslSession, applicationProtocol);
      };
    }
    return new VeniceClientTlsStrategy(
        sslContextCopy,
        tlsVersionsCopy,
        ciphersCopy,
        sslBufferMode != null ? sslBufferMode : SSLBufferMode.STATIC,
        hostnameVerifier != null ? hostnameVerifier : HttpsSupport.getDefaultHostnameVerifier(),
        tlsDetailsFactoryCopy);
  }
}
