package com.linkedin.venice.httpclient5;

import java.net.SocketAddress;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import org.apache.hc.client5.http.ssl.DefaultClientTlsStrategy;
import org.apache.hc.core5.function.Factory;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.ssl.TLS;
import org.apache.hc.core5.http2.HttpVersionPolicy;
import org.apache.hc.core5.http2.ssl.H2TlsSupport;
import org.apache.hc.core5.net.NamedEndpoint;
import org.apache.hc.core5.reactor.ssl.SSLBufferMode;
import org.apache.hc.core5.reactor.ssl.SSLSessionInitializer;
import org.apache.hc.core5.reactor.ssl.SSLSessionVerifier;
import org.apache.hc.core5.reactor.ssl.TlsDetails;
import org.apache.hc.core5.reactor.ssl.TransportSecurityLayer;
import org.apache.hc.core5.util.Args;
import org.apache.hc.core5.util.Timeout;


/**
 * This class copies most of the logic from {@link DefaultClientTlsStrategy} and {@link AbstractClientTlsStrategy} to
 * get rid of the cipher check to be backward compatible.
 */
public class VeniceClientTlsStrategy extends DefaultClientTlsStrategy {
  private final SSLContext sslContext;
  private final String[] supportedProtocols;
  private final String[] supportedCipherSuites;
  private final SSLBufferMode sslBufferManagement;
  private final Factory<SSLEngine, TlsDetails> tlsDetailsFactory;

  public VeniceClientTlsStrategy(
      SSLContext sslContext,
      String[] supportedProtocols,
      String[] supportedCipherSuites,
      SSLBufferMode sslBufferManagement,
      HostnameVerifier hostnameVerifier,
      Factory<SSLEngine, TlsDetails> tlsDetailsFactory) {
    super(
        sslContext,
        supportedProtocols,
        supportedCipherSuites,
        sslBufferManagement,
        hostnameVerifier,
        tlsDetailsFactory);

    this.sslContext = Args.notNull(sslContext, "SSL context");
    this.supportedProtocols = supportedProtocols;
    this.supportedCipherSuites = supportedCipherSuites;
    this.sslBufferManagement = sslBufferManagement != null ? sslBufferManagement : SSLBufferMode.STATIC;
    this.tlsDetailsFactory = tlsDetailsFactory;
  }

  @Override
  public boolean upgrade(
      final TransportSecurityLayer tlsSession,
      final HttpHost host,
      final SocketAddress localAddress,
      final SocketAddress remoteAddress,
      final Object attachment,
      final Timeout handshakeTimeout) {
    tlsSession.startTls(sslContext, host, sslBufferManagement, new SSLSessionInitializer() {
      @Override
      public void initialize(final NamedEndpoint endpoint, final SSLEngine sslEngine) {

        final HttpVersionPolicy versionPolicy =
            attachment instanceof HttpVersionPolicy ? (HttpVersionPolicy) attachment : HttpVersionPolicy.NEGOTIATE;

        final SSLParameters sslParameters = sslEngine.getSSLParameters();
        if (supportedProtocols != null) {
          sslParameters.setProtocols(supportedProtocols);
        } else if (versionPolicy != HttpVersionPolicy.FORCE_HTTP_1) {
          sslParameters.setProtocols(TLS.excludeWeak(sslParameters.getProtocols()));
        }
        if (supportedCipherSuites != null) {
          sslParameters.setCipherSuites(supportedCipherSuites);
        } else if (versionPolicy == HttpVersionPolicy.FORCE_HTTP_2) {
          /**
           * Skip the H2 cipher check since LinkedIn is using an unsupported cipher.
           */
          // sslParameters.setCipherSuites(TlsCiphers.excludeH2-lacklisted(sslParameters.getCipherSuites()));
          sslParameters.setCipherSuites(sslParameters.getCipherSuites());
        }

        if (versionPolicy != HttpVersionPolicy.FORCE_HTTP_1) {
          H2TlsSupport.setEnableRetransmissions(sslParameters, false);
        }

        applyParameters(sslEngine, sslParameters, H2TlsSupport.selectApplicationProtocols(attachment));

        initializeEngine(sslEngine);
      }

    }, new SSLSessionVerifier() {
      @Override
      public TlsDetails verify(final NamedEndpoint endpoint, final SSLEngine sslEngine) throws SSLException {
        verifySession(host.getHostName(), sslEngine.getSession());
        final TlsDetails tlsDetails = createTlsDetails(sslEngine);
        /**
         * Skip the H2 cipher check to be backward compatible.
         */
        // final String negotiatedCipherSuite = sslEngine.getSession().getCipherSuite();
        // if (tlsDetails != null && ApplicationProtocol.HTTP_2.id.equals(tlsDetails.getApplicationProtocol())) {
        // if (TlsCiphers.isH2-lacklisted(negotiatedCipherSuite)) {
        // throw new SSLHandshakeException("Cipher suite `" + negotiatedCipherSuite
        // + "` does not provide adequate security for HTTP/2");
        // }
        // }
        return tlsDetails;
      }

    }, handshakeTimeout);
    return true;
  }

  void applyParameters(final SSLEngine sslEngine, final SSLParameters sslParameters, final String[] appProtocols) {
    H2TlsSupport.setApplicationProtocols(sslParameters, appProtocols);
    sslEngine.setSSLParameters(sslParameters);
  }

  TlsDetails createTlsDetails(final SSLEngine sslEngine) {
    return tlsDetailsFactory != null ? tlsDetailsFactory.create(sslEngine) : null;
  }

}
