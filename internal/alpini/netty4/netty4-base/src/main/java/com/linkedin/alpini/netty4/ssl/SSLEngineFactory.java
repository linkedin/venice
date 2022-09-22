package com.linkedin.alpini.netty4.ssl;

import com.linkedin.alpini.base.ssl.SslFactory;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import java.util.Optional;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSessionContext;


/**
 * Factory interface to create {@link SSLEngine} and get {@link SSLSessionContext} objects for Netty4 pipelines
 */
public interface SSLEngineFactory extends SslFactory {
  SSLEngine createSSLEngine(ByteBufAllocator alloc, String host, int port, boolean isServer);

  SSLEngine createSSLEngine(ByteBufAllocator alloc, boolean isServer);

  SSLSessionContext sessionContext(boolean isServer);

  /**
   * The default implementation for the anonymous classes below. :69
   *
   * @param isServer {@code true} for server SSL context.
   * @return instance of {@linkplain SslContext}
   */
  default SslContext context(boolean isServer) {
    throw new UnsupportedOperationException("Not implemented");
  }

  static SSLEngineFactory adaptSSLFactory(SslFactory factory) {
    if (factory == null || factory instanceof SSLEngineFactory) {
      return (SSLEngineFactory) factory;
    }

    boolean sslEnabled = factory.isSslEnabled();
    Optional<SSLContext> sslContext = Optional.ofNullable(sslEnabled ? factory.getSSLContext() : null);
    SSLParameters sslParameters = sslEnabled ? factory.getSSLParameters() : null;

    return new SSLEngineFactory() {
      @Override
      public SSLEngine createSSLEngine(ByteBufAllocator alloc, String host, int port, boolean isServer) {
        return init(sslContext.orElseThrow(IllegalStateException::new).createSSLEngine(host, port), isServer);
      }

      @Override
      public SSLEngine createSSLEngine(ByteBufAllocator alloc, boolean isServer) {
        return init(sslContext.orElseThrow(IllegalStateException::new).createSSLEngine(), isServer);
      }

      @Override
      public SSLSessionContext sessionContext(boolean isServer) {
        return sslContext.map(sslCtx -> isServer ? sslCtx.getServerSessionContext() : sslCtx.getClientSessionContext())
            .orElseThrow(IllegalStateException::new);
      }

      private SSLEngine init(SSLEngine engine, boolean isServer) {
        engine.setUseClientMode(!isServer);
        engine.setEnabledCipherSuites(sslParameters.getCipherSuites());
        return engine;
      }

      @Override
      public SSLContext getSSLContext() {
        return sslContext.orElse(null);
      }

      @Override
      public SSLParameters getSSLParameters() {
        return sslParameters;
      }

      @Override
      public boolean isSslEnabled() {
        return sslEnabled;
      }
    };
  }
}
