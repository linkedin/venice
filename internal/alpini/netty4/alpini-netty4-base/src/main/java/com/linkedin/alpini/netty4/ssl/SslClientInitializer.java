package com.linkedin.alpini.netty4.ssl;

import com.linkedin.alpini.netty4.handlers.ChannelInitializer;
import com.linkedin.alpini.netty4.misc.NettyUtils;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.EventExecutorGroup;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Stream;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;


/**
 * Created by acurtis on 9/7/17.
 */
@ChannelHandler.Sharable
public class SslClientInitializer extends ChannelInitializer<Channel> {
  private final boolean _sslEnabled;
  private final SSLEngineFactory _sslEngineFactory;
  private final SecureClientHandler _secureClientHandler;
  private final ChannelHandler _connectHandler = new ConnectHandler();
  private Executor _sslExecutor;

  public SslClientInitializer(SSLEngineFactory sslEngineComponentFactory) {
    this(sslEngineComponentFactory, false);
  }

  public SslClientInitializer(SSLEngineFactory sslEngineComponentFactory, boolean principalValidation) {
    _sslEngineFactory = sslEngineComponentFactory;
    _sslEnabled = _sslEngineFactory != null && _sslEngineFactory.isSslEnabled();
    if (_sslEnabled) {
      _secureClientHandler = new SecureClientHandler(principalValidation ? this::clientCertificateValidation : null);
    } else {
      _secureClientHandler = null;
    }
  }

  /**
   * Configure for enabling the {@linkplain io.netty.handler.ssl.SslHandler} to offload handshake compute
   * tasks to an alternate executor.
   *
   * @param executor Executor to perform SslHandler tasks
   * @return this
   */
  public SslClientInitializer enableSslTaskExecutor(Executor executor) {
    _sslExecutor = Objects.requireNonNull(executor);
    return this;
  }

  protected boolean clientCertificateValidation(ChannelHandlerContext ctx, X509Certificate clientCert) {
    return true;
  }

  @Override
  protected void initChannel(Channel ch) throws Exception {
    if (_sslEnabled) {
      if (!ch.isActive()) {
        ch.pipeline().replace(this, "ssl-wait-connected", _connectHandler);
      } else {
        ChannelHandlerContext ctx = ch.pipeline().context(this);
        replaceChannelHandler(ctx, this, createSSLEngine(ctx, ch.remoteAddress()));
      }
    }
  }

  private SSLEngine createSSLEngine(ChannelHandlerContext ctx, SocketAddress remoteAddress) {
    SSLEngine engine;
    if (remoteAddress instanceof InetSocketAddress) {
      InetSocketAddress inet = (InetSocketAddress) remoteAddress;
      engine = _sslEngineFactory.createSSLEngine(ctx.alloc(), inet.getHostString(), inet.getPort(), false);
    } else {
      engine = _sslEngineFactory.createSSLEngine(ctx.alloc(), false);
    }
    engine.setUseClientMode(true);

    SSLParameters sslParameters = _sslEngineFactory.getSSLParameters();
    if (sslParameters != null) {
      Set<String> supportedCiphers = new HashSet<>(Arrays.asList(engine.getSupportedCipherSuites()));
      engine.setEnabledCipherSuites(
          Stream.of(sslParameters.getCipherSuites()).filter(supportedCiphers::contains).toArray(String[]::new));
    }
    return engine;
  }

  private void replaceChannelHandler(ChannelHandlerContext ctx, ChannelHandler self, SSLEngine engine) {
    EventExecutorGroup executorGroup = NettyUtils.executorGroup(ctx.channel());
    // these need to be first in the pipeline
    // they are added in reverse order for that reason
    ctx.pipeline().addAfter(executorGroup, ctx.name(), "SecureClientHandler", _secureClientHandler);
    ctx.pipeline()
        .replace(
            self,
            "SSLHandler",
            _sslExecutor != null ? new FusedSslHandler(engine, _sslExecutor) : new FusedSslHandler(engine));
  }

  /**
   * This inner handler defers construction of the SSL engine until after connection has occurred.
   * This means that failed connections do not drain the entropy pool and we can tell the SslHandler
   * the hostname of the remote host that we are connected.
   */
  @Sharable
  private class ConnectHandler extends ChannelOutboundHandlerAdapter {
    @Override
    public void connect(
        ChannelHandlerContext ctx,
        SocketAddress remoteAddress,
        SocketAddress localAddress,
        ChannelPromise promise) {
      ctx.connect(remoteAddress, localAddress, replaceHandler(ctx, remoteAddress, promise));
    }

    private ChannelPromise replaceHandler(
        ChannelHandlerContext ctx,
        SocketAddress remoteAddress,
        ChannelPromise userPromise) {
      return ctx.newPromise().addListener(future -> {
        if (future.isSuccess()) {
          if (_sslExecutor != null) {
            CompletableFuture.supplyAsync(() -> createSSLEngine(ctx, remoteAddress), _sslExecutor)
                .whenCompleteAsync(((engine, ex) -> {
                  if (ex == null) {
                    replaceChannelHandler(ctx, this, engine);
                    userPromise.setSuccess();
                  } else {
                    userPromise.setFailure(ex);
                  }
                }), ctx.executor());
          } else {
            replaceChannelHandler(ctx, this, createSSLEngine(ctx, remoteAddress));
            userPromise.setSuccess();
          }
        } else {
          userPromise.setFailure(future.cause());
        }
      });
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
      throw new IllegalStateException();
    }
  }
}
