package com.linkedin.davinci.ingestion.isolated;

import com.linkedin.alpini.netty4.ssl.SslInitializer;
import com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils;
import com.linkedin.venice.listener.VerifySslHandler;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SslUtils;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import java.util.Optional;


public class IsolatedIngestionServerChannelInitializer extends ChannelInitializer<SocketChannel> {
  private final IsolatedIngestionServer isolatedIngestionServer;
  private final Optional<SSLFactory> sslFactory;
  private final Optional<IsolatedIngestionServerAclHandler> aclHandler;
  private final VerifySslHandler verifySslHandler = new VerifySslHandler();

  public IsolatedIngestionServerChannelInitializer(IsolatedIngestionServer isolatedIngestionServer) {
    this.isolatedIngestionServer = isolatedIngestionServer;
    this.sslFactory = IsolatedIngestionUtils.getSSLFactory(isolatedIngestionServer.getConfigLoader());
    this.aclHandler = IsolatedIngestionUtils.getAclHandler(isolatedIngestionServer.getConfigLoader());
  }

  @Override
  protected void initChannel(SocketChannel ch) {
    sslFactory.ifPresent(
        sslFactory -> ch.pipeline().addLast(new SslInitializer(SslUtils.toAlpiniSSLFactory(sslFactory), false)));
    ch.pipeline().addLast(new HttpRequestDecoder());
    // Set the maximum allowed request size to 100MB as the initial metric report size is fairly large.
    ch.pipeline().addLast(new HttpObjectAggregator(1024 * 1024 * 100));
    ch.pipeline().addLast(new HttpResponseEncoder());
    if (sslFactory.isPresent()) {
      ch.pipeline().addLast(verifySslHandler);
      aclHandler
          .ifPresent(isolatedIngestionServerAclHandler -> ch.pipeline().addLast(isolatedIngestionServerAclHandler));
    }
    ch.pipeline().addLast(new IsolatedIngestionServerHandler(isolatedIngestionServer));
  }
}
