package com.linkedin.davinci.ingestion.main;

import com.linkedin.alpini.netty4.ssl.SslInitializer;
import com.linkedin.venice.listener.VerifySslHandler;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SslUtils;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import java.util.Optional;


public class MainIngestionReportChannelInitializer extends ChannelInitializer<SocketChannel> {
  private final MainIngestionMonitorService mainIngestionMonitorService;
  private final Optional<SSLFactory> sslFactory;
  private final VerifySslHandler verifySslHandler = new VerifySslHandler();

  public MainIngestionReportChannelInitializer(
      MainIngestionMonitorService mainIngestionMonitorService,
      Optional<SSLFactory> sslFactory) {
    this.sslFactory = sslFactory;
    this.mainIngestionMonitorService = mainIngestionMonitorService;
  }

  @Override
  protected void initChannel(SocketChannel ch) {
    sslFactory.ifPresent(
        sslFactory -> ch.pipeline().addLast(new SslInitializer(SslUtils.toAlpiniSSLFactory(sslFactory), false)));
    ch.pipeline().addLast(new HttpRequestDecoder());
    ch.pipeline().addLast(new HttpObjectAggregator(1024 * 1024 * 100));
    ch.pipeline().addLast(new HttpResponseEncoder());
    if (sslFactory.isPresent()) {
      ch.pipeline().addLast(verifySslHandler);
    }
    ch.pipeline().addLast(new MainIngestionReportHandler(mainIngestionMonitorService));
  }
}
