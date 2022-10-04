package com.linkedin.alpini.netty4.ssl;

import com.linkedin.alpini.base.concurrency.Executors;
import com.linkedin.alpini.netty4.handlers.ChannelInitializer;
import com.linkedin.alpini.netty4.http2.SSLContextBuilder;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioChannelOption;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.util.ReferenceCountUtil;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLEngine;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups = "unit")
public class TestFusedSslHandler {
  private static final Logger LOG = LogManager.getLogger(TestFusedSslHandler.class);

  public void testBasic() throws Exception {

    SslContext clientContext = SSLContextBuilder.makeClientContext(0, 0);
    SslContext serverContext = SSLContextBuilder.makeServerContext(0, 0);

    SSLEngine serverEngine = serverContext.newEngine(UnpooledByteBufAllocator.DEFAULT);
    SSLEngine clientEngine = clientContext.newEngine(UnpooledByteBufAllocator.DEFAULT);

    NioEventLoopGroup group = new NioEventLoopGroup(1);
    try {
      CountDownLatch writeFailed = new CountDownLatch(1);

      ServerBootstrap serverBootstrap = new ServerBootstrap().channel(NioServerSocketChannel.class)
          .group(group)
          .localAddress(new InetSocketAddress(0))
          .childOption(NioChannelOption.SO_SNDBUF, 1024)
          .childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
              ch.pipeline()
                  .addLast(new FusedSslHandler(serverEngine, Executors.newSingleThreadExecutor()))
                  .addLast(new FlushConsolidationHandler())
                  .addLast(new ChannelInboundHandlerAdapter() {
                    @Override
                    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
                      if (evt instanceof SslHandshakeCompletionEvent) {
                        SslHandler handler = ctx.pipeline().get(SslHandler.class);
                        if (handler instanceof FusedSslHandler) {
                          ((FusedSslHandler) handler).setOutboundQueueSizeLimit(10_000);
                        }
                      }
                      super.userEventTriggered(ctx, evt);
                    }
                  })
                  .addLast(new LineBasedFrameDecoder(1024))
                  .addLast(new ChannelInboundHandlerAdapter() {
                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                      ctx.executor().schedule(() -> {
                        try {
                          if (msg instanceof ByteBuf) {
                            ByteBuf buf = (ByteBuf) msg;
                            if (buf.isReadable()) {

                              ChannelFuture future;
                              do {
                                future = ctx.write(buf.retainedDuplicate());
                                ctx.write(Unpooled.wrappedBuffer(new byte[] { 13, 10 }));
                              } while (buf.readableBytes() == 4 && (!future.isDone() || future.isSuccess()));

                              ctx.flush();

                              if (future.isDone() && !future.isSuccess()) {
                                LOG.info(buf.toString(StandardCharsets.US_ASCII));
                                writeFailed.countDown();
                              }
                            }
                          }
                        } finally {
                          ReferenceCountUtil.release(msg);
                        }
                      }, 100, TimeUnit.MILLISECONDS);
                    }
                  });
            }
          });

      Channel server = serverBootstrap.bind().sync().channel();
      try {

        LinkedBlockingQueue<String> received = new LinkedBlockingQueue<>();

        Bootstrap clientBootstrap = new Bootstrap().channel(NioSocketChannel.class)
            .group(group)
            .remoteAddress(server.localAddress())
            .option(NioChannelOption.SO_RCVBUF, 1024)
            .handler(new ChannelInitializer<SocketChannel>() {
              @Override
              protected void initChannel(SocketChannel ch) {
                ch.pipeline()
                    .addLast(new FusedSslHandler(clientEngine))
                    .addLast(new LineBasedFrameDecoder(1024))
                    .addLast(new ChannelInboundHandlerAdapter() {
                      @Override
                      public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                        try {
                          if (msg instanceof ByteBuf) {
                            ByteBuf buf = (ByteBuf) msg;
                            if (buf.isReadable()) {
                              received.put(buf.toString(StandardCharsets.US_ASCII));
                            }
                          }
                        } finally {
                          ReferenceCountUtil.release(msg);
                        }
                      }
                    });
              }
            });

        Channel client = clientBootstrap.connect().sync().channel();
        try {

          client.writeAndFlush(Unpooled.copiedBuffer("Hello world\r\n", StandardCharsets.US_ASCII)).sync();

          Assert.assertEquals(received.take(), "Hello world");

          client.writeAndFlush(Unpooled.copiedBuffer("BOOM\r\n", StandardCharsets.US_ASCII)).sync();

          client.eventLoop().submit(() -> client.config().setAutoRead(false)).sync();

          Assert.assertTrue(writeFailed.await(10, TimeUnit.SECONDS));

          client.eventLoop().submit(() -> client.config().setAutoRead(true)).sync();

        } finally {
          client.close().sync();
        }
      } finally {
        server.close().sync();
      }
    } finally {
      group.shutdownGracefully();
    }
  }
}
