package com.linkedin.alpini.netty4.ssl;

import com.linkedin.alpini.base.concurrency.Executors;
import com.linkedin.alpini.base.misc.ExceptionUtil;
import com.linkedin.alpini.base.misc.Time;
import com.linkedin.venice.utils.TestUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.ssl.NotSslRecordException;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.ScheduledFuture;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.CertificateException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLProtocolException;
import javax.net.ssl.SSLSessionContext;
import javax.net.ssl.TrustManagerFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


@Test(groups = "unit")
public class TestSslInitializer {
  private static final String LOREM_IPSUM =
      " Lorem ipsum dolor sit amet, consectetur adipiscing elit. Maecenas tincidunt eget sapien eget aliquet. "
          + "Proin vitae lacus laoreet, bibendum nisl vel, feugiat mi. Aenean enim mauris, pretium tempus ex eget, "
          + "pellentesque placerat lacus. Maecenas volutpat neque suscipit, feugiat sapien fermentum, bibendum risus. "
          + "Vivamus et dolor eu risus eleifend hendrerit eu at diam. Quisque vel ligula enim. Sed varius placerat "
          + "turpis vel suscipit. Donec eu cursus sem. Vestibulum cursus, velit vel rhoncus lobortis, nulla magna "
          + "scelerisque ex, porttitor fermentum mi ante in dolor. Phasellus id mauris ac diam rhoncus accumsan ac "
          + "sed dui. Nulla sodales tempus maximus. Cras fringilla sapien sed ultricies faucibus. Integer nec dui at "
          + "dui aliquet pulvinar. Ut pharetra eleifend lectus in pulvinar. Vestibulum ante ipsum primis in faucibus "
          + "orci luctus et ultrices posuere cubilia Curae; Vivamus efficitur velit commodo eros interdum aliquam.\n"
          + "\n"
          + "Nulla volutpat velit id hendrerit lacinia. Aliquam lectus diam, tempus ac rhoncus laoreet, laoreet vel "
          + "tellus. Praesent urna lacus, efficitur eget aliquam in, hendrerit at dolor. Quisque eros urna, volutpat "
          + "vitae aliquet in, iaculis et purus. Maecenas tortor sem, semper in varius fringilla, mattis sit amet ex. "
          + "Curabitur molestie erat vitae purus malesuada, at feugiat nulla tincidunt. Sed ut neque ut urna sagittis "
          + "elementum auctor vel ipsum. Curabitur non congue ipsum, in iaculis nisi. Proin ultrices scelerisque nisl, "
          + "at luctus ligula tempus quis. Praesent suscipit consequat ultricies. Donec molestie cursus ligula quis "
          + "porttitor. Integer vulputate metus quis ex laoreet pulvinar. Vivamus vitae dui eget nulla congue "
          + "vestibulum eu in dolor. Proin aliquam sollicitudin eros in tincidunt. Morbi vel vehicula justo. Sed "
          + "viverra diam at neque porta lacinia eu sed lorem. ";

  private static final Logger LOG = LogManager.getLogger(TestSslInitializer.class);

  private SelfSignedCertificate _selfSignedCertificate;
  private EventExecutorGroup _resolveExecutor;
  private final UnaryOperator<SslInitializer> _enableResolveClient = this::enableResolveClient;
  private final UnaryOperator<SslInitializer> _slowResolveClient = this::slowResolveClient;
  private final UnaryOperator<SslInitializer> _failFirstResolveClient = this::failFirstResolveClient;

  private static final int RESOLVE_EXECUTOR_THREADS = 2;

  @BeforeClass
  public void createCertificate() throws CertificateException {
    _selfSignedCertificate = new SelfSignedCertificate();
    _resolveExecutor = new DefaultEventExecutorGroup(RESOLVE_EXECUTOR_THREADS);
  }

  @AfterClass(alwaysRun = true)
  public void done() {
    Optional.ofNullable(_resolveExecutor).ifPresent(EventExecutorGroup::shutdown);
  }

  private void configure(SSLEngineFactoryImpl.Config sslEngineConfig) {
    sslEngineConfig.setSslEnabled(true);
    sslEngineConfig.setRequireClientCertOnLocalHost(false);
    sslEngineConfig.setKeyStoreFilePath(getClass().getResource("/clientkeystore").getPath());
    sslEngineConfig.setKeyStorePassword("clientpassword");
    sslEngineConfig.setTrustStoreFilePath(getClass().getResource("/myTrustStore").getPath());
    sslEngineConfig.setTrustStoreFilePassword("trustPassword");
  }

  private SSLEngineFactoryImpl newSSLEngineFactory() throws Exception {
    SSLEngineFactoryImpl.Config sslEngineConfig = new SSLEngineFactoryImpl.Config();

    configure(sslEngineConfig);

    return new SSLEngineFactoryImpl(sslEngineConfig);
  }

  private SSLEngineFactoryImpl newSslEngineRefCntFactory() throws Exception {
    SSLEngineFactoryImpl.Config sslEngineConfig = new SSLEngineFactoryImpl.Config();

    configure(sslEngineConfig);
    sslEngineConfig.setUseRefCount(true);

    return new SSLEngineFactoryImpl(sslEngineConfig);
  }

  private SSLEngineFactoryImpl newSslEngineSunJSSEFactory() throws Exception {
    SSLEngineFactoryImpl.Config sslEngineConfig = new SSLEngineFactoryImpl.Config();

    configure(sslEngineConfig);
    sslEngineConfig.setSslContextProvider(Security.getProvider("SunJSSE"));
    Assert.assertNotNull(sslEngineConfig.getSslContextProvider());

    return new SSLEngineFactoryImpl(sslEngineConfig);
  }

  private <SERVER extends ServerChannel, CLIENT extends Channel> void simpleTest(
      EventLoopGroup eventLoopGroup,
      Class<SERVER> serverClass,
      Class<CLIENT> clientClass,
      SocketAddress bindAddress,
      SslInitializer serverSslInitializer,
      SslClientInitializer clientSslInitializer,
      String testString,
      int clientCount,
      Consumer<ChannelHandlerContext> serverCheck,
      Consumer<ChannelHandlerContext> beforeSslServerCheck) throws Throwable {
    CompletableFuture<Void> testFuture = new CompletableFuture<>();
    AttributeKey<CompositeByteBuf> receivedKey = AttributeKey.valueOf(getClass(), "received");
    try {
      ServerBootstrap serverBootstrap = new ServerBootstrap().group(eventLoopGroup)
          .channel(serverClass)
          .childHandler(new ChannelInitializer<CLIENT>() {
            @Override
            protected void initChannel(CLIENT ch) throws Exception {
              ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                @Override
                public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                  super.channelRead(ctx, msg);
                  beforeSslServerCheck.accept(ctx);
                }
              }, serverSslInitializer, new SimpleChannelInboundHandler<ByteBuf>() {
                @Override
                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                  if (!testFuture.completeExceptionally(cause)) {
                    super.exceptionCaught(ctx, cause);
                  }
                }

                @Override
                protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
                  serverCheck.accept(ctx);
                  ctx.writeAndFlush(msg.readBytes(msg.readableBytes()));
                }
              });
            }
          });

      Bootstrap clientBootstrap =
          new Bootstrap().group(eventLoopGroup).channel(clientClass).handler(new ChannelInitializer<CLIENT>() {
            @Override
            protected void initChannel(CLIENT ch) throws Exception {
              ch.pipeline().addLast(clientSslInitializer, new SimpleChannelInboundHandler<ByteBuf>() {
                @Override
                public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                  if (ctx.channel().hasAttr(receivedKey)) {
                    ctx.channel().attr(receivedKey).getAndSet(null).release();
                  }
                  super.channelInactive(ctx);
                }

                @Override
                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                  if (!testFuture.completeExceptionally(cause)) {
                    super.exceptionCaught(ctx, cause);
                  }
                }

                @Override
                protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
                  Attribute<CompositeByteBuf> attr = ctx.channel().attr(receivedKey);
                  if (attr.get() == null) {
                    attr.set(Unpooled.compositeBuffer());
                  }
                  attr.get().addComponent(true, msg.readBytes(msg.readableBytes()));
                }
              });
            }
          });

      ChannelFuture serverFuture = serverBootstrap.bind(bindAddress);
      try {
        serverFuture.sync();

        SocketAddress serverAddress = serverFuture.channel().localAddress();
        LOG.info("Server bound to address: {}", serverAddress);

        List<ChannelFuture> clientFutures = IntStream.range(0, clientCount)
            .mapToObj(count -> clientBootstrap.connect(serverAddress))
            .collect(Collectors.toList());
        try {
          for (ChannelFuture clientFuture: clientFutures) {
            clientFuture.sync();
          }

          List<ChannelFuture> writeFutures = clientFutures.stream()
              .map(
                  clientFuture -> clientFuture.channel()
                      .writeAndFlush(Unpooled.copiedBuffer(testString, StandardCharsets.UTF_8)))
              .collect(Collectors.toList());

          for (ChannelFuture writeFuture: writeFutures) {
            writeFuture.await();
          }

          int waitAttempts = 10;
          for (int i = 0; i < waitAttempts && !testFuture.isDone(); i++) {
            Thread.sleep(200);
            try {
              for (ChannelFuture clientFuture: clientFutures) {
                Attribute<CompositeByteBuf> attr = clientFuture.channel().attr(receivedKey);
                CompositeByteBuf received = attr.get();
                if (received == null) {
                  continue;
                }
                Assert.assertEquals(
                    ByteBufUtil.compare(received, Unpooled.copiedBuffer(testString, StandardCharsets.UTF_8)),
                    0,
                    ByteBufUtil.hexDump(received));
              }
              testFuture.complete(null);
            } catch (Throwable ex) {
              testFuture.completeExceptionally(ex);
            }
          }
        } finally {
          clientFutures.forEach(clientFuture -> clientFuture.addListener(ChannelFutureListener.CLOSE));
        }
      } finally {
        serverFuture.addListener(ChannelFutureListener.CLOSE);
      }
    } finally {
      eventLoopGroup.shutdownGracefully();
      if (testFuture.isCompletedExceptionally()) {
        try {
          testFuture.join();
        } catch (CompletionException ex) {
          if (ex.getCause() instanceof DecoderException) {
            throw ex.getCause().getCause();
          }
          throw ex.getCause();
        }
      }
    }
    try {
      testFuture.join();
    } catch (CompletionException ex) {
      if (ex.getCause() instanceof DecoderException) {
        throw ex.getCause().getCause();
      }
      throw ex.getCause();
    }
  }

  private SSLEngineFactory selfSigned() throws Exception {
    KeyStore keyStore = KeyStore.getInstance("JKS");
    try {
      keyStore.load(null);
    } catch (Exception ignored) {
    }
    keyStore.setCertificateEntry("1", _selfSignedCertificate.cert());
    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    kmf.init(keyStore, new char[0]);
    TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(keyStore);

    SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
    sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());

    return new SSLEngineFactory() {
      @Override
      public SSLEngine createSSLEngine(ByteBufAllocator alloc, String host, int port, boolean isServer) {
        return init(sslContext.createSSLEngine(host, port), isServer);
      }

      @Override
      public SSLEngine createSSLEngine(ByteBufAllocator alloc, boolean isServer) {
        return init(sslContext.createSSLEngine(), isServer);
      }

      @Override
      public SSLSessionContext sessionContext(boolean isServer) {
        return isServer ? sslContext.getServerSessionContext() : sslContext.getClientSessionContext();
      }

      private SSLEngine init(SSLEngine engine, boolean isServer) {
        engine.setUseClientMode(!isServer);
        engine.setEnabledCipherSuites(getSSLParameters().getCipherSuites());
        return engine;
      }

      @Override
      public SSLContext getSSLContext() {
        return sslContext;
      }

      @Override
      public SSLParameters getSSLParameters() {
        return sslContext.getDefaultSSLParameters();
      }

      @Override
      public boolean isSslEnabled() {
        return true;
      }
    };
  }

  private SSLEngineFactory encryptionDisabled() {
    return new SSLEngineFactory() {
      @Override
      public SSLEngine createSSLEngine(ByteBufAllocator alloc, String host, int port, boolean isServer) {
        throw new IllegalStateException();
      }

      @Override
      public SSLEngine createSSLEngine(ByteBufAllocator alloc, boolean isServer) {
        throw new IllegalStateException();
      }

      @Override
      public SSLSessionContext sessionContext(boolean isServer) {
        throw new IllegalStateException();
      }

      @Override
      public SSLContext getSSLContext() {
        throw new IllegalStateException();
      }

      @Override
      public SSLParameters getSSLParameters() {
        throw new IllegalStateException();
      }

      @Override
      public boolean isSslEnabled() {
        return false;
      }
    };
  }

  @Test
  public void testBasicLocalEncryptionDisabled() throws Throwable {
    SSLEngineFactory sslEngineFactory = encryptionDisabled();

    SslInitializer serverSslInitializer = new SslInitializer(sslEngineFactory, true, null);
    SslClientInitializer clientSslInitializer = new SslClientInitializer(sslEngineFactory);

    simpleTest(
        new DefaultEventLoopGroup(),
        LocalServerChannel.class,
        LocalChannel.class,
        new LocalAddress("testBasicLocalEncryptionDisabled"),
        serverSslInitializer,
        clientSslInitializer,
        LOREM_IPSUM,
        1,
        ctx -> {},
        ctx -> {});
  }

  @Test
  public void testBasicSocketEncryptionDisabled() throws Throwable {
    SSLEngineFactory sslEngineFactory = encryptionDisabled();

    SslInitializer serverSslInitializer = new SslInitializer(sslEngineFactory, true, null);
    SslClientInitializer clientSslInitializer = new SslClientInitializer(sslEngineFactory);

    simpleTest(
        new NioEventLoopGroup(),
        NioServerSocketChannel.class,
        NioSocketChannel.class,
        new InetSocketAddress(0),
        serverSslInitializer,
        clientSslInitializer,
        LOREM_IPSUM,
        1,
        ctx -> {},
        ctx -> {});
  }

  private void writeHelloInbound(EmbeddedChannel ch) {
    ch.writeInbound(
        Unpooled.copiedBuffer(
            new byte[] {
                // Record Header
                0x16, 0x03, 0x01, 0x00, (byte) 0xa5,

                // Client Hello
                0x01, 0x00, 0x00, (byte) 0xa1,

                // Client Version
                0x03, 0x03,

                // Client Random
                0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
                0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,

                // Session ID
                0x00,

                // Cipher Suites
                0x00, 0x20, (byte) 0xcc, (byte) 0xa8, (byte) 0xcc, (byte) 0xa9, (byte) 0xc0, 0x2f, (byte) 0xc0, 0x30,
                (byte) 0xc0, 0x2b, (byte) 0xc0, 0x2c, (byte) 0xc0, 0x13, (byte) 0xc0, 0x09, (byte) 0xc0, 0x14,
                (byte) 0xc0, 0x0a, 0x00, (byte) 0x9c, 0x00, (byte) 0x9d, 0x00, 0x2f, 0x00, 0x35, (byte) 0xc0, 0x12,
                0x00, 0x0a,

                // Compression Methods
                0x01, 0x00,

                // Extensions Length
                0x00, 0x58,

                0x00, 0x00, 0x00, 0x18, 0x00, 0x16, 0x00, 0x00, 0x13, 0x65, 0x78, 0x61, 0x6d, 0x70, 0x6c, 0x65, 0x2e,
                0x75, 0x6c, 0x66, 0x68, 0x65, 0x69, 0x6d, 0x2e, 0x6e, 0x65, 0x74,

                0x00, 0x05, 0x00, 0x05, 0x01, 0x00, 0x00, 0x00, 0x00,

                0x00, 0x0a, 0x00, 0x0a, 0x00, 0x08, 0x00, 0x1d, 0x00, 0x17, 0x00, 0x18, 0x00, 0x19,

                0x00, 0x0b, 0x00, 0x02, 0x01, 0x00,

                0x00, 0x0d, 0x00, 0x12, 0x00, 0x10, 0x04, 0x01, 0x04, 0x03, 0x05, 0x01, 0x05, 0x03, 0x06, 0x01, 0x06,
                0x03, 0x02, 0x01, 0x02, 0x03,

                (byte) 0xff, 0x01, 0x00, 0x01, 0x00,

                0x00, 0x12, 0x00, 0x00 }));
  }

  @Test
  public void testBasicShutdownException1() throws Throwable {
    SSLEngineFactory sslEngineFactory = newSslEngineSunJSSEFactory();
    EventExecutorGroup resolveExecutor = new DefaultEventExecutorGroup(1);
    AtomicInteger executorFailureCount = new AtomicInteger();
    SslInitializer serverSslInitializer = new SslInitializer(sslEngineFactory, true, null) {
      @Override
      protected void executorFailure(ChannelPromise promise, RejectedExecutionException ex) {
        executorFailureCount.incrementAndGet();
        super.executorFailure(promise, ex);
      }
    }.enableResolveBeforeSSL(resolveExecutor, 5, 1000L, 1);
    EmbeddedChannel ch = new EmbeddedChannel(serverSslInitializer);
    resolveExecutor.shutdownGracefully().sync();
    writeHelloInbound(ch);
    ch.checkException();
    ch.finishAndReleaseAll();
    Assert.assertEquals(executorFailureCount.get(), 1);
  }

  @Test
  public void testBasicShutdownException2() throws Throwable {
    SSLEngineFactory sslEngineFactory = newSslEngineSunJSSEFactory();
    EventExecutorGroup resolveExecutor = new DefaultEventExecutorGroup(1) {
      @Override
      public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        throw new RejectedExecutionException();
      }
    };
    AtomicInteger executorFailureCount = new AtomicInteger();
    SslInitializer serverSslInitializer = new SslInitializer(sslEngineFactory, true, null) {
      @Override
      protected void executorFailure(ChannelPromise promise, RejectedExecutionException ex) {
        executorFailureCount.incrementAndGet();
        super.executorFailure(promise, ex);
      }
    }.enableResolveBeforeSSL(resolveExecutor, 5, 1000L, 1);

    serverSslInitializer.setResolveAllByName(addr -> {
      resolveExecutor.shutdownGracefully(0, 10, TimeUnit.SECONDS);
      throw new UnknownHostException();
    });

    byte[] addressBytes = { 127, 0, 0, 1 };
    InetAddress address = InetAddress.getByAddress(addressBytes);

    EmbeddedChannel ch = new EmbeddedChannel(serverSslInitializer) {
      @Override
      protected SocketAddress remoteAddress0() {
        return new InetSocketAddress(address, 1234);
      }
    };
    writeHelloInbound(ch);
    resolveExecutor.terminationFuture().sync();
    ch.checkException();
    ch.finishAndReleaseAll();
    Assert.assertEquals(executorFailureCount.get(), 1);
  }

  @DataProvider
  public Object[][] sslEngineFactory() throws Exception {
    SSLEngineFactory newSSLEngineFactory = newSSLEngineFactory();
    SSLEngineFactory newSslEngineRefCntFactory = newSslEngineRefCntFactory();
    SSLEngineFactory newSslEngineSunJSSEFactory = newSslEngineSunJSSEFactory();
    return new Object[][] { new Object[] { newSSLEngineFactory, UnaryOperator.identity() },
        new Object[] { newSslEngineRefCntFactory, UnaryOperator.identity() },
        new Object[] { newSslEngineSunJSSEFactory, UnaryOperator.identity() },
        new Object[] { newSSLEngineFactory, _enableResolveClient },
        new Object[] { newSslEngineRefCntFactory, _enableResolveClient },
        new Object[] { newSslEngineSunJSSEFactory, _enableResolveClient },
        new Object[] { newSSLEngineFactory, _slowResolveClient },
        new Object[] { newSslEngineRefCntFactory, _slowResolveClient },
        new Object[] { newSslEngineSunJSSEFactory, _slowResolveClient },
        new Object[] { newSSLEngineFactory, _failFirstResolveClient },
        new Object[] { newSslEngineRefCntFactory, _failFirstResolveClient },
        new Object[] { newSslEngineSunJSSEFactory, _failFirstResolveClient } };
  }

  private SslInitializer enableResolveClient(SslInitializer sslInitializer) {
    return sslInitializer.enableResolveBeforeSSL(_resolveExecutor, 5, 1000L);
  }

  private SslInitializer slowResolveClient(SslInitializer sslInitializer) {
    sslInitializer.setResolveByAddress(address -> {
      Time.sleepUninterruptably(100);
      return InetAddress.getByAddress(address);
    });
    return enableResolveClient(sslInitializer);
  }

  private SslInitializer failFirstResolveClient(SslInitializer sslInitializer) {
    sslInitializer.setResolveByAddress(new SslInitializer.ResolveByAddress() {
      boolean _second;

      @Override
      public InetAddress getByAddress(byte[] address) throws UnknownHostException {
        if (_second) {
          return InetAddress.getByAddress(address);
        } else {
          _second = true;
          Time.sleepUninterruptably(100);
          throw new UnknownHostException("Timed out");
        }
      }
    });
    return enableResolveClient(sslInitializer);
  }

  @Test(dataProvider = "sslEngineFactory")
  public void testLocalGoodCertificates(SSLEngineFactory sslEngineFactory, UnaryOperator<SslInitializer> serverOp)
      throws Throwable {
    SslInitializer serverSslInitializer = serverOp.apply(new SslInitializer(sslEngineFactory, true, null));
    SslClientInitializer clientSslInitializer = new SslClientInitializer(sslEngineFactory);

    AtomicBoolean pendingHandshakesNonZero = new AtomicBoolean(false);
    simpleTest(
        new DefaultEventLoopGroup(),
        LocalServerChannel.class,
        LocalChannel.class,
        new LocalAddress("testLocalGoodCertificates" + ThreadLocalRandom.current().nextLong()),
        serverSslInitializer,
        clientSslInitializer,
        LOREM_IPSUM,
        1,
        ctx -> {},
        ctx -> {
          if (serverSslInitializer.getPendingHandshakes() > 0) {
            pendingHandshakesNonZero.set(true);
          }
        });

    TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.SECONDS, () -> {
      Assert.assertEquals(
          serverSslInitializer.getAvailablePermits(),
          UnaryOperator.identity().equals(serverOp) ? 0 : RESOLVE_EXECUTOR_THREADS);
      Assert.assertEquals(serverSslInitializer.getPendingHandshakes(), 0);
      Assert.assertEquals(serverSslInitializer.getHandshakesStarted(), 1L);
      Assert.assertEquals(serverSslInitializer.getHandshakesSuccessful(), 1L);
      Assert.assertEquals(serverSslInitializer.getHandshakesFailed(), 0L);
      // when not using slow handshake, we can't catch the pending-handshakes being non-zero because the handshake
      // blocks the IO worker and completes in a single pass.
      Assert.assertTrue(pendingHandshakesNonZero.get() || UnaryOperator.identity().equals(serverOp));
    });
    pendingHandshakesNonZero.set(false);

    simpleTest(
        new DefaultEventLoopGroup(),
        LocalServerChannel.class,
        LocalChannel.class,
        new LocalAddress("testLocalGoodCertificates" + ThreadLocalRandom.current().nextLong()),
        serverSslInitializer,
        clientSslInitializer,
        LOREM_IPSUM,
        1,
        ctx -> {},
        ctx -> {
          if (serverSslInitializer.getPendingHandshakes() > 0) {
            pendingHandshakesNonZero.set(true);
          }
        });
    TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.SECONDS, () -> {
      Assert.assertEquals(
          serverSslInitializer.getAvailablePermits(),
          UnaryOperator.identity().equals(serverOp) ? 0 : RESOLVE_EXECUTOR_THREADS);
      Assert.assertEquals(serverSslInitializer.getPendingHandshakes(), 0);
      Assert.assertEquals(serverSslInitializer.getHandshakesStarted(), 2L);
      Assert.assertEquals(serverSslInitializer.getHandshakesSuccessful(), 2L);
      Assert.assertEquals(serverSslInitializer.getHandshakesFailed(), 0L);
      Assert.assertTrue(pendingHandshakesNonZero.get() || UnaryOperator.identity().equals(serverOp));
    });
    pendingHandshakesNonZero.set(false);
  }

  /**
   * This test is disabled because it fails on GitHub actions but can run locally as it is. We can reproduce the issue
   * locally by increasing the concurrency to 100.
   *
   * The difference between {@link testConcurrentLocalGoodCertificates} and {@link testOffloadLocalGoodCertificates} is
   * that {@link testOffloadLocalGoodCertificates} provides an executor for SSL handshakes while the SSL handshakes in
   * {@link testConcurrentLocalGoodCertificates} are done on the caller thread.
   */
  @Test(dataProvider = "sslEngineFactory", enabled = false)
  public void testConcurrentLocalGoodCertificates(
      SSLEngineFactory sslEngineFactory,
      UnaryOperator<SslInitializer> serverOp) throws Throwable {
    SslInitializer serverSslInitializer = serverOp.apply(new SslInitializer(sslEngineFactory, true, null));
    SslClientInitializer clientSslInitializer = new SslClientInitializer(sslEngineFactory);

    int concurrency = 10;

    concurrentLocalGoodCertificates(
        concurrency,
        "testConcurrentLocalGoodCertificates",
        serverSslInitializer,
        clientSslInitializer,
        serverOp);
  }

  @Test(dataProvider = "sslEngineFactory")
  public void testOffloadLocalGoodCertificates(
      SSLEngineFactory sslEngineFactory,
      UnaryOperator<SslInitializer> serverOp) throws Throwable {
    SslInitializer serverSslInitializer = serverOp.apply(new SslInitializer(sslEngineFactory, true, null));
    SslClientInitializer clientSslInitializer = new SslClientInitializer(sslEngineFactory);

    int concurrency = 10;

    ExecutorService sslExecutor = Executors.newSingleThreadExecutor();
    try {
      concurrentLocalGoodCertificates(
          concurrency,
          "testOffloadLocalGoodCertificates",
          serverSslInitializer.enableSslTaskExecutor(sslExecutor),
          clientSslInitializer.enableSslTaskExecutor(sslExecutor),
          serverOp);
    } finally {
      sslExecutor.shutdownNow();
    }
  }

  private void concurrentLocalGoodCertificates(
      int concurrency,
      String test,
      SslInitializer serverSslInitializer,
      SslClientInitializer clientSslInitializer,
      UnaryOperator<SslInitializer> serverOp) throws Throwable {

    AtomicBoolean pendingHandshakesMoreThanOne = new AtomicBoolean(false);
    simpleTest(
        new DefaultEventLoopGroup(),
        LocalServerChannel.class,
        LocalChannel.class,
        new LocalAddress(test + ThreadLocalRandom.current().nextLong()),
        serverSslInitializer,
        clientSslInitializer,
        LOREM_IPSUM,
        concurrency,
        ctx -> {},
        ctx -> {
          if (serverSslInitializer.getPendingHandshakes() > 1) {
            pendingHandshakesMoreThanOne.set(true);
          }
        });
    TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.SECONDS, () -> {
      Assert.assertEquals(
          serverSslInitializer.getAvailablePermits(),
          UnaryOperator.identity().equals(serverOp) ? 0 : RESOLVE_EXECUTOR_THREADS);
      Assert.assertEquals(serverSslInitializer.getPendingHandshakes(), 0);
      Assert.assertEquals(serverSslInitializer.getHandshakesStarted(), concurrency);
      Assert.assertEquals(serverSslInitializer.getHandshakesSuccessful(), concurrency);
      Assert.assertEquals(serverSslInitializer.getHandshakesFailed(), 0L);
      // when not using slow handshake, we can't catch the pending-handshakes being non-zero because the handshake
      // blocks the IO worker and completes in a single pass.
      Assert.assertTrue(pendingHandshakesMoreThanOne.get() || UnaryOperator.identity().equals(serverOp));
    });
    pendingHandshakesMoreThanOne.set(false);

    simpleTest(
        new DefaultEventLoopGroup(),
        LocalServerChannel.class,
        LocalChannel.class,
        new LocalAddress(test + ThreadLocalRandom.current().nextLong()),
        serverSslInitializer,
        clientSslInitializer,
        LOREM_IPSUM,
        concurrency,
        ctx -> {},
        ctx -> {
          if (serverSslInitializer.getPendingHandshakes() > 1) {
            pendingHandshakesMoreThanOne.set(true);
          }
        });
    TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.SECONDS, () -> {
      Assert.assertEquals(
          serverSslInitializer.getAvailablePermits(),
          UnaryOperator.identity().equals(serverOp) ? 0 : RESOLVE_EXECUTOR_THREADS);
      Assert.assertEquals(serverSslInitializer.getPendingHandshakes(), 0);
      Assert.assertEquals(serverSslInitializer.getHandshakesStarted(), 2 * concurrency);
      Assert.assertEquals(serverSslInitializer.getHandshakesSuccessful(), 2 * concurrency);
      Assert.assertEquals(serverSslInitializer.getHandshakesFailed(), 0L);
      Assert.assertTrue(pendingHandshakesMoreThanOne.get() || UnaryOperator.identity().equals(serverOp));
    });
    pendingHandshakesMoreThanOne.set(false);
  }

  @Test(dataProvider = "sslEngineFactory")
  public void testLocalMismatchedClientDisabled(
      SSLEngineFactory sslEngineFactory,
      UnaryOperator<SslInitializer> serverOp) throws Throwable {
    SslInitializer serverSslInitializer = serverOp.apply(new SslInitializer(sslEngineFactory, true, null));
    SslClientInitializer clientSslInitializer = new SslClientInitializer(encryptionDisabled());

    Assert.assertThrows(
        NotSslRecordException.class,
        () -> simpleTest(
            new DefaultEventLoopGroup(),
            LocalServerChannel.class,
            LocalChannel.class,
            new LocalAddress("testLocalMismatchedClientDisabled" + ThreadLocalRandom.current().nextLong()),
            serverSslInitializer,
            clientSslInitializer,
            LOREM_IPSUM,
            1,
            ctx -> {},
            ctx -> {}));
    TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.SECONDS, () -> {
      Assert.assertEquals(
          serverSslInitializer.getAvailablePermits(),
          UnaryOperator.identity().equals(serverOp) ? 0 : RESOLVE_EXECUTOR_THREADS);
      Assert.assertEquals(serverSslInitializer.getPendingHandshakes(), 0);
      Assert.assertEquals(serverSslInitializer.getHandshakesStarted(), 0L);
      Assert.assertEquals(serverSslInitializer.getHandshakesSuccessful(), 0L);
      Assert.assertEquals(serverSslInitializer.getHandshakesFailed(), 0L);
    });

    Assert.assertThrows(
        NotSslRecordException.class,
        () -> simpleTest(
            new DefaultEventLoopGroup(),
            LocalServerChannel.class,
            LocalChannel.class,
            new LocalAddress("testLocalMismatchedClientDisabled" + ThreadLocalRandom.current().nextLong()),
            serverSslInitializer,
            clientSslInitializer,
            LOREM_IPSUM,
            1,
            ctx -> {},
            ctx -> {}));
    TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.SECONDS, () -> {
      Assert.assertEquals(
          serverSslInitializer.getAvailablePermits(),
          UnaryOperator.identity().equals(serverOp) ? 0 : RESOLVE_EXECUTOR_THREADS);
      Assert.assertEquals(serverSslInitializer.getPendingHandshakes(), 0);
      Assert.assertEquals(serverSslInitializer.getHandshakesStarted(), 0L);
      Assert.assertEquals(serverSslInitializer.getHandshakesSuccessful(), 0L);
      Assert.assertEquals(serverSslInitializer.getHandshakesFailed(), 0L);
    });
  }

  @Test(dataProvider = "sslEngineFactory")
  public void testLocalMismatchedClientSelfSigned(
      SSLEngineFactory sslEngineFactory,
      UnaryOperator<SslInitializer> serverOp) throws Throwable {
    SslInitializer serverSslInitializer = serverOp.apply(new SslInitializer(sslEngineFactory, true, null));
    SslClientInitializer clientSslInitializer = new SslClientInitializer(selfSigned());

    Assert.assertThrows(
        SSLHandshakeException.class,
        () -> simpleTest(
            new DefaultEventLoopGroup(),
            LocalServerChannel.class,
            LocalChannel.class,
            new LocalAddress("testLocalMismatchedClientSelfSigned" + ThreadLocalRandom.current().nextLong()),
            serverSslInitializer,
            clientSslInitializer,
            LOREM_IPSUM,
            1,
            ctx -> {},
            ctx -> {}));
    TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.SECONDS, () -> {
      Assert.assertEquals(
          serverSslInitializer.getAvailablePermits(),
          UnaryOperator.identity().equals(serverOp) ? 0 : RESOLVE_EXECUTOR_THREADS);
      Assert.assertEquals(serverSslInitializer.getPendingHandshakes(), 0);
      Assert.assertEquals(serverSslInitializer.getHandshakesStarted(), 1L);
      Assert.assertEquals(serverSslInitializer.getHandshakesSuccessful(), 0L);
      Assert.assertEquals(serverSslInitializer.getHandshakesFailed(), 1L);
    });

    Assert.assertThrows(
        SSLHandshakeException.class,
        () -> simpleTest(
            new DefaultEventLoopGroup(),
            LocalServerChannel.class,
            LocalChannel.class,
            new LocalAddress("testLocalMismatchedClientSelfSigned" + ThreadLocalRandom.current().nextLong()),
            serverSslInitializer,
            clientSslInitializer,
            LOREM_IPSUM,
            1,
            ctx -> {},
            ctx -> {}));
    TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.SECONDS, () -> {
      Assert.assertEquals(
          serverSslInitializer.getAvailablePermits(),
          UnaryOperator.identity().equals(serverOp) ? 0 : RESOLVE_EXECUTOR_THREADS);
      Assert.assertEquals(serverSslInitializer.getPendingHandshakes(), 0);
      Assert.assertEquals(serverSslInitializer.getHandshakesStarted(), 2L);
      Assert.assertEquals(serverSslInitializer.getHandshakesSuccessful(), 0L);
      Assert.assertEquals(serverSslInitializer.getHandshakesFailed(), 2L);
    });
  }

  @Test(dataProvider = "sslEngineFactory")
  public void testLocalMismatchedServerDisabled(
      SSLEngineFactory sslEngineFactory,
      UnaryOperator<SslInitializer> serverOp) throws Throwable {
    SslInitializer serverSslInitializer = serverOp.apply(new SslInitializer(encryptionDisabled(), true, null));
    SslClientInitializer clientSslInitializer = new SslClientInitializer(sslEngineFactory);

    Assert.assertThrows(
        SSLProtocolException.class,
        () -> simpleTest(
            new DefaultEventLoopGroup(),
            LocalServerChannel.class,
            LocalChannel.class,
            new LocalAddress("testLocalMismatchedServerDisabled" + ThreadLocalRandom.current().nextLong()),
            serverSslInitializer,
            clientSslInitializer,
            LOREM_IPSUM,
            1,
            ctx -> {},
            ctx -> {}));
    TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.SECONDS, () -> {
      Assert.assertEquals(
          serverSslInitializer.getAvailablePermits(),
          UnaryOperator.identity().equals(serverOp) ? 0 : RESOLVE_EXECUTOR_THREADS);
      Assert.assertEquals(serverSslInitializer.getPendingHandshakes(), 0);
      Assert.assertEquals(serverSslInitializer.getHandshakesStarted(), 0L);
      Assert.assertEquals(serverSslInitializer.getHandshakesSuccessful(), 0L);
      Assert.assertEquals(serverSslInitializer.getHandshakesFailed(), 0L);
    });

    Assert.assertThrows(
        SSLProtocolException.class,
        () -> simpleTest(
            new DefaultEventLoopGroup(),
            LocalServerChannel.class,
            LocalChannel.class,
            new LocalAddress("testLocalMismatchedServerDisabled" + ThreadLocalRandom.current().nextLong()),
            serverSslInitializer,
            clientSslInitializer,
            LOREM_IPSUM,
            1,
            ctx -> {},
            ctx -> {}));
    TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.SECONDS, () -> {
      Assert.assertEquals(
          serverSslInitializer.getAvailablePermits(),
          UnaryOperator.identity().equals(serverOp) ? 0 : RESOLVE_EXECUTOR_THREADS);
      Assert.assertEquals(serverSslInitializer.getPendingHandshakes(), 0);
      Assert.assertEquals(serverSslInitializer.getHandshakesStarted(), 0L);
      Assert.assertEquals(serverSslInitializer.getHandshakesSuccessful(), 0L);
      Assert.assertEquals(serverSslInitializer.getHandshakesFailed(), 0L);
    });
  }

  @Test(dataProvider = "sslEngineFactory")
  public void testSocketGoodCertificates(SSLEngineFactory sslEngineFactory, UnaryOperator<SslInitializer> serverOp)
      throws Throwable {

    SslInitializer serverSslInitializer = serverOp.apply(new SslInitializer(sslEngineFactory, true, null));
    SslClientInitializer clientSslInitializer = new SslClientInitializer(sslEngineFactory);
    socketGoodCertificates(serverSslInitializer, clientSslInitializer, serverOp);
  }

  @Test(dataProvider = "sslEngineFactory")
  public void testOffloadSocketGoodCertificates(
      SSLEngineFactory sslEngineFactory,
      UnaryOperator<SslInitializer> serverOp) throws Throwable {

    SslInitializer serverSslInitializer = serverOp.apply(new SslInitializer(sslEngineFactory, true, null));
    SslClientInitializer clientSslInitializer = new SslClientInitializer(sslEngineFactory);
    ExecutorService sslExecutor = Executors.newSingleThreadExecutor();
    try {
      socketGoodCertificates(
          serverSslInitializer.enableSslTaskExecutor(sslExecutor),
          clientSslInitializer.enableSslTaskExecutor(sslExecutor),
          serverOp);
    } finally {
      sslExecutor.shutdownNow();
    }
  }

  private void socketGoodCertificates(
      SslInitializer serverSslInitializer,
      SslClientInitializer clientSslInitializer,
      UnaryOperator<SslInitializer> serverOp) throws Throwable {
    {
      CompletableFuture<String> protocol = new CompletableFuture<>();
      simpleTest(
          new NioEventLoopGroup(),
          NioServerSocketChannel.class,
          NioSocketChannel.class,
          new InetSocketAddress(0),
          serverSslInitializer,
          clientSslInitializer,
          LOREM_IPSUM,
          1,
          ctx -> {
            SslHandler sslHandler = ctx.pipeline().get(SslHandler.class);
            protocol.complete(sslHandler.engine().getSession().getProtocol());
          },
          ctx -> {});

      Assert.assertTrue(protocol.isDone());
      Assert.assertEquals(protocol.getNow(null), "TLSv1.2");
    }
    TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.SECONDS, () -> {
      Assert.assertEquals(
          serverSslInitializer.getAvailablePermits(),
          UnaryOperator.identity().equals(serverOp) ? 0 : RESOLVE_EXECUTOR_THREADS);
      Assert.assertEquals(serverSslInitializer.getPendingHandshakes(), 0);
      Assert.assertEquals(serverSslInitializer.getHandshakesStarted(), 1L);
      Assert.assertEquals(serverSslInitializer.getHandshakesSuccessful(), 1L);
      Assert.assertEquals(serverSslInitializer.getHandshakesFailed(), 0L);
    });

    {
      CompletableFuture<String> protocol = new CompletableFuture<>();
      simpleTest(
          new NioEventLoopGroup(),
          NioServerSocketChannel.class,
          NioSocketChannel.class,
          new InetSocketAddress(0),
          serverSslInitializer,
          clientSslInitializer,
          LOREM_IPSUM,
          1,
          ctx -> {
            SslHandler sslHandler = ctx.pipeline().get(SslHandler.class);
            protocol.complete(sslHandler.engine().getSession().getProtocol());
          },
          ctx -> {});

      Assert.assertTrue(protocol.isDone());
      Assert.assertEquals(protocol.getNow(null), "TLSv1.2");
    }
    TestUtils.waitForNonDeterministicAssertion(1, TimeUnit.SECONDS, () -> {
      Assert.assertEquals(
          serverSslInitializer.getAvailablePermits(),
          UnaryOperator.identity().equals(serverOp) ? 0 : RESOLVE_EXECUTOR_THREADS);
      Assert.assertEquals(serverSslInitializer.getPendingHandshakes(), 0);
      Assert.assertEquals(serverSslInitializer.getHandshakesStarted(), 2L);
      Assert.assertEquals(serverSslInitializer.getHandshakesSuccessful(), 2L);
      Assert.assertEquals(serverSslInitializer.getHandshakesFailed(), 0L);
    });
  }

  /**
   * The tests for support of legacy TLS has been disabled by default starting from JDK 11.0.11.
   * These tests can be made to succeed by removing TLSv1 and TLSv1.1 from {@code jdk.tls.disabledAlgorithms} in
   * {@code $JAVA_HOME/conf/security/java.security}.
   * We can evaluate at a later time if support for these legacy algorithms is necessary.
   */
  @Test(enabled = false)
  public void testLegacyClientTLS() throws Throwable {
    SSLEngineFactoryImpl.Config sslEngineConfig = new SSLEngineFactoryImpl.Config();
    configure(sslEngineConfig);
    sslEngineConfig.setUseInsecureLegacyTlsProtocolBecauseOfBrokenPeersUsingAncientJdk(true);
    SSLEngineFactory sslEngineFactory = new SSLEngineFactoryImpl(sslEngineConfig);

    SslInitializer serverSslInitializer = new SslInitializer(newSSLEngineFactory(), true, null);
    SslClientInitializer clientSslInitializer = new SslClientInitializer(sslEngineFactory);

    CompletableFuture<String> protocol = new CompletableFuture<>();
    CompletableFuture<String> cipherSuite = new CompletableFuture<>();
    simpleTest(
        new NioEventLoopGroup(),
        NioServerSocketChannel.class,
        NioSocketChannel.class,
        new InetSocketAddress(0),
        serverSslInitializer,
        clientSslInitializer,
        LOREM_IPSUM,
        1,
        ctx -> {
          SslHandler sslHandler = ctx.pipeline().get(SslHandler.class);
          protocol.complete(sslHandler.engine().getSession().getProtocol());
          cipherSuite.complete(sslHandler.engine().getSession().getCipherSuite());
        },
        ctx -> {});

    Assert.assertTrue(protocol.isDone());
    Assert.assertEquals(protocol.getNow(null), "TLSv1");
    Assert.assertEquals(cipherSuite.getNow(null), "TLS_RSA_WITH_AES_128_CBC_SHA");
  }

  @Test(enabled = false)
  public void testLegacyServerTLS() throws Throwable {
    SSLEngineFactoryImpl.Config sslEngineConfig = new SSLEngineFactoryImpl.Config();
    configure(sslEngineConfig);
    sslEngineConfig.setUseInsecureLegacyTlsProtocolBecauseOfBrokenPeersUsingAncientJdk(true);
    SSLEngineFactory sslEngineFactory = new SSLEngineFactoryImpl(sslEngineConfig);

    SslInitializer serverSslInitializer = new SslInitializer(sslEngineFactory, true, null);
    SslClientInitializer clientSslInitializer = new SslClientInitializer(newSSLEngineFactory());

    CompletableFuture<String> protocol = new CompletableFuture<>();
    CompletableFuture<String> cipherSuite = new CompletableFuture<>();
    simpleTest(
        new NioEventLoopGroup(),
        NioServerSocketChannel.class,
        NioSocketChannel.class,
        new InetSocketAddress(0),
        serverSslInitializer,
        clientSslInitializer,
        LOREM_IPSUM,
        1,
        ctx -> {
          SslHandler sslHandler = ctx.pipeline().get(SslHandler.class);
          protocol.complete(sslHandler.engine().getSession().getProtocol());
          cipherSuite.complete(sslHandler.engine().getSession().getCipherSuite());
        },
        ctx -> {});

    Assert.assertTrue(protocol.isDone());
    Assert.assertEquals(protocol.get(), "TLSv1");
    Assert.assertEquals(cipherSuite.getNow(null), "TLS_RSA_WITH_AES_128_CBC_SHA");
  }

  @Test
  public void testIsNoSslHandshake() {
    Assert.assertTrue(SslInitializer.isNoSslHandshake(SslInitializer.NO_SSL_HANDSHAKE.cause()));
    Assert.assertFalse(
        SslInitializer.isNoSslHandshake(ExceptionUtil.withoutStackTrace(new SSLHandshakeException("No SSL"))));
  }
}
